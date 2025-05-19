use std::path::{Path, PathBuf};

use rusqlite::Connection;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("Could not send result")]
    SendResult,
    #[error(transparent)]
    Custom(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub trait Command {
    type Output;

    fn migrate(conn: &mut Connection) -> Result<()>;

    fn execute(self, conn: &mut Connection) -> Result<Self::Output>;
}

pub type CommandChannelItem<C> = (C, oneshot::Sender<Result<<C as Command>::Output>>);

#[derive(Debug, Clone)]
enum OpenKind {
    File(PathBuf),
    InMemory,
}

#[derive(Debug)]
pub struct Database<C: Command> {
    open_kind: OpenKind,
    command_receiver: mpsc::Receiver<CommandChannelItem<C>>,
}

impl<C: Command> Database<C> {
    fn open(kind: OpenKind) -> (Self, mpsc::Sender<CommandChannelItem<C>>) {
        let (command_sender, command_receiver) = mpsc::channel(10);

        (
            Self {
                open_kind: kind,
                command_receiver,
            },
            command_sender,
        )
    }

    pub fn open_file(path: impl AsRef<Path>) -> (Self, mpsc::Sender<CommandChannelItem<C>>) {
        Self::open(OpenKind::File(path.as_ref().to_path_buf()))
    }

    pub fn open_in_memory() -> (Self, mpsc::Sender<CommandChannelItem<C>>) {
        Self::open(OpenKind::InMemory)
    }

    pub async fn run(mut self, cancellation_token: CancellationToken) -> Result<()> {
        let mut conn = match self.open_kind {
            OpenKind::File(path) => Connection::open(path)?,
            OpenKind::InMemory => Connection::open_in_memory()?,
        };

        tracing::info!("Database opened");

        if let Err(e) = C::migrate(&mut conn) {
            tracing::error!("Database migration failed: {}", e);
            return Err(e);
        }

        tracing::info!("Database migrated");

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                Some((command, sender)) = self.command_receiver.recv() => {
                    let result = command.execute(&mut conn);
                    sender.send(result).map_err(|_| Error::SendResult)?;
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }
}
