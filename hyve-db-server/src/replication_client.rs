use std::{
    collections::HashSet,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::future::{self, FutureExt as _};
use hyve_db::{
    db::{self, Database},
    raft::{
        self, Client as _, CommandError, CommandHandle, IncomingCommand, PersistentState,
        PersistentStateDbCommand, Rpc, RpcData,
    },
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::{
    Client, Id, RaftState,
    cache::Cache,
    cli::Cli,
    command::{TransactionCommand, TransactionCommandResponse},
    error::{Error, Result},
};

#[derive(Debug)]
pub struct ReplicationClientBuilder {
    hash: u32,
    cli: Arc<Cli>,
    main_client: Weak<Client>,
}

impl ReplicationClientBuilder {
    pub fn new(hash: u32, cli: Arc<Cli>, main_client: Weak<Client>) -> Self {
        Self {
            hash,
            cli,
            main_client,
        }
    }
}

impl raft::ClientBuilder for ReplicationClientBuilder {
    type Client = ReplicationClient;

    fn build(
        self,
        rpc_sender: mpsc::Sender<raft::RpcChannelItem<Self::Client>>,
        rpc_response_sender: mpsc::Sender<raft::RpcResponseChannelItem<Self::Client>>,
        command_sender: mpsc::Sender<raft::CommandChannelItem<Self::Client>>,
        cancellation_token: CancellationToken,
    ) -> Result<Arc<ReplicationClient>> {
        let (expiration_sender, expiration_receiver) = mpsc::channel(5);
        let cache_state = CacheState {
            cache: Cache::new(self.cli.command_timeout, expiration_sender),
            expiration_receiver: Arc::new(Mutex::new(Some(expiration_receiver))),
        };

        let (new_peers_sender, new_peers_receiver) = mpsc::channel(5);

        let (persistent_state_db, persistent_state_command_sender) =
            Database::open_file(format!("{}_replication_{}.db3", self.cli.port, self.hash));

        let my_id = self.main_client.upgrade().ok_or(Error::ClientDropped)?.id();

        Ok(Arc::new(ReplicationClient {
            my_id,
            hash: self.hash,
            cli: self.cli.clone(),
            main_client: self.main_client,
            state: RaftState {
                rpc_sender,
                rpc_response_sender,
                command_sender,
            },
            cache_state,
            new_peers_sender,
            persistent_state_db: Mutex::new(Some(persistent_state_db)),
            persistent_state_command_sender,
            should_shutdown: Arc::new(AtomicBool::new(false)),
            new_peers_receiver: Arc::new(Mutex::new(new_peers_receiver)),
            cancellation_token,
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Command {
    command: TransactionCommand,
    id: Ulid,
}

impl raft::Command for Command {}

#[derive(Debug)]
struct CacheState {
    cache: Arc<Cache<Ulid, oneshot::Sender<db::Result<TransactionCommandResponse>>>>,
    expiration_receiver: Arc<
        Mutex<
            Option<
                mpsc::Receiver<(
                    Ulid,
                    oneshot::Sender<db::Result<TransactionCommandResponse>>,
                )>,
            >,
        >,
    >,
}

#[derive(Debug)]
pub struct ReplicationClient {
    my_id: Id,
    hash: u32,
    cli: Arc<Cli>,
    main_client: Weak<Client>,
    pub state: RaftState<Self>,
    cache_state: CacheState,
    new_peers_sender: mpsc::Sender<HashSet<Id>>,
    persistent_state_db: Mutex<Option<Database<PersistentStateDbCommand<Id, Command>>>>,
    persistent_state_command_sender:
        mpsc::Sender<db::CommandChannelItem<PersistentStateDbCommand<Id, Command>>>,
    should_shutdown: Arc<AtomicBool>,
    new_peers_receiver: Arc<Mutex<mpsc::Receiver<HashSet<Id>>>>,
    cancellation_token: CancellationToken,
}

impl raft::Client for ReplicationClient {
    type Error = Error;
    type Id = Id;
    type Command = Command;
    type RpcId = Ulid;
    type Builder = ReplicationClientBuilder;

    fn id(self: &Arc<Self>) -> Id {
        // tracing::info!(hash=%self.hash, "Getting client ID");

        self.my_id
    }

    async fn send_persistent_state_command(
        self: &Arc<Self>,
        command: PersistentStateDbCommand<Self::Id, Command>,
    ) -> Result<Option<PersistentState<Self::Id, Command>>> {
        // tracing::info!(hash=%self.hash, "Sending persistent state command");

        let (sender, receiver) = oneshot::channel();

        self.persistent_state_command_sender
            .send((command, sender))
            .await
            .map_err(|_| Error::DbCommandSend)?;

        receiver
            .await
            .map_err(|_| Error::DbResponseChannelClosed)?
            .map_err(Into::into)
    }

    async fn send_rpc(self: &Arc<Self>, id: Id, rpc: RpcData<Rpc<Id, Command>>) -> Result<Ulid> {
        self.main_client
            .upgrade()
            .ok_or(Error::ClientDropped)?
            .send_replication_rpc(id, self.hash, rpc, self.state.rpc_response_sender.clone())
            .await
    }

    async fn apply_command(
        self: &Arc<Self>,
        Command { command, id }: &Command,
        _origin: Id,
    ) -> Result<()> {
        let (sender, receiver) = oneshot::channel();

        self.main_client
            .upgrade()
            .ok_or(Error::ClientDropped)?
            .state
            .db_command_sender
            .send((command.clone(), sender))
            .await
            .map_err(|_| Error::DbCommandSend)?;

        let response = receiver.await.map_err(|_| Error::CommandResponseReceive)?;

        if let Some(sender) = self.cache_state.cache.remove(*id).await? {
            if sender.send(response).is_err() {
                tracing::error!("Failed to send command response");
            }
        }

        Ok(())
    }

    async fn next_election_timeout_duration(self: &Arc<Self>) -> Result<Duration> {
        let timeout = self.cli.election_timeout_range.select_random();

        // tracing::info!(
        //     hash=%self.hash,
        //     "Next election timeout duration: {}",
        //     humantime::format_duration(Duration::from_millis(timeout.as_millis() as u64))
        // );

        Ok(timeout)
    }

    async fn new_configuration(self: &Arc<Self>, _ids: &HashSet<Id>) -> Result<()> {
        Ok(())
    }

    async fn new_peers(self: &Arc<Self>, _ids: impl IntoIterator<Item = Id>) -> Result<()> {
        Ok(())
    }

    async fn exit_configuration(self: &Arc<Self>) -> Result<()> {
        if self.should_shutdown.load(Ordering::Acquire) {
            self.cancellation_token.cancel();

            Ok(())
        } else {
            Err(Error::ExitedConfiguration)
        }
    }

    async fn run(self: Arc<Self>) -> Result<()> {
        let _guard = self.cancellation_token.clone().drop_guard();

        let peers_task = tokio::spawn(Self::update_peers(
            self.state.clone(),
            self.new_peers_receiver.clone(),
            self.cancellation_token.clone(),
            self.hash,
        ));

        let persistent_state_db_task =
            if let Some(db) = self.persistent_state_db.lock().await.take() {
                tokio::spawn(Self::run_db(
                    self.cli.port,
                    self.hash,
                    db,
                    self.should_shutdown.clone(),
                    self.cancellation_token.clone(),
                ))
            } else {
                tokio::spawn(future::ready(Ok(())))
            };

        let cache_task = tokio::spawn(
            self.cache_state
                .cache
                .clone()
                .run(self.cancellation_token.clone()),
        );

        let cache_expiration_task =
            if let Some(receiver) = self.cache_state.expiration_receiver.lock().await.take() {
                tokio::spawn(Self::handle_expirations(
                    receiver,
                    self.cancellation_token.clone(),
                ))
            } else {
                tokio::spawn(future::ready(Ok(())))
            };

        tokio::try_join!(
            peers_task.map(Result::unwrap),
            persistent_state_db_task.map(Result::unwrap),
            cache_task.map(Result::unwrap),
            cache_expiration_task.map(Result::unwrap),
        )
        .map(|_| ())
    }
}

impl ReplicationClient {
    async fn update_peers(
        state: RaftState<Self>,
        new_peers_receiver: Arc<Mutex<mpsc::Receiver<HashSet<Id>>>>,
        cancellation_token: CancellationToken,
        hash: u32,
    ) -> Result<()> {
        let _guard = cancellation_token.clone().drop_guard();

        let mut receiver = new_peers_receiver.lock().await;

        let mut retry_peers = None;

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                res = tokio::time::timeout(Duration::from_secs(1), receiver.recv()) => {
                    match (res, retry_peers.take()) {
                        (Ok(Some(new_peers)), _) | (Err(_), Some(new_peers)) => {
                            if !Self::send_peers_to_raft(new_peers.clone(), &state.command_sender, hash).await? {
                                retry_peers = Some(new_peers);
                            }
                        }
                        (Ok(None), _) => break,
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn new_peers(self: &Arc<Self>, peers: HashSet<Id>) -> Result<()> {
        self.new_peers_sender
            .send(peers)
            .await
            .map_err(|_| Error::PeersSend)
    }

    pub async fn send_command(
        self: &Arc<Self>,
        command: TransactionCommand,
        origin: Id,
    ) -> Result<Result<TransactionCommandResponse, (CommandError<Id>, TransactionCommand)>> {
        match command {
            TransactionCommand::Read(_) => {
                let (sender, receiver) = oneshot::channel();

                self.main_client
                    .upgrade()
                    .ok_or(Error::ClientDropped)?
                    .state
                    .db_command_sender
                    .send((command.clone(), sender))
                    .await
                    .map_err(|_| Error::DbCommandSend)?;

                receiver
                    .await
                    .map_err(|_| Error::CommandResponseReceive)?
                    .map(Ok)
                    .map_err(Into::into)
            }
            _ => {
                let id = Ulid::new();

                let (cache_sender, cache_receiver) = oneshot::channel();

                self.cache_state.cache.insert(id, cache_sender).await?;

                let command = Command { command, id };

                let (raft_sender, raft_receiver) = oneshot::channel();
                self.state
                    .command_sender
                    .send(Ok(CommandHandle::new(
                        IncomingCommand::Command { command, origin },
                        raft_sender,
                    )))
                    .await
                    .map_err(|_| Error::CommandSend)?;

                if let Err((e, command)) = raft_receiver
                    .await
                    .map_err(|_| Error::CommandResponseReceive)?
                {
                    self.cache_state.cache.remove(id).await?;

                    let IncomingCommand::Command {
                        command: Command { command, .. },
                        ..
                    } = command
                    else {
                        panic!("Unexpected command type");
                    };

                    return Ok(Err((e, command)));
                }

                cache_receiver
                    .await
                    .map_err(|_| Error::CommandResponseReceive)?
                    .map(Ok)
                    .map_err(Into::into)
            }
        }
    }

    pub async fn schedule_shutdown(self: &Arc<Self>, peers: HashSet<Id>) -> Result<()> {
        self.should_shutdown.store(true, Ordering::Release);
        self.new_peers(peers).await
    }

    async fn send_peers_to_raft(
        peers: HashSet<Id>,
        command_sender: &mpsc::Sender<raft::CommandChannelItem<ReplicationClient>>,
        hash: u32,
    ) -> Result<bool> {
        tracing::info!(%hash, "Sending peers to Raft");

        let (sender, receiver) = oneshot::channel();
        command_sender
            .send(Ok(CommandHandle::new(
                IncomingCommand::Configuration(peers),
                sender,
            )))
            .await
            .map_err(|_| Error::CommandSend)?;

        Ok(
            match receiver.await.map_err(|_| Error::CommandResponseReceive)? {
                Ok(()) => {
                    tracing::info!(%hash, "Configuration updated successfully");
                    true
                }
                Err((raft::CommandError::NotEnoughNodes, _)) => {
                    tracing::info!(%hash, "Not enough nodes in configuration");
                    true
                }
                Err((e, _)) => {
                    tracing::info!(%hash, "Failed to update configuration: {e:?}");
                    false
                }
            },
        )
    }

    async fn run_db(
        port: u16,
        my_hash: u32,
        db: Database<PersistentStateDbCommand<Id, Command>>,
        should_shutdown: Arc<AtomicBool>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let _guard = cancellation_token.clone().drop_guard();

        db.run(cancellation_token).await?;

        if should_shutdown.load(Ordering::Acquire) {
            tokio::fs::remove_file(format!("{port}_replication_{my_hash}.db3")).await?;
        }

        Ok(())
    }

    async fn handle_expirations(
        mut receiver: mpsc::Receiver<(
            Ulid,
            oneshot::Sender<db::Result<TransactionCommandResponse>>,
        )>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let _guard = cancellation_token.clone().drop_guard();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                Some((_, sender)) = receiver.recv() => {
                    sender.send(Ok(TransactionCommandResponse::Failure("Command timed out".to_string()))).map_err(|_| Error::CommandResponseSend)?;
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }
}
