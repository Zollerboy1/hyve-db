use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    hash::Hash,
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use derive_where::derive_where;
use either::Either;
use futures::future::OptionFuture;
use rusqlite::{ToSql, types::FromSql};
use serde::{Deserialize, Serialize, de};
use thiserror::Error;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;

use crate::{db, offset_vec::OffsetVec};

#[derive(Debug, Clone)]
pub struct Config {
    pub n: usize,
    pub heartbeat_interval: Duration,
}

#[derive(Debug, Error)]
pub enum Error<E> {
    #[error("Client error: {0}")]
    Client(#[from] E),
    #[error("Response channel closed")]
    ResponseChannelClosed,
    #[error("Peer not in next index")]
    NextIndexNotFound,
    #[error("Peer not in match index")]
    MatchIndexNotFound,
    #[error("Configuration not stable")]
    ConfigurationNotStable,
    #[error("Multiple uncommitted configuration changes")]
    MultipleUncommittedConfigurationChanges,
}

pub trait Id: ToSql + FromSql + Hash + Eq {
    const SQL_TYPE: &str;
}

pub trait Command {}

impl Command for () {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigurationChange<I: Id> {
    old: HashSet<I>,
    append: HashSet<I>,
    remove: HashSet<I>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Configuration<I: Id> {
    Stable(HashSet<I>),
    Changing(ConfigurationChange<I>),
}

impl<I: Id + Copy> Configuration<I> {
    fn all_ids(&self) -> impl Iterator<Item = I> {
        match self {
            Configuration::Stable(ids) => Either::Left(ids.iter().copied()),
            Configuration::Changing(change) => {
                Either::Right(change.old.union(&change.append).copied())
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigurationCommand<I: Id, C: Command> {
    Command { command: C, origin: I },
    Configuration(Configuration<I>),
    Sync,
}

impl<I: Id, C: Command> ConfigurationCommand<I, C> {
    fn configuration(&self) -> Option<&Configuration<I>> {
        if let ConfigurationCommand::Configuration(configuration) = self {
            Some(configuration)
        } else {
            None
        }
    }
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Term(usize);

impl Term {
    fn increment(&mut self) {
        self.0 += 1;
    }
}

impl From<usize> for Term {
    fn from(value: usize) -> Self {
        Term(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry<I: Id, C: Command> {
    term: Term,
    command: ConfigurationCommand<I, C>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntries<I: Id, C: Command> {
    leader_id: I,
    prev_log_index: usize,
    prev_log_term: Term,
    entries: Vec<LogEntry<I, C>>,
    leader_commit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVote<I: Id> {
    candidate_id: I,
    last_log_index: usize,
    last_log_term: Term,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestVoteResponse {
    vote_granted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Rpc<I: Id, C: Command> {
    AppendEntries(AppendEntries<I, C>),
    RequestVote(RequestVote<I>),
}

impl<I: Id, C: Command> Rpc<I, C> {
    pub fn name(&self) -> &'static str {
        match self {
            Self::AppendEntries(_) => "AppendEntries",
            Self::RequestVote(_) => "RequestVote",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RpcResponse {
    AppendEntries(AppendEntriesResponse),
    RequestVote(RequestVoteResponse),
}

impl RpcResponse {
    pub fn name(&self) -> &'static str {
        match self {
            Self::AppendEntries(_) => "AppendEntries",
            Self::RequestVote(_) => "RequestVote",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcData<T> {
    pub term: Term,
    pub data: T,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("Not enough nodes")]
    NotEnoughNodes,
    #[error("Too many nodes")]
    TooManyNodes,
    #[error("Not a member")]
    NotAMember,
}

#[derive(Debug)]
pub struct RpcHandle<I: Id, C: Command> {
    term: Term,
    rpc: Rpc<I, C>,
    response_sender: oneshot::Sender<Result<RpcData<RpcResponse>, RpcError>>,
}

impl<I: Id, C: Command> RpcHandle<I, C> {
    pub fn new(
        term: Term,
        rpc: Rpc<I, C>,
        response_sender: oneshot::Sender<Result<RpcData<RpcResponse>, RpcError>>,
    ) -> Self {
        Self {
            term,
            rpc,
            response_sender,
        }
    }

    fn respond<E>(self, response: Result<RpcData<RpcResponse>, RpcError>) -> Result<(), Error<E>> {
        self.response_sender
            .send(response)
            .map_err(|_| Error::ResponseChannelClosed)
    }
}

#[derive(Debug)]
pub enum IncomingCommand<I: Id, C: Command> {
    Command { command: C, origin: I },
    Configuration(HashSet<I>),
}

#[derive(Debug)]
pub enum CommandError<I: Id> {
    Leader(I),
    NoLeader,
    AlreadyProcessingConfigurationChange,
    NotEnoughNodes,
    TooManyNodes,
    NotAMember,
}

impl<I: Id> From<Option<I>> for CommandError<I> {
    fn from(leader: Option<I>) -> Self {
        match leader {
            Some(leader) => CommandError::Leader(leader),
            None => CommandError::NoLeader,
        }
    }
}

#[derive(Debug)]
pub struct CommandHandle<I: Id, C: Command> {
    command: IncomingCommand<I, C>,
    response_sender: oneshot::Sender<Result<(), CommandError<I>>>,
}

impl<I: Id, C: Command> CommandHandle<I, C> {
    pub fn new(
        command: IncomingCommand<I, C>,
        response_sender: oneshot::Sender<Result<(), CommandError<I>>>,
    ) -> Self {
        Self {
            command,
            response_sender,
        }
    }

    fn respond<E>(self, response: Result<(), CommandError<I>>) -> Result<(), Error<E>> {
        self.response_sender
            .send(response)
            .map_err(|_| Error::ResponseChannelClosed)
    }
}

#[derive(Debug)]
#[derive_where(Default)]
pub struct PersistentState<I: Id, C: Command> {
    pub current_term: Term,
    pub voted_for: Option<I>,
    pub log: OffsetVec<LogEntry<I, C>>,
}

#[derive(Debug)]
pub enum PersistentStateDbCommand<I: Id, C: Command> {
    Persist {
        current_term: Term,
        voted_for: Option<I>,
        new_log_entries: Vec<LogEntry<I, C>>,
    },
    GetPersisted,
}

impl<
    I: Id + Serialize + for<'a> de::Deserialize<'a>,
    C: Command + Serialize + for<'a> de::Deserialize<'a>,
> db::Command for PersistentStateDbCommand<I, C>
{
    type Output = Option<PersistentState<I, C>>;

    fn migrate(conn: &mut rusqlite::Connection) -> db::Result<()> {
        conn.execute_batch(&format!(
            "BEGIN;
            CREATE TABLE IF NOT EXISTS persistent_state (
                id INTEGER PRIMARY KEY,
                current_term INTEGER NOT NULL,
                voted_for {}
            );
            CREATE TABLE IF NOT EXISTS persistent_state_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                term INTEGER NOT NULL,
                command BLOB NOT NULL
            );
            INSERT OR IGNORE INTO persistent_state (id, current_term, voted_for) VALUES (0, 0, NULL);
            END;",
            I::SQL_TYPE,
        ))
        .map_err(Into::into)
    }

    fn execute(self, conn: &mut rusqlite::Connection) -> db::Result<Option<PersistentState<I, C>>> {
        Ok(match self {
            Self::Persist {
                current_term,
                voted_for,
                new_log_entries,
            } => {
                let tx = conn.transaction()?;
                {
                    tx.execute(
                        "UPDATE persistent_state SET current_term = ?, voted_for = ? WHERE id = 0",
                        (&current_term.0, &voted_for),
                    )?;

                    if !new_log_entries.is_empty() {
                        let mut stmt = tx.prepare(
                            "INSERT INTO persistent_state_log (term, command) VALUES (?, ?)",
                        )?;
                        for entry in new_log_entries {
                            let command_blob = cbor4ii::serde::to_vec(Vec::new(), &entry.command)
                                .map_err(|e| {
                                Box::new(e) as Box<dyn std::error::Error + Send + Sync>
                            })?;
                            stmt.execute((&entry.term.0, &command_blob))?;
                        }
                    }
                }
                tx.commit()?;

                None
            }
            Self::GetPersisted => {
                let mut stmt = conn
                    .prepare("SELECT current_term, voted_for FROM persistent_state WHERE id = 0")?;
                let (current_term, voted_for) =
                    stmt.query_row([], |row| Ok((Term(row.get(0)?), row.get(1)?)))?;

                let mut stmt =
                    conn.prepare("SELECT term, command FROM persistent_state_log ORDER BY id")?;
                let log = stmt
                    .query_map([], |row| {
                        let term = Term(row.get(0)?);
                        let command_blob = row.get_ref(1)?;
                        let command = cbor4ii::serde::from_slice(&command_blob.as_blob()?)
                            .map_err(|e| {
                                rusqlite::Error::FromSqlConversionFailure(
                                    1,
                                    command_blob.data_type(),
                                    Box::new(e),
                                )
                            })?;
                        Ok(LogEntry { term, command })
                    })?
                    .collect::<Result<_, _>>()?;

                Some(PersistentState {
                    current_term,
                    voted_for,
                    log,
                })
            }
        })
    }
}

#[expect(async_fn_in_trait)]
pub trait Client {
    type Error: Debug + Display + Send + 'static;
    type Id: Id + Debug + Display + Copy + Serialize + for<'a> de::Deserialize<'a> + 'static;
    type Command: Command + Debug + Clone + Serialize + for<'a> de::Deserialize<'a>;
    type RpcId: Eq + Hash + Debug + Display + Copy;
    type Builder: ClientBuilder<Client = Self>;

    fn id(self: &Arc<Self>) -> Self::Id;

    async fn send_persistent_state_command(
        self: &Arc<Self>,
        command: PersistentStateDbCommand<Self::Id, Self::Command>,
    ) -> Result<Option<PersistentState<Self::Id, Self::Command>>, Self::Error>;

    async fn send_rpc(
        self: &Arc<Self>,
        id: Self::Id,
        rpc: RpcData<Rpc<Self::Id, Self::Command>>,
    ) -> Result<Self::RpcId, Self::Error>;

    async fn apply_command(
        self: &Arc<Self>,
        command: &Self::Command,
        origin: Self::Id,
    ) -> Result<(), Self::Error>;

    async fn next_election_timeout_duration(self: &Arc<Self>) -> Result<Duration, Self::Error>;

    async fn new_configuration(
        self: &Arc<Self>,
        ids: &HashSet<Self::Id>,
    ) -> Result<(), Self::Error>;

    async fn new_peers(
        self: &Arc<Self>,
        ids: impl IntoIterator<Item = Self::Id>,
    ) -> Result<(), Self::Error>;

    async fn exit_configuration(self: &Arc<Self>) -> Result<(), Self::Error>;

    fn run(self: Arc<Self>) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static;
}

pub type RpcChannelItem<C> =
    Result<RpcHandle<<C as Client>::Id, <C as Client>::Command>, <C as Client>::Error>;

pub type RpcResponseChannelItem<C> = (
    <C as Client>::RpcId,
    Result<RpcData<RpcResponse>, <C as Client>::Error>,
);

pub type CommandChannelItem<C> =
    Result<CommandHandle<<C as Client>::Id, <C as Client>::Command>, <C as Client>::Error>;

pub trait ClientBuilder {
    type Client: Client<Builder = Self>;

    fn build(
        self,
        rpc_sender: mpsc::Sender<RpcChannelItem<Self::Client>>,
        rpc_response_sender: mpsc::Sender<RpcResponseChannelItem<Self::Client>>,
        command_sender: mpsc::Sender<CommandChannelItem<Self::Client>>,
        cancellation_token: CancellationToken,
    ) -> Result<Arc<Self::Client>, <Self::Client as Client>::Error>;
}

#[derive_where(Debug)]
struct FollowerState<C: Client> {
    known_leader: Option<C::Id>,
    election_timeout: Pin<Box<time::Sleep>>,
}

#[derive_where(Debug)]
struct CandidateState<C: Client> {
    votes: HashSet<C::Id>,
    pending_responses: HashMap<C::RpcId, C::Id>,
    election_timeout: Pin<Box<time::Sleep>>,
}

#[derive_where(Debug)]
struct LeaderState<C: Client> {
    next_index: HashMap<C::Id, usize>,
    match_index: HashMap<C::Id, usize>,
    pending_responses: HashMap<C::RpcId, (C::Id, usize)>,
    heartbeat_timeout: Option<Pin<Box<time::Sleep>>>,
}

#[derive_where(Debug)]
enum State<C: Client> {
    None,
    Follower(FollowerState<C>),
    Candidate(CandidateState<C>),
    Leader(LeaderState<C>),
}

macro_rules! convert_to_follower {
    ($self:ident) => {{
        if !matches!($self.state, State::Follower(_)) {
            tracing::info!(name=%$self.name, term = %$self.persistent_state.current_term.0, "Converting to follower");

            $self.state = Self::new_follower_state(&$self.client).await?;
        }

        match &mut $self.state {
            State::Follower(state) => state,
            _ => unreachable!(),
        }
    }};
}

macro_rules! configuration {
    ($self:ident) => {
        $self
            .configuration_entries
            .last()
            .map(|i| {
                if let ConfigurationCommand::Configuration(configuration) =
                    &$self.persistent_state.log[*i].command
                {
                    configuration
                } else {
                    unreachable!("configuration_entries should always point to correct log entries")
                }
            })
            .unwrap_or(&$self.initial_configuration)
    };
}

macro_rules! handle_configuration_command {
    ($self:ident, $leader_state:expr, $configuration:expr $(,)?) => {{
        let leader_state: Option<&mut LeaderState<C>> = $leader_state;
        if $self
            .configuration_entries
            .last()
            .is_some_and(|i| *i > $self.commit_index)
        {
            Err(Err(CommandError::AlreadyProcessingConfigurationChange))
        } else if $configuration.len() <= $self.config.n / 2 {
            Err(Err(CommandError::NotEnoughNodes))
        } else if $configuration.len() > $self.config.n {
            Err(Err(CommandError::TooManyNodes))
        } else {
            let Configuration::Stable(old) = configuration!($self) else {
                return Err(Error::ConfigurationNotStable);
            };

            if old.is_superset(&$configuration) {
                Err(Ok(()))
            } else {
                let append = $configuration
                    .difference(&old)
                    .cloned()
                    .collect::<HashSet<_>>();

                if let Some(state) = leader_state {
                    for id in &append {
                        state
                            .next_index
                            .insert(id.clone(), $self.persistent_state.log.len() + 2);
                        state.match_index.insert(id.clone(), 0);
                    }
                }

                $self
                    .configuration_entries
                    .push($self.persistent_state.log.len() + 1);

                let configuration = Configuration::Changing(ConfigurationChange {
                    old: old.clone(),
                    append,
                    remove: old.difference(&$configuration).cloned().collect(),
                });

                Self::new_configuration(&$self.client, &configuration).await?;

                Ok(ConfigurationCommand::Configuration(configuration))
            }
        }
    }};
}

#[derive(Debug)]
pub struct Raft<C: Client> {
    config: Config,
    my_id: C::Id,
    name: String,
    persistent_state: PersistentState<C::Id, C::Command>,
    last_persisted_index: usize,
    commit_index: usize,
    last_applied: usize,
    state: State<C>,
    initial_configuration: Configuration<C::Id>,
    configuration_entries: Vec<usize>,
    cancellation_token: CancellationToken,
    pub client: Arc<C>,
    client_task: JoinHandle<Result<(), C::Error>>,
    rpc_receiver: mpsc::Receiver<RpcChannelItem<C>>,
    rpc_response_receiver: mpsc::Receiver<RpcResponseChannelItem<C>>,
    command_receiver: mpsc::Receiver<CommandChannelItem<C>>,
}

impl<C: Client> Raft<C> {
    pub async fn new(
        config: Config,
        name: String,
        initial_configuration: HashSet<C::Id>,
        cancellation_token: CancellationToken,
        client_builder: C::Builder,
    ) -> Result<Self, Error<C::Error>> {
        tracing::info!(name=%name, "Initializing Raft node");

        let (rpc_sender, rpc_receiver) = mpsc::channel(10);
        let (rpc_response_sender, rpc_response_receiver) = mpsc::channel(10);
        let (command_sender, command_receiver) = mpsc::channel(10);

        let client = client_builder.build(
            rpc_sender,
            rpc_response_sender,
            command_sender,
            cancellation_token.clone(),
        )?;

        let client_task = tokio::spawn(client.clone().run());

        let persistent_state: PersistentState<C::Id, C::Command> = client
            .send_persistent_state_command(PersistentStateDbCommand::GetPersisted)
            .await?
            .unwrap_or_default();

        let last_persisted_index = persistent_state.log.len();

        let configuration_entries = persistent_state
            .log
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.command.configuration().is_some())
            .map(|(index, _)| index + 1)
            .collect::<Vec<_>>();

        let initial_configuration = configuration_entries
            .last()
            .map(|i| {
                if let ConfigurationCommand::Configuration(configuration) =
                    &persistent_state.log[*i].command
                {
                    configuration.clone()
                } else {
                    unreachable!("configuration_entries should always point to correct log entries")
                }
            })
            .unwrap_or(Configuration::Stable(initial_configuration));

        let my_id = client.id();

        let state = if initial_configuration.all_ids().count() > config.n / 2
            && initial_configuration.all_ids().any(|id| id == my_id)
        {
            tracing::info!(name=%name, "Raft node initialized");

            Self::new_follower_state(&client).await?
        } else {
            tracing::info!(name=%name, "Raft node waiting for more nodes");

            State::None
        };

        Self::new_configuration(&client, &initial_configuration).await?;

        Ok(Self {
            config,
            my_id,
            name,
            persistent_state,
            last_persisted_index,
            commit_index: 0,
            last_applied: 0,
            state,
            initial_configuration,
            configuration_entries: Vec::new(),
            cancellation_token,
            client,
            client_task,
            rpc_receiver,
            rpc_response_receiver,
            command_receiver,
        })
    }

    pub async fn run(mut self) -> Result<(), Error<C::Error>> {
        tracing::info!(name=%self.name, "Raft node started");

        let _guard = self.cancellation_token.clone().drop_guard();

        loop {
            let applied_commands = self.commit_index > self.last_applied;
            let mut exit_configuration = false;
            while self.commit_index > self.last_applied {
                self.last_applied += 1;
                let command = &self.persistent_state.log[self.last_applied].command;

                if let ConfigurationCommand::Configuration(Configuration::Stable(ids)) = command {
                    if self
                        .configuration_entries
                        .last()
                        .is_some_and(|i| *i == self.last_applied)
                        && !ids.contains(&self.my_id)
                    {
                        exit_configuration = true;
                    }
                }

                let new_command = if let State::Leader(state) = &mut self.state {
                    if let ConfigurationCommand::Configuration(Configuration::Changing(change)) =
                        command
                    {
                        if self
                            .configuration_entries
                            .last()
                            .is_some_and(|i| *i == self.last_applied)
                        {
                            for id in &change.remove {
                                state.next_index.remove(id);
                                state.match_index.remove(id);
                            }

                            state.heartbeat_timeout = None;

                            let new = change
                                .old
                                .union(&change.append)
                                .filter(|id| !change.remove.contains(id))
                                .cloned()
                                .collect::<HashSet<_>>();

                            let configuration = Configuration::Stable(new.clone());

                            Self::new_configuration(&self.client, &configuration).await?;

                            Some(LogEntry {
                                term: self.persistent_state.current_term,
                                command: ConfigurationCommand::Configuration(configuration),
                            })
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                self.apply_committed_command(&command).await?;

                if let Some(new_command) = new_command {
                    self.configuration_entries
                        .push(self.persistent_state.log.len() + 1);

                    self.persistent_state.log.push(new_command);
                }
            }

            if applied_commands {
                self.send_append_entries().await?;
            }

            if exit_configuration {
                tracing::info!(name=%self.name, "Exiting configuration");

                self.state = State::None;
                self.client.exit_configuration().await?;
            }

            match &mut self.state {
                State::None => {
                    tokio::select! {
                        _ = self.cancellation_token.cancelled() => {
                            break;
                        }
                        Some(res) = self.rpc_receiver.recv() => {
                            res?.respond(Err(RpcError::NotAMember))?;
                        }
                        Some(_) = self.rpc_response_receiver.recv() => {
                            continue;
                        }
                        Some(res) = self.command_receiver.recv() => {
                            let handle = res?;

                            let response = if let IncomingCommand::Configuration(configuration) = handle.command {
                                if configuration.len() <= self.config.n / 2 {
                                    Err(CommandError::NotEnoughNodes)
                                } else if configuration.len() > self.config.n {
                                    Err(CommandError::TooManyNodes)
                                } else if !configuration.contains(&self.my_id) {
                                    Err(CommandError::NotAMember)
                                } else {
                                    self.initial_configuration = Configuration::Stable(configuration);
                                    self.state = Self::new_follower_state(&self.client).await?;
                                    tracing::info!(name=%self.name, "Raft node initialized");

                                    Self::new_configuration(&self.client, &self.initial_configuration).await?;

                                    Ok(())
                                }
                            } else {
                                Err(CommandError::NotAMember)
                            };

                            handle
                                .response_sender
                                .send(response)
                                .map_err(|_| Error::ResponseChannelClosed)?;
                        }
                        else => {
                            break;
                        }
                    }
                }
                State::Follower(state) => {
                    tokio::select! {
                        _ = self.cancellation_token.cancelled() => {
                            break;
                        }
                        _ = &mut state.election_timeout => {
                            self.start_election().await?;
                        }
                        Some(res) = self.rpc_receiver.recv() => {
                            self.handle_rpc(res?).await?;
                        }
                        Some((rpc_id, res)) = self.rpc_response_receiver.recv() => {
                            let term =  match res {
                                Ok(RpcData { term, .. }) => term,
                                Err(e) => {
                                    tracing::info!(
                                        name=%self.name,
                                        term = %self.persistent_state.current_term.0,
                                        %rpc_id,
                                        "Got error response: {e}"
                                    );

                                    continue;
                                },
                            };

                            self.update_term(term).await?;
                        }
                        Some(res) = self.command_receiver.recv() => {
                            let handle = res?;

                            let response = match (&handle.command, configuration!(self)) {
                                (IncomingCommand::Configuration(new), Configuration::Stable(old)) if old.is_superset(&new) => {
                                    Ok(())
                                }
                                _ => Err(state.known_leader.clone().into())
                            };

                            handle.respond(response)?;
                        }
                        else => {
                            break;
                        }
                    }
                }
                State::Candidate(state) => {
                    tokio::select! {
                        _ = self.cancellation_token.cancelled() => {
                            break;
                        }
                        _ = &mut state.election_timeout => {
                            self.start_election().await?;
                        }
                        Some(res) = self.rpc_receiver.recv() => {
                            self.handle_rpc(res?).await?;
                        }
                        Some((rpc_id, res)) = self.rpc_response_receiver.recv() => {
                            let RpcData { term, data } =  match res {
                                Ok(data) => data,
                                Err(e) => {
                                    tracing::info!(
                                        name=%self.name,
                                        term = %self.persistent_state.current_term.0,
                                        %rpc_id,
                                        "Got error response: {e}"
                                    );

                                    continue;
                                },
                            };

                            self.update_term(term).await?;

                            if let RpcResponse::RequestVote(response) = data {
                                self.handle_request_vote_response(rpc_id, response).await?;
                            }
                        }
                        Some(res) = self.command_receiver.recv() => {
                            let handle = res?;

                            let response = match (&handle.command, configuration!(self)) {
                                (IncomingCommand::Configuration(new), Configuration::Stable(old)) => {
                                    if old.is_superset(&new) {
                                        Ok(())
                                    } else {
                                        match handle_configuration_command!(self, None, new) {
                                            Ok(command) => {
                                                self.persistent_state.log.push(LogEntry {
                                                    term: self.persistent_state.current_term,
                                                    command,
                                                });

                                                Ok(())
                                            },
                                            Err(res) => res,
                                        }
                                    }
                                }
                                _ => Err(CommandError::NoLeader)
                            };

                            handle.respond(response)?;
                        }
                        else => {
                            break;
                        }
                    }
                }
                State::Leader(state) => {
                    let heartbeat_timeout: OptionFuture<_> =
                        state.heartbeat_timeout.as_mut().into();
                    tokio::select! {
                        _ = self.cancellation_token.cancelled() => {
                            break;
                        }
                        _ = heartbeat_timeout => {
                            self.send_append_entries().await?;
                        }
                        Some(res) = self.rpc_receiver.recv() => {
                            self.handle_rpc(res?).await?;
                        }
                        Some((rpc_id, res)) = self.rpc_response_receiver.recv() => {
                            let RpcData { term, data } =  match res {
                                Ok(data) => data,
                                Err(e) => {
                                    tracing::info!(
                                        name=%self.name,
                                        term = %self.persistent_state.current_term.0,
                                        %rpc_id,
                                        "Got error response: {e}"
                                    );

                                    continue;
                                },
                            };

                            self.update_term(term).await?;

                            if let RpcResponse::AppendEntries(response) = data {
                                self.handle_append_entries_response(rpc_id, response).await?;
                            }
                        }
                        Some(res) = self.command_receiver.recv() => {
                            self.handle_command(res?).await?;
                        }
                        else => {
                            break;
                        }
                    }
                }
            };
        }

        self.cancellation_token.cancel();

        self.client_task.await.unwrap().map_err(Into::into)
    }

    async fn handle_rpc(
        &mut self,
        mut handle: RpcHandle<C::Id, C::Command>,
    ) -> Result<(), Error<C::Error>> {
        self.update_term(handle.term).await?;

        let response = match &mut handle.rpc {
            Rpc::AppendEntries(req) => {
                let success = if handle.term < self.persistent_state.current_term {
                    tracing::warn!(
                        name=%self.name,
                        term=%self.persistent_state.current_term.0,
                        "Received RPC with outdated term"
                    );

                    false
                } else {
                    let state = convert_to_follower!(self);

                    let success = if req.prev_log_index > self.persistent_state.log.len() {
                        tracing::warn!(
                            name=%self.name,
                            term=%self.persistent_state.current_term.0,
                            "Received RPC with prev_log_index out of bounds"
                        );

                        false
                    } else if req.prev_log_index > 0
                        && self.persistent_state.log[req.prev_log_index].term != req.prev_log_term
                    {
                        tracing::warn!(
                            name=%self.name,
                            term=%self.persistent_state.current_term.0,
                            "Received RPC with prev_log_term mismatch"
                        );

                        false
                    } else {
                        state.known_leader = Some(req.leader_id.clone());

                        let mut entries = req.entries.drain(..).enumerate();
                        if let Some((index, entry)) = entries.by_ref().find(|(i, entry)| {
                            let index = req.prev_log_index + i + 1;
                            index > self.persistent_state.log.len()
                                || entry.term != self.persistent_state.log[index].term
                        }) {
                            self.persistent_state
                                .log
                                .truncate(req.prev_log_index + index);

                            while self
                                .configuration_entries
                                .last()
                                .is_some_and(|i| *i > req.prev_log_index + index)
                            {
                                self.configuration_entries.pop();
                            }

                            let mut configuration_changed = false;
                            if entry.command.configuration().is_some() {
                                self.configuration_entries
                                    .push(req.prev_log_index + index + 1);

                                configuration_changed = true;
                            }

                            self.persistent_state.log.push(entry);

                            let entries = entries.map(|(i, entry)| {
                                if entry.command.configuration().is_some() {
                                    self.configuration_entries.push(req.prev_log_index + i + 1);

                                    configuration_changed = true;
                                }

                                entry
                            });

                            self.persistent_state.log.extend(entries);

                            if configuration_changed {
                                Self::new_configuration(&self.client, configuration!(self)).await?;
                            }
                        }

                        if req.leader_commit > self.commit_index {
                            self.commit_index =
                                req.leader_commit.min(self.persistent_state.log.len());
                        }

                        true
                    };

                    state.election_timeout = Self::new_election_timeout(&self.client).await?;

                    success
                };

                RpcResponse::AppendEntries(AppendEntriesResponse { success })
            }
            Rpc::RequestVote(req) => {
                let vote_granted = if handle.term < self.persistent_state.current_term
                    || self
                        .persistent_state
                        .voted_for
                        .as_ref()
                        .is_some_and(|voted_for| *voted_for != req.candidate_id)
                    || self.persistent_state.log.last().is_some_and(|last_entry| {
                        last_entry.term > req.last_log_term
                            || (last_entry.term == req.last_log_term
                                && self.persistent_state.log.len() > req.last_log_index)
                    }) {
                    false
                } else {
                    self.persistent_state.voted_for = Some(req.candidate_id.clone());
                    let state = convert_to_follower!(self);
                    state.election_timeout = Self::new_election_timeout(&self.client).await?;
                    true
                };

                RpcResponse::RequestVote(RequestVoteResponse { vote_granted })
            }
        };

        Self::persist(
            &self.client,
            &self.persistent_state,
            &mut self.last_persisted_index,
        )
        .await?;

        handle.respond(Ok(RpcData {
            term: self.persistent_state.current_term,
            data: response,
        }))
    }

    async fn start_election(&mut self) -> Result<(), Error<C::Error>> {
        let my_id = self.client.id();
        let mut state = CandidateState {
            votes: HashSet::from([my_id.clone()]),
            pending_responses: HashMap::new(),
            election_timeout: Self::new_election_timeout(&self.client).await?,
        };
        self.persistent_state.current_term.increment();
        self.persistent_state.voted_for = Some(my_id.clone());

        tracing::info!(name=%self.name, term = %self.persistent_state.current_term.0, "Starting election");

        Self::persist(
            &self.client,
            &self.persistent_state,
            &mut self.last_persisted_index,
        )
        .await?;

        for peer in configuration!(self).all_ids().filter(|id| *id != my_id) {
            let rpc_id = self
                .client
                .send_rpc(
                    peer,
                    RpcData {
                        term: self.persistent_state.current_term,
                        data: Rpc::RequestVote(RequestVote {
                            candidate_id: my_id.clone(),
                            last_log_index: self.persistent_state.log.len(),
                            last_log_term: self
                                .persistent_state
                                .log
                                .last()
                                .map(|entry| entry.term)
                                .unwrap_or_default(),
                        }),
                    },
                )
                .await?;

            state.pending_responses.insert(rpc_id, peer.clone());
        }

        self.state = State::Candidate(state);

        Ok(())
    }

    async fn send_append_entries(&mut self) -> Result<(), Error<C::Error>> {
        let State::Leader(state) = &mut self.state else {
            return Ok(());
        };

        Self::persist(
            &self.client,
            &self.persistent_state,
            &mut self.last_persisted_index,
        )
        .await?;

        for peer in configuration!(self)
            .all_ids()
            .filter(|id| *id != self.my_id)
        {
            let Some(next_index) = state.next_index.get(&peer).copied() else {
                return Err(Error::NextIndexNotFound);
            };

            let entries = (self.persistent_state.log.len() >= next_index)
                .then(|| self.persistent_state.log[next_index..].to_vec())
                .unwrap_or_default();

            let prev_log_term = (next_index > 1)
                .then(|| self.persistent_state.log[next_index - 1].term)
                .unwrap_or_default();

            let rpc_id = self
                .client
                .send_rpc(
                    peer,
                    RpcData {
                        term: self.persistent_state.current_term,
                        data: Rpc::AppendEntries(AppendEntries {
                            leader_id: self.my_id,
                            prev_log_index: next_index - 1,
                            prev_log_term,
                            entries,
                            leader_commit: self.commit_index,
                        }),
                    },
                )
                .await?;

            state
                .pending_responses
                .insert(rpc_id, (peer.clone(), self.persistent_state.log.len()));
        }

        state.heartbeat_timeout = Some(Box::pin(time::sleep(self.config.heartbeat_interval)));

        Ok(())
    }

    async fn handle_request_vote_response(
        &mut self,
        rpc_id: C::RpcId,
        response: RequestVoteResponse,
    ) -> Result<(), Error<C::Error>> {
        let State::Candidate(state) = &mut self.state else {
            return Ok(());
        };

        let Some(peer) = state.pending_responses.remove(&rpc_id) else {
            return Ok(());
        };

        if response.vote_granted {
            state.votes.insert(peer);
        }

        let configuration = configuration!(self);

        if Self::is_majority(configuration, &state.votes, self.config.n) {
            tracing::info!(name=%self.name, term = %self.persistent_state.current_term.0, "Elected as leader");

            self.state = State::Leader(LeaderState {
                next_index: configuration
                    .all_ids()
                    .filter_map(|id| {
                        (id != self.my_id)
                            .then(|| (id.clone(), self.persistent_state.log.len() + 1))
                    })
                    .collect(),
                match_index: configuration
                    .all_ids()
                    .filter_map(|id| (id != self.my_id).then(|| (id.clone(), 0)))
                    .collect(),
                pending_responses: HashMap::new(),
                heartbeat_timeout: None,
            });

            if self.commit_index < self.persistent_state.log.len() {
                self.persistent_state.log.push(LogEntry {
                    term: self.persistent_state.current_term,
                    command: ConfigurationCommand::Sync,
                });
            }
        }

        Ok(())
    }

    async fn handle_append_entries_response(
        &mut self,
        rpc_id: C::RpcId,
        response: AppendEntriesResponse,
    ) -> Result<(), Error<C::Error>> {
        let State::Leader(state) = &mut self.state else {
            return Ok(());
        };

        let Some((peer, index)) = state.pending_responses.remove(&rpc_id) else {
            return Ok(());
        };

        if response.success {
            *state
                .next_index
                .get_mut(&peer)
                .ok_or(Error::NextIndexNotFound)? = index + 1;
            *state
                .match_index
                .get_mut(&peer)
                .ok_or(Error::MatchIndexNotFound)? = index;
        } else {
            tracing::info!(name=%self.name, term = %self.persistent_state.current_term.0, "AppendEntries RPC failed for peer {peer}");

            let next_index = state
                .next_index
                .get_mut(&peer)
                .ok_or(Error::NextIndexNotFound)?;

            *next_index = 1.max(*next_index - 1);
        }

        let (min_index, commit_peers) = state
            .match_index
            .iter()
            .filter(|(_, index)| **index > self.commit_index)
            .fold(
                (
                    self.persistent_state.log.len(),
                    HashSet::from([&self.my_id]),
                ),
                |(min_index, mut commit_peers), (peer, index)| {
                    commit_peers.insert(peer);
                    (min_index.min(*index), commit_peers)
                },
            );

        if min_index > 0
            && self.persistent_state.log[min_index].term == self.persistent_state.current_term
            && Self::is_majority(&configuration!(self), &commit_peers, self.config.n)
        {
            tracing::info!(name=%self.name, term = %self.persistent_state.current_term.0, "Committing index {min_index}");

            self.commit_index = min_index;
        }

        Ok(())
    }

    async fn handle_command(
        &mut self,
        handle: CommandHandle<C::Id, C::Command>,
    ) -> Result<(), Error<C::Error>> {
        let State::Leader(state) = &mut self.state else {
            return Ok(());
        };

        let command = match handle.command {
            IncomingCommand::Command { command, origin } => {
                ConfigurationCommand::Command { command, origin }
            }
            IncomingCommand::Configuration(configuration) => {
                match handle_configuration_command!(self, Some(&mut *state), configuration) {
                    Ok(command) => command,
                    Err(res) => {
                        return handle
                            .response_sender
                            .send(res)
                            .map_err(|_| Error::ResponseChannelClosed);
                    }
                }
            }
        };

        self.persistent_state.log.push(LogEntry {
            term: self.persistent_state.current_term,
            command,
        });

        state.heartbeat_timeout = None;

        handle
            .response_sender
            .send(Ok(()))
            .map_err(|_| Error::ResponseChannelClosed)
    }

    async fn update_term(&mut self, term: Term) -> Result<(), Error<C::Error>> {
        if term > self.persistent_state.current_term {
            tracing::info!(name=%self.name, term = %term.0, "Updating term");

            self.persistent_state.current_term = term;
            self.persistent_state.voted_for = None;
            convert_to_follower!(self);
        }

        Ok(())
    }

    async fn apply_committed_command(
        &self,
        command: &ConfigurationCommand<C::Id, C::Command>,
    ) -> Result<(), Error<C::Error>> {
        match command {
            ConfigurationCommand::Command { command, origin } => self
                .client
                .apply_command(command, *origin)
                .await
                .map_err(Into::into),
            ConfigurationCommand::Configuration(_) => Ok(()), // configuration changes are applied when they enter the log
            ConfigurationCommand::Sync => Ok(()),             // Sync command does nothing
        }
    }

    async fn persist(
        client: &Arc<C>,
        persistent_state: &PersistentState<C::Id, C::Command>,
        last_persisted_index: &mut usize,
    ) -> Result<(), Error<C::Error>> {
        assert!(
            client
                .send_persistent_state_command(PersistentStateDbCommand::Persist {
                    current_term: persistent_state.current_term,
                    voted_for: persistent_state.voted_for.clone(),
                    new_log_entries: persistent_state.log[*last_persisted_index + 1..].to_vec(),
                })
                .await?
                .is_none()
        );

        *last_persisted_index = persistent_state.log.len();

        Ok(())
    }

    async fn new_configuration(
        client: &Arc<C>,
        configuration: &Configuration<C::Id>,
    ) -> Result<(), Error<C::Error>> {
        match configuration {
            Configuration::Stable(ids) => client.new_configuration(ids).await,
            Configuration::Changing(_) => client.new_peers(configuration.all_ids()).await,
        }
        .map_err(Into::into)
    }

    async fn new_follower_state(client: &Arc<C>) -> Result<State<C>, Error<C::Error>> {
        Ok(State::Follower(FollowerState {
            known_leader: None,
            election_timeout: Self::new_election_timeout(client).await?,
        }))
    }

    async fn new_election_timeout(
        client: &Arc<C>,
    ) -> Result<Pin<Box<time::Sleep>>, Error<C::Error>> {
        client
            .next_election_timeout_duration()
            .await
            .map(time::sleep)
            .map(Box::pin)
            .map_err(Into::into)
    }

    fn is_majority(
        configuration: &Configuration<C::Id>,
        ids: &HashSet<impl Borrow<C::Id> + Eq + Hash>,
        n: usize,
    ) -> bool {
        if ids.len() <= n / 2 {
            return false;
        }

        match configuration {
            Configuration::Stable(configuration) => {
                configuration.iter().filter(|id| ids.contains(id)).count() > configuration.len() / 2
            }
            Configuration::Changing(ConfigurationChange {
                old,
                append,
                remove,
            }) => {
                if old.iter().filter(|id| ids.contains(id)).count() > old.len() / 2 {
                    let new = old.union(&append).filter(|id| !remove.contains(id));
                    new.clone().filter(|id| ids.contains(id)).count() > new.count() / 2
                } else {
                    false
                }
            }
        }
    }
}
