#![feature(iter_intersperse)]
#![feature(result_flattening)]

use std::{
    borrow::Cow,
    collections::HashSet,
    fmt::{self, Display},
    sync::Arc,
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::{Json, Query, State as AxumState},
    http::StatusCode,
};
use clap::Parser as _;
use cli::Cli;
use command::{Command, DbState, TransactionCommand, TransactionCommandResponse, Value};
use derive_where::derive_where;
use error::{Error, JsonResult, Result};
use futures::future::{self, FutureExt as _, TryFutureExt as _};
use hyve_db::{
    db::{self, Database},
    raft::{
        self, Client as RaftClient, CommandError, CommandHandle, Config as RaftConfig,
        IncomingCommand, PersistentState, PersistentStateDbCommand, Raft, Rpc, RpcData, RpcHandle,
        RpcResponse, RpcResponseChannelItem,
    },
};
use peers::Peers;
use reqwest::RequestBuilder;
use rusqlite::{
    ToSql,
    types::{FromSql, FromSqlResult, ToSqlOutput, ValueRef},
};
use serde::{Deserialize, Serialize};
use tokio::{
    net::TcpListener,
    sync::{Mutex, RwLock, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt as _};
use ulid::Ulid;

mod cache;
mod cli;
mod command;
mod error;
mod peers;
mod replication_client;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Id(u16);

impl ToSql for Id {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        self.0.to_sql()
    }
}

impl FromSql for Id {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        u16::column_result(value).map(Self)
    }
}

impl raft::Id for Id {
    const SQL_TYPE: &str = "INTEGER";
}

impl Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u16> for Id {
    fn from(value: u16) -> Self {
        Id(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicationQuery {
    hash: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MainCommandRequest {
    command_string: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum MainCommandResponse {
    Rows { rows: Vec<Vec<Value>> },
    Inserted { id: Ulid },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReplicationTransactionRequest<'a> {
    transaction: Cow<'a, TransactionCommand>,
    origin: Id,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PeersRequest {
    id: Id,
    peers: HashSet<Id>,
}

#[derive(Debug)]
struct ClientBuilder {
    cli: Arc<Cli>,
}

impl ClientBuilder {
    fn new(cli: Arc<Cli>) -> Self {
        ClientBuilder { cli }
    }
}

impl raft::ClientBuilder for ClientBuilder {
    type Client = Client;

    fn build(
        self,
        rpc_sender: mpsc::Sender<raft::RpcChannelItem<Self::Client>>,
        rpc_response_sender: mpsc::Sender<raft::RpcResponseChannelItem<Self::Client>>,
        command_sender: mpsc::Sender<raft::CommandChannelItem<Self::Client>>,
        cancellation_token: CancellationToken,
    ) -> Result<Arc<Client>> {
        let (new_peers_sender, new_peers_receiver) = mpsc::channel(5);

        let (persistent_state_db, persistent_state_command_sender) =
            Database::open_file(format!("{}.db3", self.cli.port));

        let (db, db_command_sender) = Database::open_in_memory();

        Ok(Arc::new_cyclic(|client| Client {
            cli: self.cli.clone(),
            reqwest: reqwest::Client::new(),
            persistent_state_db: Mutex::new(Some(persistent_state_db)),
            db: Mutex::new(Some(db)),
            state: Arc::new(State {
                main: RaftState {
                    rpc_sender,
                    rpc_response_sender,
                    command_sender,
                },
                new_peers_sender,
                persistent_state_command_sender,
                db_command_sender,
                peers: Peers::new(self.cli, client.clone(), cancellation_token.child_token()),
                new_peers_receiver: Arc::new(Mutex::new(new_peers_receiver)),
            }),
            cancellation_token,
        }))
    }
}

#[derive_where(Debug, Clone)]
struct RaftState<C: raft::Client> {
    rpc_sender: mpsc::Sender<raft::RpcChannelItem<C>>,
    rpc_response_sender: mpsc::Sender<raft::RpcResponseChannelItem<C>>,
    command_sender: mpsc::Sender<raft::CommandChannelItem<C>>,
}

#[derive(Debug, Clone)]
struct State {
    main: RaftState<Client>,
    new_peers_sender: mpsc::Sender<HashSet<Id>>,
    persistent_state_command_sender:
        mpsc::Sender<db::CommandChannelItem<PersistentStateDbCommand<Id, ()>>>,
    db_command_sender: mpsc::Sender<db::CommandChannelItem<TransactionCommand>>,
    peers: Arc<RwLock<Peers>>,
    new_peers_receiver: Arc<Mutex<mpsc::Receiver<HashSet<Id>>>>,
}

#[derive(Debug)]
struct Client {
    cli: Arc<Cli>,
    reqwest: reqwest::Client,
    persistent_state_db: Mutex<Option<Database<PersistentStateDbCommand<Id, ()>>>>,
    db: Mutex<Option<Database<TransactionCommand>>>,
    state: Arc<State>,
    cancellation_token: CancellationToken,
}

impl RaftClient for Client {
    type Error = Error;
    type Id = Id;
    type Command = ();
    type RpcId = Ulid;
    type Builder = ClientBuilder;

    fn id(self: &Arc<Self>) -> Id {
        // tracing::info!("Getting client ID");

        self.cli.port.into()
    }

    async fn send_persistent_state_command(
        self: &Arc<Self>,
        command: PersistentStateDbCommand<Self::Id, Self::Command>,
    ) -> Result<Option<PersistentState<Self::Id, Self::Command>>> {
        // tracing::info!("Sending persistent state command");

        let (sender, receiver) = oneshot::channel();

        self.state
            .persistent_state_command_sender
            .send((command, sender))
            .await
            .map_err(|_| Error::DbCommandSend)?;

        receiver
            .await
            .map_err(|_| Error::DbResponseChannelClosed)?
            .map_err(Into::into)
    }

    async fn send_rpc(self: &Arc<Self>, id: Id, rpc: RpcData<Rpc<Id, ()>>) -> Result<Ulid> {
        let url = format!("http://localhost:{id}/rpc");

        self.send_rpc_impl(
            id,
            self.reqwest.post(url),
            rpc,
            self.state.main.rpc_response_sender.clone(),
            "main".into(),
        )
        .await
    }

    async fn apply_command(self: &Arc<Self>, _command: &(), _origin: Id) -> Result<()> {
        Ok(())
    }

    async fn next_election_timeout_duration(self: &Arc<Self>) -> Result<Duration> {
        let timeout = self.cli.election_timeout_range.select_random();

        // tracing::info!(
        //     "Next election timeout duration: {}",
        //     humantime::format_duration(Duration::from_millis(timeout.as_millis() as u64))
        // );

        Ok(timeout)
    }

    async fn new_configuration(self: &Arc<Self>, ids: &HashSet<Id>) -> Result<()> {
        self.state
            .peers
            .write()
            .await
            .extend(ids.iter().copied())
            .await?;

        self.state
            .new_peers_sender
            .send(ids.clone())
            .await
            .map_err(|_| Error::PeersSend)?;

        Ok(())
    }

    async fn new_peers(self: &Arc<Self>, ids: impl IntoIterator<Item = Id>) -> Result<()> {
        self.state
            .new_peers_sender
            .send(ids.into_iter().collect())
            .await
            .map_err(|_| Error::PeersSend)?;

        Ok(())
    }

    async fn exit_configuration(self: &Arc<Self>) -> Result<()> {
        Err(Error::ExitedConfiguration)
    }

    async fn run(self: Arc<Self>) -> Result<()> {
        let axum_task = tokio::spawn({
            let port = self.cli.port;
            let state = self.state.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                let _guard = cancellation_token.clone().drop_guard();

                let router = axum::Router::new()
                    .route("/rpc", axum::routing::post(Self::handle_main_rpc))
                    .route(
                        "/replication_rpc",
                        axum::routing::post(Self::handle_replication_rpc),
                    )
                    .route("/command", axum::routing::post(Self::handle_main_command))
                    .route(
                        "/replication_transaction",
                        axum::routing::post(Self::handle_replication_transaction),
                    )
                    .route("/peers", axum::routing::post(Self::handle_peers))
                    .with_state(state);

                let listener = TcpListener::bind(("0.0.0.0", port)).await?;
                axum::serve(listener, router)
                    .with_graceful_shutdown(cancellation_token.cancelled_owned())
                    .await?;

                Ok(())
            }
        });

        let peers_task = tokio::spawn(Self::update_peers(
            self.cli.port,
            self.reqwest.clone(),
            self.state.clone(),
            self.cancellation_token.clone(),
        ));

        let persistent_state_db_task =
            if let Some(db) = self.persistent_state_db.lock().await.take() {
                let cancellation_token = self.cancellation_token.clone();
                tokio::spawn(db.run(cancellation_token).map_err(Into::into))
            } else {
                tokio::spawn(future::ready(Ok(())))
            };

        let db_task = if let Some(db) = self.db.lock().await.take() {
            let state = DbState::new(self.cli.clone());
            let cancellation_token = self.cancellation_token.clone();
            tokio::spawn(
                db.run_with_state(state, cancellation_token)
                    .map_err(Into::into),
            )
        } else {
            tokio::spawn(future::ready(Ok(())))
        };

        let finalize_peers_task = tokio::spawn({
            let state = self.state.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                cancellation_token.cancelled_owned().await;

                state.peers.write().await.finalize().await
            }
        });

        let _guard = self.cancellation_token.clone().drop_guard();

        tokio::try_join!(
            axum_task.map(Result::unwrap),
            peers_task.map(Result::unwrap),
            persistent_state_db_task.map(Result::unwrap),
            db_task.map(Result::unwrap),
            finalize_peers_task.map(Result::unwrap),
        )
        .map(|_| ())
    }
}

impl Client {
    async fn send_replication_rpc(
        self: &Arc<Self>,
        id: Id,
        replication_hash: u32,
        rpc: RpcData<Rpc<Id, replication_client::Command>>,
        response_sender: mpsc::Sender<RpcResponseChannelItem<Self>>,
    ) -> Result<Ulid> {
        let url = format!("http://localhost:{id}/replication_rpc");

        let request_builder = self.reqwest.post(url).query(&ReplicationQuery {
            hash: replication_hash,
        });

        self.send_rpc_impl(
            id,
            request_builder,
            rpc,
            response_sender,
            format!("{replication_hash}").into(),
        )
        .await
    }

    async fn send_rpc_impl<Command: raft::Command + Serialize + Send + 'static>(
        self: &Arc<Self>,
        id: Id,
        request_builder: RequestBuilder,
        rpc: RpcData<Rpc<Id, Command>>,
        response_sender: mpsc::Sender<RpcResponseChannelItem<Self>>,
        raft_name: Arc<str>,
    ) -> Result<Ulid> {
        let rpc_id = Ulid::new();

        tokio::spawn({
            async move {
                let name = rpc.data.name();

                tracing::info!(%rpc_id, "Sending {name} RPC to {raft_name} raft on {id}");

                let response = future::ready(cbor4ii::serde::to_vec(Vec::new(), &rpc))
                    .map_err(Into::into)
                    .and_then(move |data| {
                        request_builder
                            .body(data)
                            .send()
                            .and_then(reqwest::Response::json::<JsonResult<_, _>>)
                            .map_err(Into::into)
                            .and_then(|res| {
                                future::ready(
                                    Result::from(res).map_err(|(_, string)| Error::Foreign(string)),
                                )
                            })
                    })
                    .await;

                if response_sender.send((rpc_id, response)).await.is_err() {
                    tracing::error!(%rpc_id, "Failed to forward {name} RPC response to {raft_name} raft");
                }
            }
        });

        Ok(rpc_id)
    }

    async fn send_replication_transaction(
        self: &Arc<Self>,
        id: Id,
        replication_hash: u32,
        transaction: &TransactionCommand,
    ) -> Result<TransactionCommandResponse> {
        let url = format!("http://localhost:{id}/replication_transaction");

        let request_builder = self.reqwest.post(url).query(&ReplicationQuery {
            hash: replication_hash,
        });

        tracing::info!("Sending transaction to {replication_hash} raft on {id}");

        let request = ReplicationTransactionRequest {
            transaction: Cow::Borrowed(transaction),
            origin: self.id(),
        };

        future::ready(cbor4ii::serde::to_vec(Vec::new(), &request))
            .map_err(Into::into)
            .and_then(move |data| {
                request_builder
                    .body(data)
                    .send()
                    .and_then(reqwest::Response::json::<JsonResult<_, _>>)
                    .map_err(Into::into)
                    .and_then(|res| {
                        future::ready(
                            Result::from(res).map_err(|(_, string)| Error::Foreign(string)),
                        )
                    })
            })
            .await
    }

    async fn handle_main_rpc(
        AxumState(state): AxumState<Arc<State>>,
        body: Bytes,
    ) -> JsonResult<RpcData<RpcResponse>, String> {
        Self::handle_main_rpc_impl(state, body).await.into()
    }

    async fn handle_main_rpc_impl(
        state: Arc<State>,
        body: Bytes,
    ) -> Result<RpcData<RpcResponse>, (StatusCode, String)> {
        let data = cbor4ii::serde::from_slice(&body).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to deserialize RPC data: {e}"),
            )
        })?;

        Self::handle_rpc_impl(data, state.main.rpc_sender.clone(), "main".into()).await
    }

    async fn handle_replication_rpc(
        AxumState(state): AxumState<Arc<State>>,
        Query(ReplicationQuery { hash }): Query<ReplicationQuery>,
        body: Bytes,
    ) -> JsonResult<RpcData<RpcResponse>, String> {
        Self::handle_replication_rpc_impl(state, body, hash)
            .await
            .into()
    }

    async fn handle_replication_rpc_impl(
        state: Arc<State>,
        body: Bytes,
        hash: u32,
    ) -> Result<RpcData<RpcResponse>, (StatusCode, String)> {
        let data = cbor4ii::serde::from_slice(&body).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to deserialize RPC data: {e}"),
            )
        })?;

        let rpc_sender = {
            state
                .peers
                .read()
                .await
                .replication_states
                .get(&hash)
                .ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Replication hash not found".to_string(),
                    )
                })?
                .client
                .state
                .rpc_sender
                .clone()
        };

        Self::handle_rpc_impl(data, rpc_sender, format!("{hash}").into()).await
    }

    async fn handle_rpc_impl<'a, Command: raft::Command + Deserialize<'a> + Send + 'static>(
        RpcData { term, data }: RpcData<Rpc<Id, Command>>,
        rpc_sender: mpsc::Sender<Result<RpcHandle<Id, Command>, Error>>,
        raft_name: Arc<str>,
    ) -> Result<RpcData<RpcResponse>, (StatusCode, String)> {
        let name = data.name();

        let res = {
            let raft_name = raft_name.clone();

            async move {
                let (sender, receiver) = oneshot::channel();

                tracing::info!("Received {name} RPC for {raft_name} raft");

                rpc_sender
                    .send(Ok(RpcHandle::new(term, data, sender)))
                    .await
                    .map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Failed to forward {name} RPC to {raft_name} raft"),
                        )
                    })?;

                tracing::info!("Forwarded {name} RPC to {raft_name} raft");

                receiver
                    .await
                    .map_err(|_| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Failed to get {name} RPC response from {raft_name} raft"),
                        )
                    })?
                    .map_err(|e| {
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Failed to get {name} RPC response from {raft_name} raft: {e}"),
                        )
                    })
            }
            .await
        };

        match &res {
            Ok(_) => {
                tracing::info!("Successfully handled {raft_name} raft {name} RPC");
            }
            Err((_, err)) => {
                tracing::error!("Error handling {raft_name} raft {name} RPC: {err}");
            }
        }

        res
    }

    async fn handle_main_command(
        AxumState(state): AxumState<Arc<State>>,
        Json(MainCommandRequest { command_string }): Json<MainCommandRequest>,
    ) -> JsonResult<MainCommandResponse, String> {
        Self::handle_main_command_impl(state, command_string)
            .await
            .into()
    }

    async fn handle_main_command_impl(
        state: Arc<State>,
        command_string: String,
    ) -> Result<MainCommandResponse, (StatusCode, String)> {
        let command = Command::parse(&command_string).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Error parsing command: {e}"),
            )
        })?;

        let (sender, receiver) = oneshot::channel();

        {
            state
                .peers
                .read()
                .await
                .queue_command(command, sender)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Error queuing command: {e}"),
                    )
                })?;
        }

        receiver
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error receiving response: {e}"),
                )
            })?
            .map_err(|e| (StatusCode::OK, format!("Command failed: {e}")))
    }

    async fn handle_replication_transaction(
        AxumState(state): AxumState<Arc<State>>,
        Query(ReplicationQuery { hash }): Query<ReplicationQuery>,
        body: Bytes,
    ) -> JsonResult<TransactionCommandResponse, String> {
        Self::handle_replication_transaction_impl(state, body, hash)
            .await
            .into()
    }

    async fn handle_replication_transaction_impl(
        state: Arc<State>,
        body: Bytes,
        hash: u32,
    ) -> Result<TransactionCommandResponse, (StatusCode, String)> {
        let ReplicationTransactionRequest {
            transaction,
            origin,
        } = cbor4ii::serde::from_slice(&body).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("Failed to deserialize transaction data: {e}"),
            )
        })?;

        tracing::info!("Received transaction for {hash} raft");

        let replication_client = {
            state
                .peers
                .read()
                .await
                .replication_states
                .get(&hash)
                .ok_or_else(|| {
                    (
                        StatusCode::BAD_REQUEST,
                        "Replication hash not found".to_string(),
                    )
                })?
                .client
                .clone()
        };

        let response = replication_client
            .send_command(transaction.into_owned(), origin)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Failed to send transaction: {e}"),
                )
            })?;

        Ok(match response {
            Ok(response) => response,
            Err((CommandError::Leader(id), _)) => TransactionCommandResponse::Leader(id),
            Err((CommandError::NoLeader, _)) => TransactionCommandResponse::NoLeader,
            Err((e, _)) => {
                let error = error::CommandError::from(e);
                TransactionCommandResponse::Failure(format!(
                    "Failed to send command to raft: {error}"
                ))
            }
        })
    }

    async fn handle_peers(
        AxumState(state): AxumState<Arc<State>>,
        Json(request): Json<PeersRequest>,
    ) -> JsonResult<HashSet<Id>, String> {
        Self::handle_peers_impl(state, request)
            .await
            .map_err(|s| (StatusCode::INTERNAL_SERVER_ERROR, s))
            .into()
    }

    async fn handle_peers_impl(
        state: Arc<State>,
        PeersRequest { id, mut peers }: PeersRequest,
    ) -> Result<HashSet<Id>, String> {
        // tracing::info!("Received peers from {}", id.0);

        peers.insert(id);

        {
            state
                .peers
                .write()
                .await
                .known_peer_peers
                .entry(id)
                .or_default()
                .extend(&peers);
        }

        state
            .new_peers_sender
            .send(peers)
            .await
            .map_err(|_| "Failed to send peers".to_string())?;
        Ok(state.peers.read().await.known_peers.clone())
    }

    async fn update_peers(
        my_id: u16,
        reqwest: reqwest::Client,
        state: Arc<State>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let _guard = cancellation_token.clone().drop_guard();

        let mut receiver = state.new_peers_receiver.lock().await;

        let mut retry: Option<(JoinHandle<Result<()>>, CancellationToken)> = None;

        while let Some(new_peers) = receiver.recv().await {
            // tracing::info!("Handling new peers");

            let new_peers = { state.peers.write().await.extend_known(new_peers) };

            if let Some(new_peers) = new_peers {
                if let Some((task, token)) = retry.take() {
                    token.cancel();
                    task.await.unwrap()?;
                }

                if !Self::send_peers_to_raft(new_peers.clone(), &state.main.command_sender).await? {
                    let cancellation_token = cancellation_token.child_token();

                    let task = tokio::spawn({
                        let new_peers = new_peers.clone();
                        let command_sender = state.main.command_sender.clone();
                        let cancellation_token = cancellation_token.clone();
                        async move {
                            loop {
                                if cancellation_token
                                    .run_until_cancelled(tokio::time::sleep(Duration::from_secs(1)))
                                    .await
                                    .is_none()
                                {
                                    break;
                                }

                                if Self::send_peers_to_raft(new_peers.clone(), &command_sender)
                                    .await?
                                {
                                    break;
                                }
                            }

                            Ok(())
                        }
                    });

                    retry = Some((task, cancellation_token));
                }

                for peer in new_peers.iter().filter(|peer| peer.0 != my_id) {
                    if {
                        state
                            .peers
                            .read()
                            .await
                            .known_peer_peers
                            .get(peer)
                            .is_some_and(|peers| peers.is_superset(&new_peers))
                    } {
                        continue;
                    }

                    let url = format!("http://localhost:{peer}/peers");

                    tracing::info!("Sending peers to {peer}");

                    let response = reqwest
                        .post(url)
                        .json(&PeersRequest {
                            id: Id(my_id),
                            peers: new_peers.clone(),
                        })
                        .send()
                        .and_then(reqwest::Response::json::<JsonResult<HashSet<Id>, _>>)
                        .map_err(Into::into)
                        .and_then(|res| {
                            future::ready(
                                Result::from(res).map_err(|(_, string)| Error::Foreign(string)),
                            )
                        })
                        .await;

                    match response {
                        Ok(new_peers) => {
                            {
                                state
                                    .peers
                                    .write()
                                    .await
                                    .known_peer_peers
                                    .entry(*peer)
                                    .or_default()
                                    .extend(&new_peers);
                            }

                            state
                                .new_peers_sender
                                .send(new_peers)
                                .await
                                .map_err(|_| Error::PeersSend)?;
                        }
                        Err(e) => {
                            tracing::error!("Failed to send peers to {peer}: {e}");
                        }
                    }
                }
            }
        }

        if let Some((task, token)) = retry.take() {
            token.cancel();
            task.await.unwrap()?;
        }

        Ok(())
    }

    async fn send_peers_to_raft(
        peers: HashSet<Id>,
        command_sender: &mpsc::Sender<raft::CommandChannelItem<Client>>,
    ) -> Result<bool> {
        tracing::info!("Sending peers to Raft");

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
                    tracing::info!("Configuration updated successfully");
                    true
                }
                Err((raft::CommandError::NotEnoughNodes, _)) => {
                    tracing::info!("Not enough nodes in configuration");
                    true
                }
                Err((e, _)) => {
                    tracing::info!("Failed to update configuration: {e:?}");
                    false
                }
            },
        )
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let cli = Arc::new(Cli::parse());
    let client_builder = ClientBuilder::new(cli.clone());

    let peers = cli
        .peers
        .iter()
        .copied()
        .chain(Some(cli.port))
        .map(Id)
        .collect::<HashSet<_>>();

    let raft_config = RaftConfig {
        n: cli.max_nodes,
        heartbeat_interval: cli.heartbeat_interval,
    };

    let raft = Raft::<Client>::new(
        raft_config,
        "main".to_string(),
        peers,
        CancellationToken::new(),
        client_builder,
    )
    .await?;

    raft.run().await.map_err(Into::into)
}
