use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use futures::{
    future::{self, FutureExt as _, TryFutureExt as _},
    stream::{FuturesUnordered, StreamExt as _, TryStreamExt as _},
};
use hyve_db::{
    db::{self, Database},
    raft::{
        self, Client as _, CommandHandle, Config as RaftConfig, IncomingCommand, PersistentState,
        PersistentStateDbCommand, Raft, Rpc, RpcData,
    },
};
use mur3::murmurhash3_x86_32;
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::{
    Client, Id, RaftState,
    cli::Cli,
    error::{Error, Result},
};

#[derive(Debug)]
pub struct ReplicationClientBuilder {
    hash: u32,
    cli: Cli,
    main_client: Weak<Client>,
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
            new_peers_sender,
            persistent_state_db: Mutex::new(Some(persistent_state_db)),
            persistent_state_command_sender,
            should_shutdown: Arc::new(AtomicBool::new(false)),
            new_peers_receiver: Arc::new(Mutex::new(new_peers_receiver)),
            cancellation_token,
        }))
    }
}

#[derive(Debug)]
pub struct ReplicationClient {
    my_id: Id,
    hash: u32,
    cli: Cli,
    main_client: Weak<Client>,
    pub state: RaftState<Self>,
    new_peers_sender: mpsc::Sender<HashSet<Id>>,
    persistent_state_db: Mutex<Option<Database<PersistentStateDbCommand<Id, ()>>>>,
    persistent_state_command_sender:
        mpsc::Sender<db::CommandChannelItem<PersistentStateDbCommand<Id, ()>>>,
    should_shutdown: Arc<AtomicBool>,
    new_peers_receiver: Arc<Mutex<mpsc::Receiver<HashSet<Id>>>>,
    cancellation_token: CancellationToken,
}

impl raft::Client for ReplicationClient {
    type Error = Error;
    type Id = Id;
    type Command = ();
    type RpcId = Ulid;
    type Builder = ReplicationClientBuilder;

    fn id(self: &Arc<Self>) -> Id {
        // tracing::info!(hash=%self.hash, "Getting client ID");

        self.my_id
    }

    async fn send_persistent_state_command(
        self: &Arc<Self>,
        command: PersistentStateDbCommand<Self::Id, Self::Command>,
    ) -> Result<Option<PersistentState<Self::Id, Self::Command>>> {
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

    async fn send_rpc(self: &Arc<Self>, id: Id, rpc: RpcData<Rpc<Id, ()>>) -> Result<Ulid> {
        self.main_client
            .upgrade()
            .ok_or(Error::ClientDropped)?
            .send_replication_rpc(id, self.hash, rpc, self.state.rpc_response_sender.clone())
            .await
    }

    async fn apply_command(self: &Arc<Self>, _command: &(), _origin: Id) -> Result<()> {
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

        let _guard = self.cancellation_token.clone().drop_guard();

        tokio::try_join!(
            peers_task.map(Result::unwrap),
            persistent_state_db_task.map(Result::unwrap),
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

    async fn new_peers(self: &Arc<Self>, peers: HashSet<Id>) -> Result<()> {
        self.new_peers_sender
            .send(peers)
            .await
            .map_err(|_| Error::PeersSend)
    }

    async fn schedule_shutdown(self: &Arc<Self>, peers: HashSet<Id>) -> Result<()> {
        self.should_shutdown.store(true, Ordering::Release);
        self.new_peers(peers).await
    }

    async fn send_peers_to_raft(
        peers: HashSet<Id>,
        command_sender: &mpsc::Sender<raft::CommandChannelItem<Client>>,
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
                Err(raft::CommandError::NotEnoughNodes) => {
                    tracing::info!(%hash, "Not enough nodes in configuration");
                    true
                }
                Err(e) => {
                    tracing::info!(%hash, "Failed to update configuration: {e:?}");
                    false
                }
            },
        )
    }

    async fn run_db(
        port: u16,
        my_hash: u32,
        db: Database<PersistentStateDbCommand<Id, ()>>,
        should_shutdown: Arc<AtomicBool>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        db.run(cancellation_token).await?;

        if should_shutdown.load(Ordering::Acquire) {
            tokio::fs::remove_file(format!("{port}_replication_{my_hash}.db3")).await?;
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct ReplicationRaftState {
    raft_task: JoinHandle<Result<()>>,
    pub client: Arc<ReplicationClient>,
}

#[derive(Debug)]
pub struct Peers {
    my_hash: u32,
    cli: Cli,
    client: Weak<Client>,
    pub known_peers: HashSet<Id>,
    pub known_peer_peers: HashMap<Id, HashSet<Id>>,
    pub peers: HashSet<Id>,
    hashes: BTreeMap<u32, Id>,
    pub replication_states: HashMap<u32, ReplicationRaftState>,
    abandoned_tasks_sender: mpsc::Sender<Vec<JoinHandle<Result<()>>>>,
    abandoned_tasks_task: JoinHandle<Result<()>>,
    cancellation_token: CancellationToken,
}

impl Peers {
    pub fn new(cli: Cli, client: Weak<Client>, cancellation_token: CancellationToken) -> Self {
        let my_id = Id(cli.port);
        let my_hash = Self::hash(&my_id, cli.seed);

        tracing::info!(hash=%my_hash, "Initializing peers");

        let (abandoned_tasks_sender, mut abandoned_tasks_receiver) = mpsc::channel(5);
        let abandoned_tasks_task = tokio::spawn({
            let cancellation_token = cancellation_token.clone();
            async move {
                let mut abandoned_tasks = FuturesUnordered::new();
                loop {
                    tokio::select! {
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                        Some(tasks) = abandoned_tasks_receiver.recv() => {
                            for task in tasks {
                                abandoned_tasks.push(task);
                            }
                        }
                        res = abandoned_tasks.try_next().map(Result::unwrap) => {
                            if let Some(res) = res {
                                res?;
                            }
                        }
                        else => {
                            break;
                        }
                    }
                }

                abandoned_tasks
                    .map(Result::unwrap)
                    .try_fold((), |_, _| future::ok(()))
                    .await
            }
        });

        Self {
            my_hash,
            cli,
            client,
            known_peers: HashSet::from([my_id]),
            known_peer_peers: HashMap::new(),
            peers: HashSet::new(),
            hashes: BTreeMap::new(),
            replication_states: HashMap::new(),
            abandoned_tasks_sender,
            abandoned_tasks_task,
            cancellation_token,
        }
    }

    pub async fn finalize(&mut self) -> Result<()> {
        self.cancellation_token.cancel();

        future::try_join_all(
            self.replication_states
                .drain()
                .map(async |(_, state)| state.raft_task.await.unwrap()),
        )
        .await?;

        (&mut self.abandoned_tasks_task).await.unwrap()
    }

    pub fn extend_known(&mut self, iter: impl IntoIterator<Item = Id>) -> Option<HashSet<Id>> {
        let old_len = self.known_peers.len();
        self.known_peers.extend(iter);
        (old_len < self.known_peers.len()).then(|| self.known_peers.clone())
    }

    pub async fn extend(&mut self, iter: impl IntoIterator<Item = Id>) -> Result<()> {
        let mut inserted = false;
        for new_peer in iter {
            if self.peers.insert(new_peer) {
                let hash = Self::hash(&new_peer, self.cli.seed);
                self.hashes.insert(hash, new_peer);
                inserted = true;
            }
        }

        if inserted {
            self.update_replication().await?;
        }

        Ok(())
    }

    async fn update_replication(&mut self) -> Result<()> {
        if self.peers.len() <= self.cli.max_nodes / 2
            || self.peers.len() < self.cli.replication_factor
            || !self.hashes.contains_key(&self.my_hash)
        {
            return Ok(());
        }

        let replications = Self::ring(&self.hashes, self.my_hash)
            .rev()
            .take(self.cli.replication_factor - 1)
            .map(|(hash, _)| *hash)
            .chain(Some(self.my_hash))
            .collect::<HashSet<_>>();

        println!("Replications: {replications:?}");

        let abandoned_tasks = self
            .replication_states
            .extract_if(|hash, _| !replications.contains(hash))
            .map(async |(hash, state)| {
                state
                    .client
                    .schedule_shutdown(Self::peers(hash, &self.hashes, &self.cli))
                    .await?;
                Ok::<_, Error>(state.raft_task)
            })
            .collect::<FuturesUnordered<_>>();

        for replication_hash in replications {
            let peers = Self::peers(replication_hash, &self.hashes, &self.cli);

            match self.replication_states.entry(replication_hash) {
                Entry::Vacant(entry) => {
                    let client_builder = ReplicationClientBuilder {
                        hash: replication_hash,
                        cli: self.cli.clone(),
                        main_client: self.client.clone(),
                    };

                    let raft_config = RaftConfig {
                        n: self.cli.replication_factor,
                        heartbeat_interval: self.cli.heartbeat_interval,
                    };

                    let cancellation_token = self.cancellation_token.child_token();

                    let raft = Raft::<ReplicationClient>::new(
                        raft_config,
                        format!("{replication_hash}"),
                        peers,
                        cancellation_token.clone(),
                        client_builder,
                    )
                    .await?;

                    let client = raft.client.clone();

                    let raft_task = tokio::spawn(raft.run().err_into());

                    entry.insert(ReplicationRaftState { raft_task, client });
                }
                Entry::Occupied(entry) => {
                    entry.get().client.new_peers(peers).await?;
                }
            }
        }

        self.abandoned_tasks_sender
            .send(abandoned_tasks.try_collect().await?)
            .await
            .map_err(|_| Error::AbandonedTasksSend)?;

        Ok(())
    }

    fn peers(hash: u32, hashes: &BTreeMap<u32, Id>, cli: &Cli) -> HashSet<Id> {
        Self::ring(hashes, hash)
            .take(cli.replication_factor)
            .map(|(_, id)| *id)
            .collect()
    }

    fn ring(
        hashes: &BTreeMap<u32, Id>,
        start_hash: u32,
    ) -> impl DoubleEndedIterator<Item = (&u32, &Id)> + Clone {
        hashes.range(start_hash..).chain(hashes.range(..start_hash))
    }

    fn hash(id: &Id, seed: u32) -> u32 {
        murmurhash3_x86_32(&id.0.to_le_bytes(), seed)
    }
}
