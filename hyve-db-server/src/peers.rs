use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    sync::{Arc, Weak},
    time::Duration,
};

use futures::{
    future::{self, FutureExt as _, OptionFuture, TryFutureExt as _},
    stream::{self, FuturesUnordered, StreamExt as _, TryStreamExt as _},
};
use hyve_db::raft::{Client as _, CommandError, Config as RaftConfig, Raft};
use indexmap::IndexMap;
use mur3::murmurhash3_x86_32;
use tokio::{
    sync::{RwLock, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::{
    Client, Id, MainCommandResponse,
    cli::Cli,
    command::{
        Command, ReadCommand, TransactionCommand, TransactionCommandResponse, Value, WriteCommand,
    },
    error::{self, Error, Result},
    replication_client::{ReplicationClient, ReplicationClientBuilder},
};

#[derive(Debug)]
pub struct ReplicationRaftState {
    raft_task: JoinHandle<Result<()>>,
    pub client: Arc<ReplicationClient>,
}

#[derive(Debug)]
pub struct Peers {
    my_hash: u32,
    cli: Arc<Cli>,
    client: Weak<Client>,
    pub known_peers: HashSet<Id>,
    pub known_peer_peers: HashMap<Id, HashSet<Id>>,
    pub peers: HashSet<Id>,
    hashes: BTreeMap<u32, (Id, Option<Id>)>,
    pub replication_states: HashMap<u32, ReplicationRaftState>,
    abandoned_tasks_sender: mpsc::Sender<Vec<JoinHandle<Result<()>>>>,
    abandoned_tasks_task: Option<JoinHandle<Result<()>>>,
    command_queue_sender: mpsc::Sender<(Command, oneshot::Sender<Result<MainCommandResponse>>)>,
    command_queue_task: Option<JoinHandle<Result<()>>>,
    cancellation_token: CancellationToken,
}

impl Peers {
    pub fn new(
        cli: Arc<Cli>,
        client: Weak<Client>,
        cancellation_token: CancellationToken,
    ) -> Arc<RwLock<Self>> {
        let my_id = Id(cli.port);
        let my_hash = Self::hash_id(&my_id, cli.seed);

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

        let (command_queue_sender, command_queue_receiver) = mpsc::channel(5);

        Arc::new_cyclic(|this| {
            RwLock::new(Self {
                my_hash,
                cli: cli.clone(),
                client: client.clone(),
                known_peers: HashSet::from([my_id]),
                known_peer_peers: HashMap::new(),
                peers: HashSet::new(),
                hashes: BTreeMap::new(),
                replication_states: HashMap::new(),
                abandoned_tasks_sender,
                abandoned_tasks_task: Some(abandoned_tasks_task),
                command_queue_sender,
                command_queue_task: Some(tokio::spawn(Self::run_command_queue(
                    this.clone(),
                    cli,
                    client,
                    command_queue_receiver,
                    cancellation_token.clone(),
                ))),
                cancellation_token,
            })
        })
    }

    pub async fn queue_command(
        &self,
        command: Command,
        sender: oneshot::Sender<Result<MainCommandResponse>>,
    ) -> Result<()> {
        self.command_queue_sender
            .send((command, sender))
            .await
            .map_err(|_| Error::CommandSend)
    }

    pub async fn finalize(&mut self) -> Result<()> {
        self.cancellation_token.cancel();

        tokio::try_join!(
            future::try_join_all(
                self.replication_states
                    .drain()
                    .map(async |(_, state)| state.raft_task.await.unwrap()),
            ),
            OptionFuture::from(self.abandoned_tasks_task.take())
                .map(Option::transpose)
                .map(Result::unwrap)
                .map(Option::transpose),
            OptionFuture::from(self.command_queue_task.take())
                .map(Option::transpose)
                .map(Result::unwrap)
                .map(Option::transpose),
        )?;

        Ok(())
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
                let hash = Self::hash_id(&new_peer, self.cli.seed);
                self.hashes.insert(hash, (new_peer, None));
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
            .map(|(hash, _, _)| *hash)
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
                    let client_builder = ReplicationClientBuilder::new(
                        replication_hash,
                        self.cli.clone(),
                        self.client.clone(),
                    );

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

    async fn run_command_queue(
        this: Weak<RwLock<Self>>,
        cli: Arc<Cli>,
        client: Weak<Client>,
        mut receiver: mpsc::Receiver<(Command, oneshot::Sender<Result<MainCommandResponse>>)>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        let _guard = cancellation_token.clone().drop_guard();

        let child_token = cancellation_token.child_token();

        let mut command_tasks = FuturesUnordered::new();

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                command = receiver.recv() => {
                    let Some((command, sender)) = command else {
                        break;
                    };

                    command_tasks.push(
                        child_token.run_until_cancelled(
                            Self::run_command(this.clone(), cli.clone(), client.clone(), command, sender)
                        ).map(Option::transpose)
                    );
                }
                Some(res) = command_tasks.next() => {
                    _ = res?;
                    tracing::info!("Task completed");
                }
            };
        }

        child_token.cancel();

        command_tasks.try_fold((), |_, _| future::ok(())).await
    }

    async fn run_command(
        this: Weak<RwLock<Self>>,
        cli: Arc<Cli>,
        client: Weak<Client>,
        command: Command,
        sender: oneshot::Sender<Result<MainCommandResponse>>,
    ) -> Result<()> {
        match tokio::time::timeout(
            cli.command_timeout,
            Self::run_command_impl(this, cli, client, command),
        )
        .await
        {
            Ok(Ok(response)) => sender
                .send(response)
                .map_err(|_| Error::CommandResponseSend),
            Ok(Err(err)) => Err(err),
            Err(_) => {
                tracing::error!("Command timed out");
                sender
                    .send(Err(Error::CommandTimeout))
                    .map_err(|_| Error::CommandResponseSend)
            }
        }
    }

    async fn run_command_impl(
        this: Weak<RwLock<Self>>,
        cli: Arc<Cli>,
        client: Weak<Client>,
        command: Command,
    ) -> Result<Result<MainCommandResponse>> {
        match command {
            Command::Read(ReadCommand::Select(command)) => {
                let transaction = Arc::new(TransactionCommand::Read(ReadCommand::Select(
                    command.with_id_column(),
                )));

                Ok(
                    Self::run_transaction_on_all_replications(this, client, transaction)
                        .await?
                        .and_then(|(_, response)| {
                            future::ready(match response {
                                TransactionCommandResponse::Rows(rows) => {
                                    Ok(stream::iter(rows.into_iter().map(|mut row| {
                                        if let Some(Value::Text(id_string)) = row.pop() {
                                            let id = id_string.parse::<Ulid>()?;
                                            Ok((id, row))
                                        } else {
                                            Err(TransactionCommandResponse::Failure(
                                                "Missing id column".to_string(),
                                            )
                                            .into())
                                        }
                                    })))
                                }
                                response => Err(response.into()),
                            })
                        })
                        .try_flatten()
                        .try_collect()
                        .await
                        .map(IndexMap::<_, _>::into_values)
                        .map(Iterator::collect)
                        .map(|rows| MainCommandResponse::Rows { rows }),
                )
            }
            Command::Write(WriteCommand::Insert(mut command)) => {
                let id = *command.get_or_insert_id();
                let transaction = Arc::new(TransactionCommand::Run(WriteCommand::Insert(command)));

                let hash = Self::hash_db_id(&id, cli.seed);

                match Self::run_transaction(this, client, transaction, hash).await? {
                    TransactionCommandResponse::Success(_) => {
                        Ok(Ok(MainCommandResponse::Inserted { id }))
                    }
                    response => Ok(Err(response.into())),
                }
            }
        }
    }

    async fn run_transaction_on_all_replications(
        this: Weak<RwLock<Self>>,
        client: Weak<Client>,
        transaction: Arc<TransactionCommand>,
    ) -> Result<
        FuturesUnordered<
            impl Future<Output = Result<(u32, TransactionCommandResponse)>> + Send + 'static,
        >,
    > {
        let replication_hashes = {
            let this = this.upgrade().ok_or(Error::ClientDropped)?;
            this.read().await.hashes.keys().copied().collect::<Vec<_>>()
        };

        tracing::info!("Running transaction on all replications");

        async fn run_transaction_helper(
            this: Weak<RwLock<Peers>>,
            client: Weak<Client>,
            transaction: Arc<TransactionCommand>,
            hash: u32,
        ) -> Result<(u32, TransactionCommandResponse)> {
            let response = Peers::run_transaction(this, client, transaction.clone(), hash).await?;
            Ok((hash, response))
        }

        Ok(replication_hashes
            .into_iter()
            .map({
                move |hash| {
                    run_transaction_helper(this.clone(), client.clone(), transaction.clone(), hash)
                }
            })
            .collect())
    }

    async fn run_transaction(
        this: Weak<RwLock<Self>>,
        client: Weak<Client>,
        mut transaction: Arc<TransactionCommand>,
        hash: u32,
    ) -> Result<TransactionCommandResponse> {
        let (replication_hash, id, mut leader) = {
            let this = this.upgrade().ok_or(Error::ClientDropped)?;
            let this = this.read().await;

            let Some((replication_hash, id, leader)) = Self::ring(&this.hashes, hash).next() else {
                return Ok(TransactionCommandResponse::Failure("No peers".into()));
            };

            (*replication_hash, *id, *leader)
        };

        let mut retries = 0;

        loop {
            let id = leader.unwrap_or(id);
            let my_id = client.upgrade().ok_or(Error::ClientDropped)?.id();

            tracing::info!(leader=?leader, "Running transaction for {replication_hash} on {id}");

            let res = if my_id == id {
                let replication_client = {
                    let this = this.upgrade().ok_or(Error::ClientDropped)?;
                    this.read()
                        .await
                        .replication_states
                        .get(&replication_hash)
                        .ok_or(Error::ReplicationHashNotFound)?
                        .client
                        .clone()
                };

                match replication_client
                    .send_command(Arc::unwrap_or_clone(transaction), my_id)
                    .await?
                {
                    Ok(response) => Ok(response),
                    Err((CommandError::Leader(id), transaction)) => {
                        Err((Arc::new(transaction), Some(id)))
                    }
                    Err((CommandError::NoLeader, transaction)) => {
                        Err((Arc::new(transaction), None))
                    }
                    Err((e, _)) => {
                        let error = error::CommandError::from(e);
                        Ok(TransactionCommandResponse::Failure(format!(
                            "Failed to send command to raft: {error}"
                        )))
                    }
                }
            } else {
                match client
                    .upgrade()
                    .ok_or(Error::ClientDropped)?
                    .send_replication_transaction(id, replication_hash, transaction.as_ref())
                    .await?
                {
                    TransactionCommandResponse::Leader(id) => Err((transaction, Some(id))),
                    TransactionCommandResponse::NoLeader => Err((transaction, None)),
                    response => Ok(response),
                }
            };

            break match res {
                Ok(response) => Ok(response),
                Err((new_transaction, new_leader)) if retries < 3 => {
                    transaction = new_transaction;
                    leader = new_leader;
                    retries += 1;

                    if new_leader.is_none() {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }

                    continue;
                }
                _ => Ok(TransactionCommandResponse::Failure(
                    "Too many retries".to_string(),
                )),
            };
        }
    }

    fn peers(hash: u32, hashes: &BTreeMap<u32, (Id, Option<Id>)>, cli: &Cli) -> HashSet<Id> {
        Self::ring(hashes, hash)
            .take(cli.replication_factor)
            .map(|(_, id, _)| *id)
            .collect()
    }

    fn ring(
        hashes: &BTreeMap<u32, (Id, Option<Id>)>,
        start_hash: u32,
    ) -> impl DoubleEndedIterator<Item = (&u32, &Id, &Option<Id>)> + Clone {
        hashes
            .range(start_hash..)
            .map(|(hash, (id, owner))| (hash, id, owner))
            .chain(
                hashes
                    .range(..start_hash)
                    .map(|(hash, (id, owner))| (hash, id, owner)),
            )
    }

    fn hash_id(id: &Id, seed: u32) -> u32 {
        murmurhash3_x86_32(&id.0.to_le_bytes(), seed)
    }

    fn hash_db_id(id: &Ulid, seed: u32) -> u32 {
        murmurhash3_x86_32(&id.to_bytes(), seed)
    }
}
