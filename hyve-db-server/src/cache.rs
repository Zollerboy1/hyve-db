use std::{collections::HashMap, hash::Hash, sync::Arc, time::Duration};

use futures::stream::StreamExt as _;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::{
    sync::CancellationToken,
    time::{DelayQueue, delay_queue},
};

use crate::error::{Error, Result};

#[derive(Debug)]
struct CacheStorage<K: Eq + Hash + Clone, V> {
    timeout: Duration,
    entries: HashMap<K, (V, delay_queue::Key)>,
    expirations: DelayQueue<K>,
    command_receiver: mpsc::Receiver<Command<K, V>>,
    expiration_sender: mpsc::Sender<(K, V)>,
}

#[derive(Debug)]
enum Command<K: Eq + Hash + Clone, V> {
    Insert {
        key: K,
        value: V,
        sender: oneshot::Sender<()>,
    },
    Remove {
        key: K,
        sender: oneshot::Sender<Option<V>>,
    },
}

#[derive(Debug)]
pub struct Cache<K: Eq + Hash + Clone, V> {
    command_sender: mpsc::Sender<Command<K, V>>,
    storage: Arc<Mutex<Option<CacheStorage<K, V>>>>,
}

impl<K: Eq + Hash + Clone, V> Cache<K, V> {
    pub fn new(timeout: Duration, expiration_sender: mpsc::Sender<(K, V)>) -> Arc<Self> {
        let (command_sender, command_receiver) = mpsc::channel(5);

        let storage = CacheStorage {
            timeout,
            entries: HashMap::new(),
            expirations: DelayQueue::new(),
            command_receiver,
            expiration_sender,
        };

        Arc::new(Self {
            command_sender,
            storage: Arc::new(Mutex::new(Some(storage))),
        })
    }

    pub async fn insert(self: &Arc<Self>, key: K, value: V) -> Result<()> {
        let (sender, receiver) = oneshot::channel();

        let command = Command::Insert { key, value, sender };

        self.command_sender
            .send(command)
            .await
            .map_err(|_| Error::CommandSend)?;

        receiver.await.map_err(|_| Error::CommandResponseReceive)
    }

    pub async fn remove(self: &Arc<Self>, key: K) -> Result<Option<V>> {
        let (sender, receiver) = oneshot::channel();

        let command = Command::Remove { key, sender };

        self.command_sender
            .send(command)
            .await
            .map_err(|_| Error::CommandSend)?;

        receiver.await.map_err(|_| Error::CommandResponseReceive)
    }

    pub async fn run(self: Arc<Self>, cancellation_token: CancellationToken) -> Result<()> {
        let Some(CacheStorage {
            timeout,
            mut entries,
            mut expirations,
            mut command_receiver,
            expiration_sender,
        }) = self.storage.lock().await.take()
        else {
            return Ok(());
        };

        loop {
            tokio::select! {
                _ = cancellation_token.cancelled() => {
                    break;
                }
                Some(command) = command_receiver.recv() => {
                    match command {
                        Command::Insert { key, value, sender } => {
                            let delay_key = expirations.insert(key.clone(), timeout);
                            entries.insert(key, (value, delay_key));

                            sender.send(()).map_err(|_| Error::CommandResponseSend)?;
                        }
                        Command::Remove { key, sender } => {
                            let value = if let Some((value, delay_key)) = entries.remove(&key) {
                                expirations.remove(&delay_key);
                                Some(value)
                            } else {
                                None
                            };

                            sender.send(value).map_err(|_| Error::CommandResponseSend)?;
                        }
                    }
                }
                Some(expired) = expirations.next() => {
                    let key = expired.into_inner();
                    let value = entries.remove(&key).ok_or(Error::ExpiredCacheEntryNotFound)?.0;

                    expiration_sender.send((key, value)).await.map_err(|_| Error::ExpiredCacheEntrySend)?;
                }
                else => {
                    break;
                }
            }
        }

        Ok(())
    }
}
