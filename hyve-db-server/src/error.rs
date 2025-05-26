use std::{collections::TryReserveError, io};

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use hyve_db::{db, raft};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{Id, command::TransactionCommandResponse};

#[derive(Debug, Clone, Error, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum CommandError {
    #[error("{message}")]
    Error { message: String },
    #[error("Leader ID: {}", leader.0)]
    Leader { leader: Id },
    #[error("No leader")]
    NoLeader,
}

impl From<raft::CommandError<Id>> for CommandError {
    fn from(error: raft::CommandError<Id>) -> Self {
        match error {
            raft::CommandError::Leader(id) => Self::Leader { leader: id },
            raft::CommandError::NoLeader => Self::NoLeader,
            raft::CommandError::AlreadyProcessingConfigurationChange => Self::Error {
                message: "Already processing configuration change".to_string(),
            },
            raft::CommandError::NotEnoughNodes => Self::Error {
                message: "Not enough nodes".to_string(),
            },
            raft::CommandError::TooManyNodes => Self::Error {
                message: "Too many nodes".to_string(),
            },
            raft::CommandError::NotAMember => Self::Error {
                message: "Not a member".to_string(),
            },
        }
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("Raft error: {0}")]
    Raft(#[from] Box<raft::Error<Self>>),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("Database error: {0}")]
    Db(#[from] db::Error),
    #[error("Encoding error: {0}")]
    Encoding(#[from] cbor4ii::serde::EncodeError<TryReserveError>),
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionCommandResponse),
    #[error("Id parsing error: {0}")]
    Id(#[from] ulid::DecodeError),
    #[error("Parser error: {0}")]
    Parser(String),
    #[error("Foreign error: {0}")]
    Foreign(String),
    #[error("Could not send command")]
    CommandSend,
    #[error("Could not send command response")]
    CommandResponseSend,
    #[error("Could not recieve command response")]
    CommandResponseReceive,
    #[error("Could not send peers")]
    PeersSend,
    #[error("Could not send database command")]
    DbCommandSend,
    #[error("Database response channel closed")]
    DbResponseChannelClosed,
    #[error("Client dropped")]
    ClientDropped,
    #[error("Exited configuration")]
    ExitedConfiguration,
    #[error("Could not send abandoned tasks")]
    AbandonedTasksSend,
    #[error("Could not find expired cache entry")]
    ExpiredCacheEntryNotFound,
    #[error("Could not send expired cache entry")]
    ExpiredCacheEntrySend,
    #[error("Command timed out")]
    CommandTimeout,
    #[error("Replication hash not found")]
    ReplicationHashNotFound,
}

impl From<raft::Error<Self>> for Error {
    fn from(error: raft::Error<Self>) -> Self {
        Self::Raft(Box::new(error))
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum JsonResult<T, E> {
    #[serde(rename = "success")]
    Ok { data: T },
    #[serde(rename = "error")]
    Err { error: E, code: u16 },
}

impl<T: Serialize, E: Serialize> IntoResponse for JsonResult<T, E> {
    fn into_response(self) -> Response {
        match &self {
            JsonResult::Ok { .. } => (StatusCode::OK, Json(self)).into_response(),
            JsonResult::Err { code, .. } => (
                StatusCode::from_u16(*code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                Json(self),
            )
                .into_response(),
        }
    }
}

impl<T, E> From<Result<T, (StatusCode, E)>> for JsonResult<T, E> {
    fn from(result: Result<T, (StatusCode, E)>) -> Self {
        match result {
            Ok(data) => JsonResult::Ok { data },
            Err((status_code, error)) => JsonResult::Err {
                error,
                code: status_code.into(),
            },
        }
    }
}

impl<T, E> From<JsonResult<T, E>> for Result<T, (StatusCode, E)> {
    fn from(json_result: JsonResult<T, E>) -> Self {
        match json_result {
            JsonResult::Ok { data } => Ok(data),
            JsonResult::Err { error, code } => Err((
                StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                error,
            )),
        }
    }
}
