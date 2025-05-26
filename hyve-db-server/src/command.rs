use std::{borrow::Cow, iter, ops::Deref, sync::Arc, time::Instant};

use hyve_db::db;
use rusqlite::{
    Connection, DropBehavior, ToSql,
    types::{FromSql, FromSqlResult, ToSqlOutput, Value as RusqliteValue, ValueRef},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;
use ulid::Ulid;

use crate::{Id, cli::Cli};

mod parser;

#[derive(Debug, Clone, Error)]
pub enum ConversionError {
    #[error("Invalid number type")]
    InvalidNumberType,
    #[error("Invalid byte array element")]
    InvalidByteArrayElement,
    #[error("Could not convert array element to byte: {0}")]
    ByteArrayElementConversion(#[from] std::num::TryFromIntError),
    #[error("Invalid type")]
    InvalidType,
    #[error("Number is not finite")]
    NumberNotFinite,
}

#[derive(Debug, Clone, Error)]
enum Error {
    #[error("Tried to execute insert command without id")]
    InsertWithoutId,
    #[error("Distributed table '{0}' does not have an 'id' column")]
    IdColumnNotFound(String),
    #[error("The type of the 'id' column in distributed table '{0}' is not TEXT")]
    IdColumnNotText(String),
    #[error("The 'id' column in distributed table '{0}' is not a primary key")]
    IdColumnNotPk(String),
    #[error("Inserted rows into distributed table '{0}' during migration")]
    TableHasRows(String),
}

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<RusqliteValue> for Value {
    fn from(value: RusqliteValue) -> Self {
        match value {
            RusqliteValue::Null => Self::Null,
            RusqliteValue::Integer(i) => Self::Integer(i),
            RusqliteValue::Real(f) => Self::Real(f),
            RusqliteValue::Text(s) => Self::Text(s),
            RusqliteValue::Blob(b) => Self::Blob(b),
        }
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = ConversionError;

    fn try_from(value: serde_json::Value) -> Result<Self, ConversionError> {
        Ok(match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(b) => Self::Integer(b.into()),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Self::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    Self::Real(f)
                } else {
                    return Err(ConversionError::InvalidNumberType);
                }
            }
            serde_json::Value::String(s) => Self::Text(s),
            serde_json::Value::Array(a) => Self::Blob(
                a.into_iter()
                    .map(|v| {
                        v.as_u64()
                            .ok_or(ConversionError::InvalidByteArrayElement)
                            .and_then(|v| u8::try_from(v).map_err(Into::into))
                    })
                    .collect::<Result<_, _>>()?,
            ),
            _ => {
                return Err(ConversionError::InvalidType);
            }
        })
    }
}

impl<'a> From<&'a Value> for ValueRef<'a> {
    fn from(value: &'a Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Integer(i) => Self::Integer(*i),
            Value::Real(f) => Self::Real(*f),
            Value::Text(s) => Self::Text(s.as_bytes()),
            Value::Blob(b) => Self::Blob(b.as_ref()),
        }
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> FromSqlResult<Self> {
        Ok(RusqliteValue::from(value).into())
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(self))
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Integer(i) => i.serialize(serializer),
            Self::Real(f) => f.serialize(serializer),
            Self::Text(s) => serializer.serialize_str(s),
            Self::Blob(v) => v.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        serde_json::Value::deserialize(deserializer)?
            .try_into()
            .map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResultColumn {
    Wildcard,
    Column(String),
}

impl ResultColumn {
    fn escaped_sql(&self) -> Cow<'_, str> {
        match self {
            Self::Wildcard => "*".into(),
            Self::Column(name) => pg_escape::quote_identifier(&name),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BinOp {
    Eq,
    Is,
    IsNot,
    And,
    Or,
}

impl BinOp {
    fn escaped_sql(&self) -> Cow<'_, str> {
        match self {
            Self::Eq => "=".into(),
            Self::Is => "IS".into(),
            Self::IsNot => "IS NOT".into(),
            Self::And => "AND".into(),
            Self::Or => "OR".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Expr {
    Value(Value),
    Column(String),
    BinOp(BinOp, Box<Expr>, Box<Expr>),
}

impl Expr {
    fn escaped_sql_and_params(&self) -> (Cow<'_, str>, Vec<&Value>) {
        match self {
            Self::Value(value) => ("?".into(), vec![value]),
            Self::Column(name) => (pg_escape::quote_identifier(&name), vec![]),
            Self::BinOp(op, left, right) => {
                let (left_sql, left_params) = left.escaped_sql_and_params();
                let (right_sql, right_params) = right.escaped_sql_and_params();
                let op_sql = op.escaped_sql();
                (
                    format!("({left_sql} {op_sql} {right_sql})").into(),
                    [&left_params[..], &right_params[..]].concat(),
                )
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectCommand {
    table: String,
    result_columns: Vec<ResultColumn>,
    where_clause: Option<Expr>,
}

impl SelectCommand {
    pub fn with_id_column(mut self) -> Self {
        self.result_columns.push(ResultColumn::Column("id".into()));
        self
    }

    fn execute(self, conn: impl Deref<Target = Connection>) -> db::Result<Vec<Vec<Value>>> {
        let escaped_table = pg_escape::quote_identifier(&self.table);
        let escaped_result_columns = self
            .result_columns
            .iter()
            .map(ResultColumn::escaped_sql)
            .intersperse(", ".into())
            .collect::<String>();

        if let Some(where_clause) = self.where_clause {
            let (where_sql, where_params) = where_clause.escaped_sql_and_params();

            let mut stmt = conn.prepare(&format!(
                "SELECT {escaped_result_columns} FROM {escaped_table} WHERE {where_sql}"
            ))?;

            stmt.query_map(rusqlite::params_from_iter(where_params), |row| {
                (0..row.as_ref().column_count())
                    .map(|i| row.get(i))
                    .collect()
            })?
            .collect::<Result<_, _>>()
            .map_err(Into::into)
        } else {
            let mut stmt = conn.prepare(&format!(
                "SELECT {escaped_result_columns} FROM {escaped_table}"
            ))?;

            stmt.query_map([], |row| {
                (0..row.as_ref().column_count())
                    .map(|i| row.get(i))
                    .collect()
            })?
            .collect::<Result<_, _>>()
            .map_err(Into::into)
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsertCommand {
    table: String,
    id: Option<Ulid>,
    columns: Vec<String>,
    values: Vec<Value>,
}

impl InsertCommand {
    pub fn get_or_insert_id(&mut self) -> &mut Ulid {
        self.id.get_or_insert_with(Ulid::new)
    }

    fn execute(&self, conn: impl Deref<Target = Connection>) -> db::Result<usize> {
        let Some(id) = self.id else {
            return Err(db::Error::Custom(Box::from(Error::InsertWithoutId)));
        };

        let escaped_table = pg_escape::quote_identifier(&self.table);

        let escaped_columns = self
            .columns
            .iter()
            .map(AsRef::as_ref)
            .map(pg_escape::quote_identifier)
            .chain(Some("id".into()))
            .intersperse(", ".into())
            .collect::<String>();

        let value_placeholders = iter::repeat_n("?", self.values.len() + 1)
            .intersperse(", ".into())
            .collect::<String>();

        conn.execute(
            &format!(
                "INSERT INTO {escaped_table} ({escaped_columns}) VALUES ({value_placeholders})"
            ),
            rusqlite::params_from_iter(
                self.values
                    .iter()
                    .chain(Some(&Value::Text(id.to_string().into()))),
            ),
        )
        .map_err(Into::into)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReadCommand {
    Select(SelectCommand),
}

impl ReadCommand {
    fn execute(self, conn: impl Deref<Target = Connection>) -> db::Result<Vec<Vec<Value>>> {
        match self {
            Self::Select(command) => command.execute(conn),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteCommand {
    Insert(InsertCommand),
}

impl WriteCommand {
    fn execute(&self, conn: impl Deref<Target = Connection>) -> db::Result<usize> {
        match self {
            Self::Insert(command) => command.execute(conn),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    Read(ReadCommand),
    Write(WriteCommand),
}

#[derive(Debug)]
pub struct DbState {
    cli: Arc<Cli>,
    transaction: Option<(Ulid, WriteCommand, Instant)>,
}

impl DbState {
    pub fn new(cli: Arc<Cli>) -> Self {
        Self {
            cli,
            transaction: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionCommand {
    Read(ReadCommand),
    Run(WriteCommand),
    TryAndRollback(WriteCommand),
    Commit(Ulid),
    Cancel(Ulid),
}

#[derive(Debug, Clone, Serialize, Deserialize, Error)]
pub enum TransactionCommandResponse {
    #[error("Unexpected rows")]
    Rows(Vec<Vec<Value>>),
    #[error("Unexpected success")]
    Success(usize),
    #[error("Unexpected rollback")]
    Rollback(Ulid),
    #[error("Transaction was cancelled")]
    Cancelled,
    #[error("{0}")]
    Failure(String),
    #[error("Raft leader id: {}", .0.0)]
    Leader(Id),
    #[error("Raft has no leader")]
    NoLeader,
}

impl TransactionCommand {
    fn execute_with_state_impl(
        self,
        conn: &mut Connection,
        state: &mut DbState,
    ) -> db::Result<TransactionCommandResponse> {
        Ok(match self {
            Self::Read(command) => TransactionCommandResponse::Rows(command.execute(conn)?),
            Self::Run(command) => {
                if let Some(transaction) = state.transaction.take() {
                    if Instant::now() < transaction.2 {
                        state.transaction = Some(transaction);
                        return Ok(TransactionCommandResponse::Failure(
                            "Transaction in progress".to_string(),
                        ));
                    }
                }

                TransactionCommandResponse::Success(command.execute(conn)?)
            }
            Self::TryAndRollback(command) => {
                if let Some(transaction) = state.transaction.take() {
                    if Instant::now() < transaction.2 {
                        state.transaction = Some(transaction);
                        return Ok(TransactionCommandResponse::Failure(
                            "Transaction in progress".to_string(),
                        ));
                    }
                }

                let tx = conn.transaction()?;
                command.execute(tx)?;

                let id = Ulid::new();
                state.transaction = Some((id, command, Instant::now() + state.cli.commit_timeout));

                TransactionCommandResponse::Rollback(id)
            }
            Self::Commit(commit_id) => {
                if let Some((id, command, timeout)) = state.transaction.take() {
                    if id == commit_id {
                        if Instant::now() > timeout {
                            TransactionCommandResponse::Failure(
                                "Transaction has timed out".to_string(),
                            )
                        } else {
                            let mut tx = conn.transaction()?;
                            tx.set_drop_behavior(DropBehavior::Commit);
                            TransactionCommandResponse::Success(command.execute(tx)?)
                        }
                    } else if Instant::now() > timeout {
                        TransactionCommandResponse::Failure(
                            "No transaction in progress".to_string(),
                        )
                    } else {
                        state.transaction = Some((id, command, timeout));
                        TransactionCommandResponse::Failure(
                            "Transaction in progress has different id".to_string(),
                        )
                    }
                } else {
                    TransactionCommandResponse::Failure("No transaction in progress".to_string())
                }
            }
            Self::Cancel(cancel_id) => {
                if let Some((id, command, timeout)) = state.transaction.take() {
                    if id == cancel_id {
                        TransactionCommandResponse::Cancelled
                    } else if Instant::now() > timeout {
                        TransactionCommandResponse::Failure(
                            "No transaction in progress".to_string(),
                        )
                    } else {
                        state.transaction = Some((id, command, timeout));
                        TransactionCommandResponse::Failure(
                            "Transaction in progress has different id".to_string(),
                        )
                    }
                } else {
                    TransactionCommandResponse::Failure("No transaction in progress".to_string())
                }
            }
        })
    }
}

impl db::Command for TransactionCommand {
    type State = DbState;
    type Output = TransactionCommandResponse;

    async fn migrate_with_state(conn: &mut Connection, state: &mut DbState) -> db::Result<()> {
        let migration = tokio::fs::read_to_string(&state.cli.migration_file)
            .await
            .map_err(Box::from)?;

        let tx = conn.transaction()?;
        tx.execute_batch(&migration)?;
        tx.commit()?;

        let mut stmt =
            conn.prepare("SELECT name FROM pragma_table_list WHERE name NOT LIKE 'sqlite_%';")?;
        let tables = stmt.query_map([], |row| row.get::<_, String>(0))?;

        let mut stmt = conn.prepare("SELECT name, type, pk FROM pragma_table_info(?);")?;
        for table in tables {
            let table = table?;

            let rows = stmt
                .query_map([&table], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, bool>(2)?,
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;

            let Some((_, type_, pk)) = rows.iter().find(|(name, _, _)| name == "id") else {
                return Err(db::Error::Custom(Box::from(Error::IdColumnNotFound(table))));
            };

            if type_ != "TEXT" {
                return Err(db::Error::Custom(Box::from(Error::IdColumnNotText(table))));
            } else if !pk {
                return Err(db::Error::Custom(Box::from(Error::IdColumnNotPk(table))));
            }

            let escaped_table = pg_escape::quote_identifier(&table);
            let mut stmt = conn.prepare(&format!("SELECT COUNT(*) FROM {escaped_table}"))?;
            if stmt.query_row([], |row| row.get::<_, u32>(0))? != 0 {
                return Err(db::Error::Custom(Box::from(Error::TableHasRows(table))));
            }
        }

        Ok(())
    }

    async fn execute_with_state(
        self,
        conn: &mut Connection,
        state: &mut DbState,
    ) -> db::Result<TransactionCommandResponse> {
        let res = self.execute_with_state_impl(conn, state);

        Ok(res.unwrap_or_else(|e| TransactionCommandResponse::Failure(e.to_string())))
    }
}
