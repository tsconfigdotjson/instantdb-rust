//! The Postgres-only part of sqlx's facade.
//!
//! Cargo.lock records optional dependencies even when their features are off.
//! Depending on the `sqlx` facade therefore kept `sqlx-sqlite` (and its
//! `libsqlite3-sys` 0.30) in the lockfile, although this workspace only uses
//! Postgres and never compiled SQLite. Re-exporting the same core + Postgres
//! pieces directly keeps `sqlx::...` imports unchanged without the unused
//! drivers in the dependency graph.

pub use sqlx_core::acquire::Acquire;
pub use sqlx_core::arguments::{Arguments, IntoArguments};
pub use sqlx_core::column::{Column, ColumnIndex};
pub use sqlx_core::connection::{ConnectOptions, Connection};
pub use sqlx_core::database::{self, Database};
pub use sqlx_core::describe::Describe;
pub use sqlx_core::error::{self, Error, Result};
pub use sqlx_core::executor::{Execute, Executor};
pub use sqlx_core::from_row::FromRow;
pub use sqlx_core::migrate;
pub use sqlx_core::pool::{self, Pool};
pub use sqlx_core::query::{query, query_with};
pub use sqlx_core::query_as::{query_as, query_as_with};
pub use sqlx_core::query_builder::{self, QueryBuilder};
pub use sqlx_core::query_scalar::{query_scalar, query_scalar_with};
pub use sqlx_core::raw_sql::{raw_sql, RawSql};
pub use sqlx_core::row::Row;
pub use sqlx_core::statement::Statement;
pub use sqlx_core::transaction::{Transaction, TransactionManager};
pub use sqlx_core::type_info::TypeInfo;
pub use sqlx_core::types::Type;
pub use sqlx_core::value::{Value, ValueRef};
pub use sqlx_core::Either;
pub use sqlx_postgres::{
    self as postgres, PgConnection, PgExecutor, PgPool, PgTransaction, Postgres,
};

/// `sqlx::query::Query` and friends (the facade exposes these as a module,
/// while `query` above is the builder function).
pub mod query {
    pub use sqlx_core::query::{Map, Query};
    pub use sqlx_core::query_as::QueryAs;
    pub use sqlx_core::query_scalar::QueryScalar;
}

pub mod types {
    pub use sqlx_core::types::*;
}
