//! End-to-End Integration Tests for KairosDB
//!
//! These tests validate the complete data flow between services:
//! - kairosdb-ingest → Cassandra → Java KairosDB (data_flow module)
//! - kairosdb-ingest → Cassandra → Rust Query Service (rust_query module)
//!
//! They require the Tilt environment to be running with all services deployed.

pub mod common;
pub mod data_flow;
pub mod health;
pub mod rust_query;
pub mod performance;
