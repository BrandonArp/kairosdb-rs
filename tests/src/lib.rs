//! End-to-End Integration Tests for KairosDB
//!
//! These tests validate the complete data flow from kairosdb-ingest to Java KairosDB.
//! They require the Tilt environment to be running with all services deployed.

pub mod common;
pub mod data_flow;
pub mod health;