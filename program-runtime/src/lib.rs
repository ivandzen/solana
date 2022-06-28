#![cfg_attr(RUSTC_WITH_SPECIALIZATION, feature(min_specialization))]
#![deny(clippy::integer_arithmetic)]
#![deny(clippy::indexing_slicing)]

pub mod accounts_data_meter;
pub mod compute_budget;
pub mod invoke_context;
pub mod log_collector;
pub mod pre_account;
pub mod prioritization_fee;
pub mod stable_log;
pub mod sysvar_cache;
pub mod timings;
