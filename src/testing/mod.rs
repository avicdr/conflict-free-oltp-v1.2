//! Testing module.

pub mod property_tests;
pub mod randomized_chaos;
pub mod scenario;
pub mod adversarial;

pub use scenario::{run_reference_scenario, assert_convergence, sync_until_quiescent};
