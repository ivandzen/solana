#[allow(deprecated)]
use {
    crate::{
        accounts::TransactionLoadResult,
        bank::{
            Bank,
            DurableNonceFee,
            TransactionExecutionResult,
        },
        transaction_error_metrics::TransactionErrorMetrics,
    },
    solana_measure::measure::Measure,
    solana_program_runtime::{
        compute_budget::{self, ComputeBudget},
        timings::ExecuteTimings,
    },
    solana_sdk::{
        feature_set::{
            add_set_compute_unit_price_ix,
            default_units_per_instruction,
            requestable_heap_size,
            tx_wide_compute_cap,
        },
        saturating_add_assign,
        transaction::SanitizedTransaction,
    },
    log::*,
};
use crate::accounts::LoadedTransaction;
use crate::bank::RentDebits;

impl Bank {
    fn load_transaction(
        &self,
        _tx: &SanitizedTransaction,
    ) -> TransactionLoadResult {
        let loaded_tx = LoadedTransaction {
            accounts: vec![],
            program_indices: vec![],
            rent: 0,
            rent_debits: RentDebits::default(),
        };

        (Ok(loaded_tx), None)
    }

    pub fn emulate_transaction(
        &self,
        tx: &SanitizedTransaction,
        enable_cpi_recording: bool,
        enable_log_recording: bool,
        enable_return_data_recording: bool,
        timings: &mut ExecuteTimings,
    ) -> TransactionExecutionResult {
        let mut error_counters = TransactionErrorMetrics::default();

        let mut load_time = Measure::start("accounts_load");
        let accs = self.load_transaction(tx);
        load_time.stop();

        let mut execution_time = Measure::start("execution_time");

        let execution_result = match accs {
            (Err(e), _nonce) => TransactionExecutionResult::NotExecuted(e.clone()),
            (Ok(mut loaded_transaction), nonce) => {
                let mut feature_set_clone_time = Measure::start("feature_set_clone");
                let feature_set = self.feature_set.clone();
                feature_set_clone_time.stop();
                saturating_add_assign!(
                    timings.execute_accessories.feature_set_clone_us,
                    feature_set_clone_time.as_us()
                );

                let compute_budget = if let Some(compute_budget) = self.compute_budget {
                    compute_budget
                } else {
                    let tx_wide_compute_cap = feature_set.is_active(&tx_wide_compute_cap::id());
                    let compute_unit_limit = if tx_wide_compute_cap {
                        compute_budget::MAX_COMPUTE_UNIT_LIMIT
                    } else {
                        compute_budget::DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT
                    };
                    let mut compute_budget = ComputeBudget::new(compute_unit_limit as u64);
                    if tx_wide_compute_cap {
                        let mut compute_budget_process_transaction_time =
                            Measure::start("compute_budget_process_transaction_time");
                        let process_transaction_result = compute_budget.process_instructions(
                            tx.message().program_instructions_iter(),
                            feature_set.is_active(&requestable_heap_size::id()),
                            feature_set.is_active(&default_units_per_instruction::id()),
                            feature_set.is_active(&add_set_compute_unit_price_ix::id()),
                        );
                        compute_budget_process_transaction_time.stop();
                        saturating_add_assign!(
                            timings
                            .execute_accessories
                            .compute_budget_process_transaction_us,
                            compute_budget_process_transaction_time.as_us()
                        );
                        if let Err(err) = process_transaction_result {
                            return TransactionExecutionResult::NotExecuted(err);
                        }
                    }
                    compute_budget
                };

                self.execute_loaded_transaction(
                    tx,
                    &mut loaded_transaction,
                    compute_budget,
                    nonce.as_ref().map(DurableNonceFee::from),
                    enable_cpi_recording,
                    enable_log_recording,
                    enable_return_data_recording,
                    timings,
                    &mut error_counters,
                )
            }
        };

        execution_time.stop();

        debug!(
            "load: {}us execute: {}us",
            load_time.as_us(),
            execution_time.as_us(),
        );

        execution_result
    }
}