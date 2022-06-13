#[allow(deprecated)]
use {
    crate::{
        account_rent_state::{check_rent_state, RentState},
        accounts::TransactionLoadResult,
        bank::{
            inner_instructions_list_from_instruction_trace,
            BuiltinPrograms,
            CachedExecutors,
            DurableNonceFee,
            TransactionExecutionDetails,
            TransactionExecutionResult,
            TransactionLogMessages,
        },
        blockhash_queue::BlockhashQueue,
        message_processor::MessageProcessor,
        transaction_error_metrics::TransactionErrorMetrics,
        rent_collector::RentCollector,
    },
    solana_measure::measure::Measure,
    solana_program_runtime::{
        invoke_context::{
            Executors,
            TransactionExecutor,
        },
        compute_budget::{self, ComputeBudget},
        timings::ExecuteTimings,
        sysvar_cache::SysvarCache,
        log_collector::LogCollector,
    },
    solana_sdk::{
        account::ReadableAccount,
        hash::Hash,
        feature_set::{
            self,
            FeatureSet,
            add_set_compute_unit_price_ix,
            default_units_per_instruction,
            requestable_heap_size,
            tx_wide_compute_cap,
        },
        message::SanitizedMessage,
        native_loader,
        saturating_add_assign,
        transaction::{
            Result,
            SanitizedTransaction,
            TransactionError,
        },
        transaction_context::{
            ExecutionRecord,
            TransactionAccount,
            TransactionContext,
        },
    },
    log::*,
    std::{
        cell::RefCell,
        rc::Rc,
        sync::{
            atomic::{
                AtomicI64,
                Ordering::{Acquire},
            },
            Arc,
            RwLock,
        },
    },
};
use crate::accounts::LoadedTransaction;
use crate::bank::RentDebits;




/*
let compute_budget = ComputeBudget {
max_units: 500_000_000_000,
heap_size: Some(256_usize.saturating_mul(1024)),
syscall_base_cost: 0,
log_64_units: 0,
invoke_units :0,
heap_cost: 0,
sysvar_base_cost: 0,
mem_op_base_cost:0,
secp256k1_recover_cost :0,
create_program_address_units :0,
sha256_base_cost:0,
sha256_byte_cost:0,
// cpi_bytes_per_unit:0,
..ComputeBudget::default()
};

pub fn feature_set() -> Arc<FeatureSet> {
    let mut features = FeatureSet::all_enabled();
    features.deactivate(&tx_wide_compute_cap::id());
    features.deactivate(&requestable_heap_size ::id());
    // features.deactivate(&prevent_calling_precompiles_as_programs ::id());
    Arc::new(features)
}

fn fill_sysvar_cache() -> SysvarCache {
    let mut sysvar_cache =  SysvarCache::default();

    if sysvar_cache.get_clock().is_err() {
        sysvar_cache.set_clock(Clock::default());
    }

    if sysvar_cache.get_epoch_schedule().is_err() {
        sysvar_cache.set_epoch_schedule(EpochSchedule::default());
    }

    #[allow(deprecated)]
    if sysvar_cache.get_fees().is_err() {
        sysvar_cache.set_fees(Fees::default());
    }

    if sysvar_cache.get_rent().is_err() {
        sysvar_cache.set_rent(Rent::default());
    }

    if sysvar_cache.get_slot_hashes().is_err() {
        sysvar_cache.set_slot_hashes(SlotHashes::default());
    }
    sysvar_cache
}



let mut builtin_programs: BuiltinPrograms = BuiltinPrograms::default();
    let mut builtins = builtins::get();
    for builtin in builtins.genesis_builtins {
        // println!("Adding program {} under {:?}", &builtin.name, &builtin.id);
        builtin_programs.vec.push(BuiltinProgram {
            program_id: builtin.id,
            process_instruction: builtin.process_instruction_with_context,
        });
    };
    let bpf_loader = solana_bpf_loader_program::solana_bpf_loader_program!();
    let upgradable_loader = solana_bpf_loader_program::solana_bpf_loader_upgradeable_program!();

    builtin_programs.vec.push(BuiltinProgram {
        program_id: solana_sdk::bpf_loader::id(),
        process_instruction: bpf_loader.2,
    });

    builtin_programs.vec.push(BuiltinProgram {
        program_id: solana_sdk::bpf_loader_upgradeable::id(),
        process_instruction: upgradable_loader.2,
    });


*/


struct EmulatorBank {
    /// FIFO queue of `recent_blockhash` items
    blockhash_queue: RwLock<BlockhashQueue>,

    /// latest rent collector, knows the epoch
    rent_collector: RentCollector,

    /// The builtin programs
    builtin_programs: BuiltinPrograms,

    pub compute_budget: Option<ComputeBudget>,

    /// Cached executors
    cached_executors: RwLock<CachedExecutors>,

    pub feature_set: Arc<FeatureSet>,

    sysvar_cache: RwLock<SysvarCache>,

    /// The initial accounts data size at the start of this Bank, before processing any transactions/etc
    accounts_data_size_initial: u64,
    /// The change to accounts data size in this Bank, due on-chain events (i.e. transactions)
    accounts_data_size_delta_on_chain: AtomicI64,
    /// The change to accounts data size in this Bank, due to off-chain events (i.e. rent collection)
    accounts_data_size_delta_off_chain: AtomicI64,
}

struct TransactionAccountStateInfo {
    rent_state: Option<RentState>, // None: readonly account
}

impl EmulatorBank {
    pub fn new(
        slot: u64,
        feature_set: Arc<FeatureSet>,
        rent_collector: RentCollector,
        blockhash_queue: RwLock::<BlockhashQueue>,
    ) -> Self {
        let mut bank = Self {
            blockhash_queue: blockhash_queue,
            rent_collector: rent_collector,
            builtin_programs: BuiltinPrograms::default(),
            compute_budget: None,
            cached_executors: RwLock::new(CachedExecutors::new(MAX_CACHED_EXECUTORS, 0)),
            feature_set: feature_set,
            sysvar_cache: RwLock::new(SysvarCache::default()),
            accounts_data_size_initial: 0,
            accounts_data_size_delta_on_chain: AtomicI64::new(0),
            accounts_data_size_delta_off_chain: AtomicI64::new(0),
        };
        bank.finish_init_builtins(None, false);

        let accounts_data_size_initial = bank.get_total_accounts_stats().unwrap().data_len as u64;
        bank.accounts_data_size_initial = accounts_data_size_initial;

        bank
    }

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

    pub fn execute_transaction(
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

    /// Execute a transaction using the provided loaded accounts and update
    /// the executors cache if the transaction was successful.
    #[allow(clippy::too_many_arguments)]
    pub fn execute_loaded_transaction(
        &self,
        tx: &SanitizedTransaction,
        loaded_transaction: &mut LoadedTransaction,
        compute_budget: ComputeBudget,
        durable_nonce_fee: Option<DurableNonceFee>,
        enable_cpi_recording: bool,
        enable_log_recording: bool,
        enable_return_data_recording: bool,
        timings: &mut ExecuteTimings,
        error_counters: &mut TransactionErrorMetrics,
    ) -> TransactionExecutionResult {
        let mut get_executors_time = Measure::start("get_executors_time");
        let executors = self.get_executors(&loaded_transaction.accounts);
        get_executors_time.stop();
        saturating_add_assign!(
            timings.execute_accessories.get_executors_us,
            get_executors_time.as_us()
        );

        let mut transaction_accounts = Vec::new();
        std::mem::swap(&mut loaded_transaction.accounts, &mut transaction_accounts);
        let mut transaction_context = TransactionContext::new(
            transaction_accounts,
            compute_budget.max_invoke_depth.saturating_add(1),
            tx.message().instructions().len(),
        );

        let pre_account_state_info =
            self.get_transaction_account_state_info(&transaction_context, tx.message());

        let log_collector = if enable_log_recording {
            Some(LogCollector::new_ref())
        } else {
            None
        };

        let (blockhash, lamports_per_signature) = self.last_blockhash_and_lamports_per_signature();

        let mut executed_units = 0u64;

        let mut process_message_time = Measure::start("process_message_time");
        let process_result = MessageProcessor::process_message(
            &self.builtin_programs.vec,
            tx.message(),
            &loaded_transaction.program_indices,
            &mut transaction_context,
            self.rent_collector.rent,
            log_collector.clone(),
            executors.clone(),
            self.feature_set.clone(),
            compute_budget,
            timings,
            &*self.sysvar_cache.read().unwrap(),
            blockhash,
            lamports_per_signature,
            self.load_accounts_data_size(),
            &mut executed_units,
        );
        process_message_time.stop();

        saturating_add_assign!(
            timings.execute_accessories.process_message_us,
            process_message_time.as_us()
        );

        let mut store_missing_executors_time = Measure::start("store_missing_executors_time");
        self.store_missing_executors(&executors);
        store_missing_executors_time.stop();
        saturating_add_assign!(
            timings.execute_accessories.update_executors_us,
            store_missing_executors_time.as_us()
        );

        let status = process_result
            .and_then(|info| {
                let post_account_state_info =
                    self.get_transaction_account_state_info(&transaction_context, tx.message());
                self.verify_transaction_account_state_changes(
                    &pre_account_state_info,
                    &post_account_state_info,
                    &transaction_context,
                )
                    .map(|_| info)
            })
            .map_err(|err| {
                match err {
                    TransactionError::InvalidRentPayingAccount
                    | TransactionError::InsufficientFundsForRent { .. } => {
                        error_counters.invalid_rent_paying_account += 1;
                    }
                    _ => {
                        error_counters.instruction_error += 1;
                    }
                }
                err
            });
        let accounts_data_len_delta = status
            .as_ref()
            .map_or(0, |info| info.accounts_data_len_delta);
        let status = status.map(|_| ());

        let log_messages: Option<TransactionLogMessages> =
            log_collector.and_then(|log_collector| {
                Rc::try_unwrap(log_collector)
                    .map(|log_collector| log_collector.into_inner().into())
                    .ok()
            });

        let ExecutionRecord {
            accounts,
            instruction_trace,
            mut return_data,
        } = transaction_context.into();
        loaded_transaction.accounts = accounts;

        let inner_instructions = if enable_cpi_recording {
            Some(inner_instructions_list_from_instruction_trace(
                &instruction_trace,
            ))
        } else {
            None
        };

        let return_data = if enable_return_data_recording {
            if let Some(end_index) = return_data.data.iter().rposition(|&x| x != 0) {
                let end_index = end_index.saturating_add(1);
                error!("end index {}", end_index);
                return_data.data.truncate(end_index);
                Some(return_data)
            } else {
                None
            }
        } else {
            None
        };

        TransactionExecutionResult::Executed {
            details: TransactionExecutionDetails {
                status,
                log_messages,
                inner_instructions,
                durable_nonce_fee,
                return_data,
                executed_units,
                accounts_data_len_delta,
            },
            executors,
        }
    }

    /// Get any cached executors needed by the transaction
    fn get_executors(&self, accounts: &[TransactionAccount]) -> Rc<RefCell<Executors>> {
        let executable_keys: Vec<_> = accounts
            .iter()
            .filter_map(|(key, account)| {
                if account.executable() && !native_loader::check_id(account.owner()) {
                    Some(key)
                } else {
                    None
                }
            })
            .collect();

        if executable_keys.is_empty() {
            return Rc::new(RefCell::new(Executors::default()));
        }

        let executors = {
            let cache = self.cached_executors.read().unwrap();
            executable_keys
                .into_iter()
                .filter_map(|key| {
                    cache
                        .get(key)
                        .map(|executor| (*key, TransactionExecutor::new_cached(executor)))
                })
                .collect()
        };

        Rc::new(RefCell::new(executors))
    }

    /// Add executors back to the bank's cache if they were missing and not updated
    fn store_missing_executors(&self, executors: &RefCell<Executors>) {
        self.store_executors_internal(executors, |e| e.is_missing())
    }

    /// Add updated executors back to the bank's cache
    fn store_updated_executors(&self, executors: &RefCell<Executors>) {
        self.store_executors_internal(executors, |e| e.is_updated())
    }

    /// Helper to write a selection of executors to the bank's cache
    fn store_executors_internal(
        &self,
        executors: &RefCell<Executors>,
        selector: impl Fn(&TransactionExecutor) -> bool,
    ) {
        let executors = executors.borrow();
        let dirty_executors: Vec<_> = executors
            .iter()
            .filter_map(|(key, executor)| selector(executor).then(|| (key, executor.get())))
            .collect();

        if !dirty_executors.is_empty() {
            self.cached_executors.write().unwrap().put(&dirty_executors);
        }
    }

    pub fn get_transaction_account_state_info(
        &self,
        transaction_context: &TransactionContext,
        message: &SanitizedMessage,
    ) -> Vec<TransactionAccountStateInfo> {
        (0..message.account_keys().len())
            .map(|i| {
                let rent_state = if message.is_writable(i) {
                    let state = if let Ok(account) = transaction_context.get_account_at_index(i) {
                        let account = account.borrow();

                        // Native programs appear to be RentPaying because they carry low lamport
                        // balances; however they will never be loaded as writable
                        debug_assert!(!native_loader::check_id(account.owner()));

                        Some(RentState::from_account(
                            &account,
                            &self.rent_collector().rent,
                        ))
                    } else {
                        None
                    };
                    debug_assert!(
                        state.is_some(),
                        "message and transaction context out of sync, fatal"
                    );
                    state
                } else {
                    None
                };
                TransactionAccountStateInfo { rent_state }
            })
            .collect()
    }

    pub fn last_blockhash_and_lamports_per_signature(&self) -> (Hash, u64) {
        let blockhash_queue = self.blockhash_queue.read().unwrap();
        let last_hash = blockhash_queue.last_hash();
        let last_lamports_per_signature = blockhash_queue
            .get_lamports_per_signature(&last_hash)
            .unwrap(); // safe so long as the BlockhashQueue is consistent
        (last_hash, last_lamports_per_signature)
    }

    /// Load the accounts data size, in bytes
    pub fn load_accounts_data_size(&self) -> u64 {
        // Mixed integer ops currently not stable, so copying the impl.
        // Copied from: https://github.com/a1phyr/rust/blob/47edde1086412b36e9efd6098b191ec15a2a760a/library/core/src/num/uint_macros.rs#L1039-L1048
        fn saturating_add_signed(lhs: u64, rhs: i64) -> u64 {
            let (res, overflow) = lhs.overflowing_add(rhs as u64);
            if overflow == (rhs < 0) {
                res
            } else if overflow {
                u64::MAX
            } else {
                u64::MIN
            }
        }
        saturating_add_signed(
            self.accounts_data_size_initial,
            self.load_accounts_data_size_delta(),
        )
    }

    pub(crate) fn verify_transaction_account_state_changes(
        &self,
        pre_state_infos: &[TransactionAccountStateInfo],
        post_state_infos: &[TransactionAccountStateInfo],
        transaction_context: &TransactionContext,
    ) -> Result<()> {
        let require_rent_exempt_accounts = self
            .feature_set
            .is_active(&feature_set::require_rent_exempt_accounts::id());
        let do_support_realloc = self
            .feature_set
            .is_active(&feature_set::do_support_realloc::id());
        let include_account_index_in_err = self
            .feature_set
            .is_active(&feature_set::include_account_index_in_rent_error::id());
        for (i, (pre_state_info, post_state_info)) in
        pre_state_infos.iter().zip(post_state_infos).enumerate()
        {
            if let Err(err) = check_rent_state(
                pre_state_info.rent_state.as_ref(),
                post_state_info.rent_state.as_ref(),
                transaction_context,
                i,
                do_support_realloc,
                include_account_index_in_err,
            ) {
                // Feature gate only wraps the actual error return so that the metrics and debug
                // logging generated by `check_rent_state()` can be examined before feature
                // activation
                if require_rent_exempt_accounts {
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    /// Load the change in accounts data size in this Bank, in bytes
    pub fn load_accounts_data_size_delta(&self) -> i64 {
        let delta_on_chain = self.load_accounts_data_size_delta_on_chain();
        let delta_off_chain = self.load_accounts_data_size_delta_off_chain();
        delta_on_chain.saturating_add(delta_off_chain)
    }

    /// Load the change in accounts data size in this Bank, in bytes, from on-chain events
    /// i.e. transactions
    pub fn load_accounts_data_size_delta_on_chain(&self) -> i64 {
        self.accounts_data_size_delta_on_chain.load(Acquire)
    }

    /// Load the change in accounts data size in this Bank, in bytes, from off-chain events
    /// i.e. rent collection
    pub fn load_accounts_data_size_delta_off_chain(&self) -> i64 {
        self.accounts_data_size_delta_off_chain.load(Acquire)
    }

    pub fn rent_collector(&self) -> &RentCollector {
        &self.rent_collector
    }
}