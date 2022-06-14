#[allow(deprecated)]
use {
    crate::{
        accounts_index::ScanResult,
        account_rent_state::{check_rent_state, RentState},
        accounts::{
            PubkeyAccountSlot,
            TransactionLoadResult,
        },
        ancestors::Ancestors,
        bank::{
            ApplyFeatureActivationsCaller,
            inner_instructions_list_from_instruction_trace,
            BuiltinPrograms,
            CachedExecutors,
            DurableNonceFee,
            MAX_CACHED_EXECUTORS,
            TotalAccountsStats,
            TransactionExecutionDetails,
            TransactionExecutionResult,
            TransactionLogMessages,
        },
        blockhash_queue::BlockhashQueue,
        builtins::{self, BuiltinAction, Builtins, BuiltinFeatureTransition},
        cost_tracker::CostTracker,
        expected_rent_collection::{ExpectedRentCollection, SlotInfoInEpoch},
        inline_spl_associated_token_account, inline_spl_token,
        message_processor::MessageProcessor,
        rent_collector::RentCollector,
        stakes::StakesCache,
        transaction_error_metrics::TransactionErrorMetrics,
    },
    solana_measure::measure::Measure,
    solana_program_runtime::{
        accounts_data_meter::MAX_ACCOUNTS_DATA_LEN,
        invoke_context::{
            BuiltinProgram,
            Executors,
            TransactionExecutor,
            ProcessInstructionWithContext,
        },
        compute_budget::{self, ComputeBudget},
        timings::ExecuteTimings,
        sysvar_cache::SysvarCache,
        log_collector::LogCollector,
    },
    solana_sdk::{
        account::{
            Account,
            AccountSharedData,
            InheritableAccountFields,
            ReadableAccount,
            WritableAccount,
        },
        clock::{
            Epoch, Slot, INITIAL_RENT_EPOCH,
        },
        hash::Hash,
        inflation::Inflation,
        feature,
        feature_set::{
            self,
            FeatureSet,
            add_set_compute_unit_price_ix,
            default_units_per_instruction,
            requestable_heap_size,
            tx_wide_compute_cap,
        },
        fee_calculator::FeeRateGovernor,
        genesis_config::ClusterType,
        message::SanitizedMessage,
        native_loader,
        native_token::sol_to_lamports,
        precompiles::get_precompiles,
        pubkey::Pubkey,
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
        collections::HashSet,
        rc::Rc,
        sync::{
            atomic::{
                AtomicBool,
                AtomicI64,
                AtomicU64,
                Ordering::{Relaxed, Acquire},
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

    /// Total capitalization, used to calculate inflation
    capitalization: AtomicU64,

    /// Bank slot (i.e. block)
    slot: Slot,

    /// Bank epoch
    epoch: Epoch,

    /// Track cluster signature throughput and adjust fee rate
    pub(crate) fee_rate_governor: FeeRateGovernor,

    /// latest rent collector, knows the epoch
    rent_collector: RentCollector,

    /// inflation specs
    inflation: Arc<RwLock<Inflation>>,

    /// The builtin programs
    builtin_programs: BuiltinPrograms,

    pub cluster_type: Option<ClusterType>,

    pub compute_budget: Option<ComputeBudget>,

    /// Dynamic feature transitions for builtin programs
    #[allow(clippy::rc_buffer)]
    builtin_feature_transitions: Arc<Vec<BuiltinFeatureTransition>>,

    // this is temporary field only to remove rewards_pool entirely
    pub rewards_pool_pubkeys: Arc<HashSet<Pubkey>>,

    /// Cached executors
    cached_executors: RwLock<CachedExecutors>,

    pub feature_set: Arc<FeatureSet>,

    pub freeze_started: AtomicBool,

    cost_tracker: RwLock<CostTracker>,

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

    pub fn new(
        slot: u64,
        epoch: u64,
        hash: Hash,
        fee_rate_governor: FeeRateGovernor,
        rent_collector: RentCollector,
        blockhash_queue: RwLock::<BlockhashQueue>,
        cluster_type: ClusterType,
    ) -> Self {
        fn new<T: Default>() -> T {
            T::default()
        }
        let feature_set = new();
        let mut bank = Self {
            blockhash_queue: blockhash_queue,
            capitalization: AtomicU64::new(0),
            slot,
            epoch,
            fee_rate_governor,
            rent_collector,
            inflation: Arc::new(RwLock::new(Inflation::default())),
            builtin_programs: new(),
            cluster_type: Some(cluster_type),
            compute_budget: None,
            builtin_feature_transitions: new(),
            rewards_pool_pubkeys: new(),
            cached_executors: RwLock::new(CachedExecutors::new(MAX_CACHED_EXECUTORS, 0)),
            feature_set: Arc::clone(&feature_set),
            freeze_started: AtomicBool::new(hash != Hash::default()),
            cost_tracker: RwLock::new(CostTracker::default()),
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

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn cluster_type(&self) -> ClusterType {
        // unwrap is safe; self.cluster_type is ensured to be Some() always...
        // we only using Option here for ABI compatibility...
        self.cluster_type.unwrap()
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }

    pub fn freeze_started(&self) -> bool {
        self.freeze_started.load(Relaxed)
    }

    /// Return the total capitalization of the Bank
    pub fn capitalization(&self) -> u64 {
        self.capitalization.load(Relaxed)
    }

    fn inherit_specially_retained_account_fields(
        &self,
        old_account: &Option<AccountSharedData>,
    ) -> InheritableAccountFields {
        const RENT_UNADJUSTED_INITIAL_BALANCE: u64 = 1;

        (
            old_account
                .as_ref()
                .map(|a| a.lamports())
                .unwrap_or(RENT_UNADJUSTED_INITIAL_BALANCE),
            old_account
                .as_ref()
                .map(|a| a.rent_epoch())
                .unwrap_or(INITIAL_RENT_EPOCH),
        )
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

    fn burn_and_purge_account(&self, program_id: &Pubkey, mut account: AccountSharedData) {
        self.capitalization.fetch_sub(account.lamports(), Relaxed);
        // Both resetting account balance to 0 and zeroing the account data
        // is needed to really purge from AccountsDb and flush the Stakes cache
        account.set_lamports(0);
        account.data_as_mut_slice().fill(0);
        self.store_account(program_id, &account);
    }

    // NOTE: must hold idempotent for the same set of arguments
    /// Add a builtin program account
    pub fn add_builtin_account(&self, name: &str, program_id: &Pubkey, must_replace: bool) {
        let existing_genuine_program =
            self.get_account_with_fixed_root(program_id)
                .and_then(|account| {
                    // it's very unlikely to be squatted at program_id as non-system account because of burden to
                    // find victim's pubkey/hash. So, when account.owner is indeed native_loader's, it's
                    // safe to assume it's a genuine program.
                    if native_loader::check_id(account.owner()) {
                        Some(account)
                    } else {
                        // malicious account is pre-occupying at program_id
                        self.burn_and_purge_account(program_id, account);
                        None
                    }
                });

        if must_replace {
            // updating builtin program
            match &existing_genuine_program {
                None => panic!(
                    "There is no account to replace with builtin program ({}, {}).",
                    name, program_id
                ),
                Some(account) => {
                    if *name == String::from_utf8_lossy(account.data()) {
                        // The existing account is well formed
                        return;
                    }
                }
            }
        } else {
            // introducing builtin program
            if existing_genuine_program.is_some() {
                // The existing account is sufficient
                return;
            }
        }

        assert!(
            !self.freeze_started(),
            "Can't change frozen bank by adding not-existing new builtin program ({}, {}). \
            Maybe, inconsistent program activation is detected on snapshot restore?",
            name,
            program_id
        );

        // Add a bogus executable builtin account, which will be loaded and ignored.
        let account = native_loader::create_loadable_account_with_fields(
            name,
            self.inherit_specially_retained_account_fields(&existing_genuine_program),
        );
        self.store_account_and_update_capitalization(program_id, &account);
    }

    /// Add a precompiled program account
    pub fn add_precompiled_account(&self, program_id: &Pubkey) {
        self.add_precompiled_account_with_owner(program_id, native_loader::id())
    }

    // Used by tests to simulate clusters with precompiles that aren't owned by the native loader
    fn add_precompiled_account_with_owner(&self, program_id: &Pubkey, owner: Pubkey) {
        if let Some(account) = self.get_account_with_fixed_root(program_id) {
            if account.executable() {
                // The account is already executable, that's all we need
                return;
            } else {
                // malicious account is pre-occupying at program_id
                self.burn_and_purge_account(program_id, account);
            }
        };

        assert!(
            !self.freeze_started(),
            "Can't change frozen bank by adding not-existing new precompiled program ({}). \
                Maybe, inconsistent program activation is detected on snapshot restore?",
            program_id
        );

        // Add a bogus executable account, which will be loaded and ignored.
        let (lamports, rent_epoch) = self.inherit_specially_retained_account_fields(&None);
        let account = AccountSharedData::from(Account {
            lamports,
            owner,
            data: vec![],
            executable: true,
            rent_epoch,
        });
        self.store_account_and_update_capitalization(program_id, &account);
    }

    pub fn last_blockhash_and_lamports_per_signature(&self) -> (Hash, u64) {
        let blockhash_queue = self.blockhash_queue.read().unwrap();
        let last_hash = blockhash_queue.last_hash();
        let last_lamports_per_signature = blockhash_queue
            .get_lamports_per_signature(&last_hash)
            .unwrap(); // safe so long as the BlockhashQueue is consistent
        (last_hash, last_lamports_per_signature)
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

    /// Remove an executor from the bank's cache
    fn remove_executor(&self, pubkey: &Pubkey) {
        let _ = self.cached_executors.write().unwrap().remove(pubkey);
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

    pub fn load_and_execute_transaction(
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

    pub fn store_account(&self, pubkey: &Pubkey, account: &AccountSharedData) {
        self.store_accounts(&[(pubkey, account)])
    }

    pub fn store_accounts(&self, accounts: &[(&Pubkey, &AccountSharedData)]) {
        todo!()
        /*
        assert!(!self.freeze_started());
        self.rc
            .accounts
            .store_accounts_cached(self.slot(), accounts);
        let mut m = Measure::start("stakes_cache.check_and_store");
        for (pubkey, account) in accounts {
            self.stakes_cache.check_and_store(pubkey, account);
        }
        m.stop();
        self.rc
            .accounts
            .accounts_db
            .stats
            .stakes_cache_check_and_store_us
            .fetch_add(m.as_us(), Relaxed);
         */
    }

    /// Technically this issues (or even burns!) new lamports,
    /// so be extra careful for its usage
    fn store_account_and_update_capitalization(
        &self,
        pubkey: &Pubkey,
        new_account: &AccountSharedData,
    ) {
        if let Some(old_account) = self.get_account_with_fixed_root(pubkey) {
            match new_account.lamports().cmp(&old_account.lamports()) {
                std::cmp::Ordering::Greater => {
                    let increased = new_account.lamports() - old_account.lamports();
                    trace!(
                        "store_account_and_update_capitalization: increased: {} {}",
                        pubkey,
                        increased
                    );
                    self.capitalization.fetch_add(increased, Relaxed);
                }
                std::cmp::Ordering::Less => {
                    let decreased = old_account.lamports() - new_account.lamports();
                    trace!(
                        "store_account_and_update_capitalization: decreased: {} {}",
                        pubkey,
                        decreased
                    );
                    self.capitalization.fetch_sub(decreased, Relaxed);
                }
                std::cmp::Ordering::Equal => {}
            }
        } else {
            trace!(
                "store_account_and_update_capitalization: created: {} {}",
                pubkey,
                new_account.lamports()
            );
            self.capitalization
                .fetch_add(new_account.lamports(), Relaxed);
        }

        self.store_account(pubkey, new_account);
    }

    fn finish_init_builtins(
        &mut self,
        additional_builtins: Option<&Builtins>,
        debug_do_not_add_builtins: bool, // False almost every time
    ) {
        let mut builtins = builtins::get();
        if let Some(additional_builtins) = additional_builtins {
            builtins
                .genesis_builtins
                .extend_from_slice(&additional_builtins.genesis_builtins);
            builtins
                .feature_transitions
                .extend_from_slice(&additional_builtins.feature_transitions);
        }
        if !debug_do_not_add_builtins {
            for builtin in builtins.genesis_builtins {
                self.add_builtin(
                    &builtin.name,
                    &builtin.id,
                    builtin.process_instruction_with_context,
                );
            }
            for precompile in get_precompiles() {
                if precompile.feature.is_none() {
                    self.add_precompile(&precompile.program_id);
                }
            }
        }
        self.builtin_feature_transitions = Arc::new(builtins.feature_transitions);

        self.apply_feature_activations(
            ApplyFeatureActivationsCaller::FinishInit,
            debug_do_not_add_builtins,
        );

        if self
            .feature_set
            .is_active(&feature_set::cap_accounts_data_len::id())
        {
            self.cost_tracker = RwLock::new(CostTracker::new_with_account_data_size_limit(Some(
                MAX_ACCOUNTS_DATA_LEN.saturating_sub(self.accounts_data_size_initial),
            )));
        }
    }

    // Hi! leaky abstraction here....
    // use this over get_account() if it's called ONLY from on-chain runtime account
    // processing (i.e. from in-band replay/banking stage; that ensures root is *fixed* while
    // running).
    // pro: safer assertion can be enabled inside AccountsDb
    // con: panics!() if called from off-chain processing
    pub fn get_account_with_fixed_root(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        //self.load_slow_with_fixed_root(&self.ancestors, pubkey)
        //    .map(|(acc, _slot)| acc)
        todo!()
    }

    pub fn get_all_accounts_with_modified_slots(&self) -> ScanResult<Vec<PubkeyAccountSlot>> {
        todo!()
        //self.rc.accounts.load_all(&self.ancestors, self.bank_id)
    }

    pub fn rent_collector(&self) -> &RentCollector {
        &self.rent_collector
    }

    /// Add an instruction processor to intercept instructions before the dynamic loader.
    pub fn add_builtin(
        &mut self,
        name: &str,
        program_id: &Pubkey,
        process_instruction: ProcessInstructionWithContext,
    ) {
        debug!("Adding program {} under {:?}", name, program_id);
        self.add_builtin_account(name, program_id, false);
        if let Some(entry) = self
            .builtin_programs
            .vec
            .iter_mut()
            .find(|entry| entry.program_id == *program_id)
        {
            entry.process_instruction = process_instruction;
        } else {
            self.builtin_programs.vec.push(BuiltinProgram {
                program_id: *program_id,
                process_instruction,
            });
        }
        debug!("Added program {} under {:?}", name, program_id);
    }

    /// Remove a builtin instruction processor if it already exists
    pub fn remove_builtin(&mut self, program_id: &Pubkey) {
        debug!("Removing program {}", program_id);
        // Don't remove the account since the bank expects the account state to
        // be idempotent
        if let Some(position) = self
            .builtin_programs
            .vec
            .iter()
            .position(|entry| entry.program_id == *program_id)
        {
            self.builtin_programs.vec.remove(position);
        }
        debug!("Removed program {}", program_id);
    }

    pub fn add_precompile(&mut self, program_id: &Pubkey) {
        debug!("Adding precompiled program {}", program_id);
        self.add_precompiled_account(program_id);
        debug!("Added precompiled program {:?}", program_id);
    }

    // This is called from snapshot restore AND for each epoch boundary
    // The entire code path herein must be idempotent
    fn apply_feature_activations(
        &mut self,
        caller: ApplyFeatureActivationsCaller,
        debug_do_not_add_builtins: bool,
    ) {
        use ApplyFeatureActivationsCaller::*;
        let allow_new_activations = match caller {
            FinishInit => false,
            NewFromParent => true,
            WarpFromParent => false,
        };
        let new_feature_activations = self.compute_active_feature_set(allow_new_activations);

        if new_feature_activations.contains(&feature_set::pico_inflation::id()) {
            *self.inflation.write().unwrap() = Inflation::pico();
            self.fee_rate_governor.burn_percent = 50; // 50% fee burn
            self.rent_collector.rent.burn_percent = 50; // 50% rent burn
        }

        if !new_feature_activations.is_disjoint(&self.feature_set.full_inflation_features_enabled())
        {
            *self.inflation.write().unwrap() = Inflation::full();
            self.fee_rate_governor.burn_percent = 50; // 50% fee burn
            self.rent_collector.rent.burn_percent = 50; // 50% rent burn
        }

        if new_feature_activations.contains(&feature_set::spl_token_v3_4_0::id()) {
            self.replace_program_account(
                &inline_spl_token::id(),
                &inline_spl_token::program_v3_4_0::id(),
                "bank-apply_spl_token_v3_4_0",
            );
        }

        if new_feature_activations.contains(&feature_set::spl_associated_token_account_v1_1_0::id())
        {
            self.replace_program_account(
                &inline_spl_associated_token_account::id(),
                &inline_spl_associated_token_account::program_v1_1_0::id(),
                "bank-apply_spl_associated_token_account_v1_1_0",
            );
        }

        if !debug_do_not_add_builtins {
            self.apply_builtin_program_feature_transitions(
                allow_new_activations,
                &new_feature_activations,
            );
            self.reconfigure_token2_native_mint();
        }
        self.ensure_no_storage_rewards_pool();

        if new_feature_activations.contains(&feature_set::cap_accounts_data_len::id()) {
            const ACCOUNTS_DATA_LEN: u64 = 50_000_000_000;
            self.accounts_data_size_initial = ACCOUNTS_DATA_LEN;
        }
    }

    // Compute the active feature set based on the current bank state, and return the set of newly activated features
    fn compute_active_feature_set(&mut self, allow_new_activations: bool) -> HashSet<Pubkey> {
        let mut active = self.feature_set.active.clone();
        let mut inactive = HashSet::new();
        let mut newly_activated = HashSet::new();
        let slot = self.slot();

        for feature_id in &self.feature_set.inactive {
            let mut activated = None;
            if let Some(mut account) = self.get_account_with_fixed_root(feature_id) {
                if let Some(mut feature) = feature::from_account(&account) {
                    match feature.activated_at {
                        None => {
                            if allow_new_activations {
                                // Feature has been requested, activate it now
                                feature.activated_at = Some(slot);
                                if feature::to_account(&feature, &mut account).is_some() {
                                    self.store_account(feature_id, &account);
                                }
                                newly_activated.insert(*feature_id);
                                activated = Some(slot);
                                info!("Feature {} activated at slot {}", feature_id, slot);
                            }
                        }
                        Some(activation_slot) => {
                            if slot >= activation_slot {
                                // Feature is already active
                                activated = Some(activation_slot);
                            }
                        }
                    }
                }
            }
            if let Some(slot) = activated {
                active.insert(*feature_id, slot);
            } else {
                inactive.insert(*feature_id);
            }
        }

        self.feature_set = Arc::new(FeatureSet { active, inactive });
        newly_activated
    }

    fn apply_builtin_program_feature_transitions(
        &mut self,
        only_apply_transitions_for_new_features: bool,
        new_feature_activations: &HashSet<Pubkey>,
    ) {
        let feature_set = self.feature_set.clone();
        let should_apply_action_for_feature_transition = |feature_id: &Pubkey| -> bool {
            if only_apply_transitions_for_new_features {
                new_feature_activations.contains(feature_id)
            } else {
                feature_set.is_active(feature_id)
            }
        };

        let builtin_feature_transitions = self.builtin_feature_transitions.clone();
        for transition in builtin_feature_transitions.iter() {
            if let Some(builtin_action) =
                transition.to_action(&should_apply_action_for_feature_transition)
            {
                match builtin_action {
                    BuiltinAction::Add(builtin) => self.add_builtin(
                        &builtin.name,
                        &builtin.id,
                        builtin.process_instruction_with_context,
                    ),
                    BuiltinAction::Remove(program_id) => self.remove_builtin(&program_id),
                }
            }
        }

        for precompile in get_precompiles() {
            #[allow(clippy::blocks_in_if_conditions)]
            if precompile.feature.map_or(false, |ref feature_id| {
                self.feature_set.is_active(feature_id)
            }) {
                self.add_precompile(&precompile.program_id);
            }
        }
    }

    fn replace_program_account(
        &mut self,
        old_address: &Pubkey,
        new_address: &Pubkey,
        datapoint_name: &'static str,
    ) {
        if let Some(old_account) = self.get_account_with_fixed_root(old_address) {
            if let Some(new_account) = self.get_account_with_fixed_root(new_address) {
                datapoint_info!(datapoint_name, ("slot", self.slot, i64));

                // Burn lamports in the old account
                self.capitalization
                    .fetch_sub(old_account.lamports(), Relaxed);

                // Transfer new account to old account
                self.store_account(old_address, &new_account);

                // Clear new account
                self.store_account(new_address, &AccountSharedData::default());

                self.remove_executor(old_address);
            }
        }
    }

    fn reconfigure_token2_native_mint(&mut self) {
        let reconfigure_token2_native_mint = match self.cluster_type() {
            ClusterType::Development => true,
            ClusterType::Devnet => true,
            ClusterType::Testnet => self.epoch() == 93,
            ClusterType::MainnetBeta => self.epoch() == 75,
        };

        if reconfigure_token2_native_mint {
            let mut native_mint_account = solana_sdk::account::AccountSharedData::from(Account {
                owner: inline_spl_token::id(),
                data: inline_spl_token::native_mint::ACCOUNT_DATA.to_vec(),
                lamports: sol_to_lamports(1.),
                executable: false,
                rent_epoch: self.epoch() + 1,
            });

            // As a workaround for
            // https://github.com/solana-labs/solana-program-library/issues/374, ensure that the
            // spl-token 2 native mint account is owned by the spl-token 2 program.
            let store = if let Some(existing_native_mint_account) =
                self.get_account_with_fixed_root(&inline_spl_token::native_mint::id())
            {
                if existing_native_mint_account.owner() == &solana_sdk::system_program::id() {
                    native_mint_account.set_lamports(existing_native_mint_account.lamports());
                    true
                } else {
                    false
                }
            } else {
                self.capitalization
                    .fetch_add(native_mint_account.lamports(), Relaxed);
                true
            };

            if store {
                self.store_account(&inline_spl_token::native_mint::id(), &native_mint_account);
            }
        }
    }

    fn ensure_no_storage_rewards_pool(&mut self) {
        let purge_window_epoch = match self.cluster_type() {
            ClusterType::Development => false,
            // never do this for devnet; we're pristine here. :)
            ClusterType::Devnet => false,
            // schedule to remove at testnet/tds
            ClusterType::Testnet => self.epoch() == 93,
            // never do this for stable; we're pristine here. :)
            ClusterType::MainnetBeta => false,
        };

        if purge_window_epoch {
            for reward_pubkey in self.rewards_pool_pubkeys.iter() {
                if let Some(mut reward_account) = self.get_account_with_fixed_root(reward_pubkey) {
                    if reward_account.lamports() == u64::MAX {
                        reward_account.set_lamports(0);
                        self.store_account(reward_pubkey, &reward_account);
                        // Adjust capitalization.... it has been wrapping, reducing the real capitalization by 1-lamport
                        self.capitalization.fetch_add(1, Relaxed);
                        info!(
                            "purged rewards pool account: {}, new capitalization: {}",
                            reward_pubkey,
                            self.capitalization()
                        );
                    }
                };
            }
        }
    }

    /// Get all the accounts for this bank and calculate stats
    pub fn get_total_accounts_stats(&self) -> ScanResult<TotalAccountsStats> {
        let accounts = self.get_all_accounts_with_modified_slots()?;
        Ok(self.calculate_total_accounts_stats(
            accounts
                .iter()
                .map(|(pubkey, account, _slot)| (pubkey, account)),
        ))
    }

    /// Given all the accounts for a bank, calculate stats
    pub fn calculate_total_accounts_stats<'a>(
        &self,
        accounts: impl Iterator<Item = (&'a Pubkey, &'a AccountSharedData)>,
    ) -> TotalAccountsStats {
        let rent_collector = self.rent_collector();
        let mut total_accounts_stats = TotalAccountsStats::default();
        accounts.for_each(|(pubkey, account)| {
            let data_len = account.data().len();
            total_accounts_stats.num_accounts += 1;
            total_accounts_stats.data_len += data_len;

            if account.executable() {
                total_accounts_stats.num_executable_accounts += 1;
                total_accounts_stats.executable_data_len += data_len;
            }

            if !rent_collector.should_collect_rent(pubkey, account)
                || rent_collector.get_rent_due(account).is_exempt()
            {
                total_accounts_stats.num_rent_exempt_accounts += 1;
            } else {
                total_accounts_stats.num_rent_paying_accounts += 1;
                total_accounts_stats.lamports_in_rent_paying_accounts += account.lamports();
                if data_len == 0 {
                    total_accounts_stats.num_rent_paying_accounts_without_data += 1;
                }
            }
        });

        total_accounts_stats
    }
}
