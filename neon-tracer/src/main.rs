use {
    solana_runtime::{
        bank::{ Bank, BankRc },
        accounts::Accounts,
        accounts_db::AccountsDb,
    },
    solana_sdk::genesis_config::GenesisConfig,
};

fn main() {
    let slot = 345;
    let mut accounts_db = AccountsDb::new_single_for_tests();

    let bank_rc = BankRc::new(Accounts::new_empty(accounts_db), slot);
    let genesis_config = GenesisConfig::default();
    let bank = Bank::tracer_new(
        bank_rc,
        &genesis_config,
        fields,
        None,
        None,
        true,
        0
    );
}