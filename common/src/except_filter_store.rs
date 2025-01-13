// Store returns true for all the filters except programs which are there in FilterStore
use crate::{
    account_data::AccountData,
    account_filter::{AccountFilter, AccountFilters},
    account_filters_interface::AccountFiltersStoreInterface,
    simple_filter_store::SimpleFilterStore,
};

#[derive(Default)]
pub struct ExceptFilterStore {
    simple_filter_store: SimpleFilterStore,
}

impl ExceptFilterStore {
    pub fn add_account_filters(&mut self, account_filters: &AccountFilters) {
        self.simple_filter_store
            .add_account_filters(account_filters)
    }
}

impl AccountFiltersStoreInterface for ExceptFilterStore {
    fn satisfies(&self, account_data: &AccountData) -> bool {
        !self.simple_filter_store.satisfies_filter(account_data)
    }

    fn contains_filter(&self, filter: &AccountFilter) -> bool {
        !self.simple_filter_store.contains_filter_internal(filter)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use solana_sdk::pubkey::Pubkey;

    use crate::{
        account_data::{Account, AccountData},
        account_filter::{AccountFilter, AccountFilterType, MemcmpFilter},
        account_filters_interface::AccountFiltersStoreInterface,
        except_filter_store::ExceptFilterStore,
    };

    #[test]
    pub fn test_except_filter_store() {
        let mut except_filter_store = ExceptFilterStore::default();
        let program_id_1 = Pubkey::new_unique();
        let program_id_2 = Pubkey::new_unique();
        except_filter_store.add_account_filters(&vec![
            AccountFilter {
                accounts: vec![],
                program_id: Some(program_id_1),
                filters: None,
            },
            AccountFilter {
                accounts: vec![],
                program_id: Some(program_id_2),
                filters: None,
            },
        ]);

        assert!(except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(Pubkey::new_unique()),
            filters: None
        }));
        assert!(except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(Pubkey::new_unique()),
            filters: Some(vec![AccountFilterType::DataSize(100)])
        }));
        assert!(except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(Pubkey::new_unique()),
            filters: Some(vec![
                AccountFilterType::DataSize(200),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 100,
                    data: crate::account_filter::MemcmpFilterData::Bytes(vec![1, 2, 3])
                })
            ])
        }));

        assert!(!except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id_1),
            filters: None
        }));
        assert!(!except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id_1),
            filters: Some(vec![AccountFilterType::DataSize(100)])
        }));
        assert!(!except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id_1),
            filters: Some(vec![
                AccountFilterType::DataSize(200),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 100,
                    data: crate::account_filter::MemcmpFilterData::Bytes(vec![1, 2, 3])
                })
            ])
        }));

        assert!(!except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id_2),
            filters: None
        }));
        assert!(!except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id_2),
            filters: Some(vec![AccountFilterType::DataSize(100)])
        }));
        assert!(!except_filter_store.contains_filter(&AccountFilter {
            accounts: vec![],
            program_id: Some(program_id_2),
            filters: Some(vec![
                AccountFilterType::DataSize(200),
                AccountFilterType::Memcmp(MemcmpFilter {
                    offset: 100,
                    data: crate::account_filter::MemcmpFilterData::Bytes(vec![1, 2, 3])
                })
            ])
        }));

        let account_data_1 = &AccountData {
            pubkey: Pubkey::new_unique(),
            account: Arc::new(Account {
                lamports: 1234,
                data: crate::account_data::Data::Uncompressed(vec![5, 6, 7]),
                owner: Pubkey::new_unique(),
                executable: false,
                rent_epoch: 873872,
            }),
            updated_slot: 1,
            write_version: 0,
        };
        assert!(except_filter_store.satisfies(account_data_1));

        let account_data_2 = &AccountData {
            pubkey: Pubkey::new_unique(),
            account: Arc::new(Account {
                lamports: 1234,
                data: crate::account_data::Data::Uncompressed(vec![5, 6, 7]),
                owner: program_id_1,
                executable: false,
                rent_epoch: 873872,
            }),
            updated_slot: 1,
            write_version: 0,
        };
        assert!(!except_filter_store.satisfies(account_data_2));

        let account_data_3 = &AccountData {
            pubkey: Pubkey::new_unique(),
            account: Arc::new(Account {
                lamports: 1234,
                data: crate::account_data::Data::Uncompressed(vec![5, 6, 7]),
                owner: program_id_2,
                executable: false,
                rent_epoch: 873872,
            }),
            updated_slot: 1,
            write_version: 0,
        };
        assert!(!except_filter_store.satisfies(account_data_3));
    }
}
