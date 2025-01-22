# Solana Lite Account Manager

This repository provides way to manage states of Accounts with slot progression.
All processed accounts are stored in memory, processed accounts will be promoted
to confirmed and finalized as their slots are finalized.

This project also enables to create a snapshot of all the account states at the moment.

This project is used in lite-rpc and quic geyser plugin.

```sh
solana account oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7 -u http://localhost:10700
solana account oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7 -u http://139.178.82.223:10700
```

## Storage Implementations

### AccountsDB

The AccountsDB (`accounts_db/`) is an on-disk storage for Solana accounts, it uses directly the AccountsDB implementation from Solana Labs.

#### Example

There is an example that demonstrates how to use the `AccountsDB` to store accounts on disk. It connects to a Geyser GRPC source and processes account updates. It provides a minimal RPC server that implements the following endpoints:

- `get_program_accounts`
- `get_snapshot`
- `get_account_info`

You can run it via:

```sh
GRPC_ADDR=https://some-grpc-source:10000 GRPC_X_TOKEN="..." cargo run --release --package simulate_from_snapshot --bin main_accountsdb
```

### Token Account Storage

The Token Account Storage (`token_account_storage/`) is an in-memory storage for SPL/SPL2022 accounts.

It's optimized to store large quantities of SPL token accounts in-memory (only 82 bytes per token account) and efficiently retrieve token accounts by mint and owner. Currently (Jan 2025) it uses ~120GB or RAM to store the blockchain token program state.

#### Example

This example demonstrates how to use the `Token Account Storage` to store SPL/SPL2022 accounts in-memory. It is able to read an on-disk snapshot and to connect to a Geyser GRPC source/QUIC source to process account updates.

It provides a minimal RPC server that implements the following endpoints:

- `get_program_accounts` endpoint: This storage type stores only SPL token accounts, thus there are some restrictions on the filters that can be applied:
  - param 1 (`string`, required): Must be either the SPL or the SPL2022 program id.
  - param 2 (`object`, required): Configuration object containing the following fields:
    - `filters` (`array`, required): An array of filters to apply to the accounts.
      - Must contain the `dataSize` filter. Must be one of the known lengths for SPL/SPL2022 program accounts (mint=`82`, token=`165`) or a larger value when filtering for SPL2022 accounts with extensions. When filtering for multisig accounts, the size must be `355` for accounts of both programs.
      - **When filtering for token accounts**: Must contain at least one `memcmp` filter. Only the mint (`offset`=`0`, `32` bytes data length) and the owner (`offset`=`32`, `32` bytes data length) filters are supported.
- `get_snapshot`
- `get_account_info`

You can run it via:

```sh
GRPC_X_TOKEN="..." cargo run --release --package simulate_from_snapshot --bin main_tokenstorage -- --snapshot-archive-path /path/to/solana-snapshot.tar.zst --grpc-addr https://some-grpc-source:10000
```
