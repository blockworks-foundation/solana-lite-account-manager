# Solana Lite Account Manager

This repository provides way to manage states of Accounts with slot progression. 
All processed accounts are stored in memory, processed accounts will be promoted 
to confirmed and finalized as their slots are finalized.

This project also enables to create a snapshot of all the account states at the moment.

This project is used in lite-rpc and quic geyser plugin.

solana account oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7 -u http://localhost:10700
solana account oQPnhXAbLbMuKHESaGrbXT17CyvWCpLyERSJA9HCYd7 -u http://139.178.82.223:10700