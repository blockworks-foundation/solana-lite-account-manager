#!/bin/bash

cd "$(dirname "${BASH_SOURCE[0]}")" || exit 1

cargo run --bin load-tester http://localhost:10700 -d 10s -c 10 get-account-info --input-file ./fixture/accounts.txt