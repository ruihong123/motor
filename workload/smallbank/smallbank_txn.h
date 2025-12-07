// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <memory>

#include "process/txn.h"
#include "smallbank/smallbank_db.h"

/******************** The business logic (Transaction) start ********************/

bool TxAmalgamate(SmallBank* smallbank_client,
                  uint64_t* seed,
                  coro_yield_t& yield,
                  tx_id_t tx_id,
                  TXN* txn);

/* Calculate the sum of saving and checking kBalance */
bool TxBalance(SmallBank* smallbank_client,
               uint64_t* seed,
               coro_yield_t& yield,
               tx_id_t tx_id,
               TXN* txn);

/* Add $1.3 to acct_id's checking account */
bool TxDepositChecking(SmallBank* smallbank_client,
                       uint64_t* seed,
                       coro_yield_t& yield,
                       tx_id_t tx_id,
                       TXN* txn);

/* Send $5 from acct_id_0's checking account to acct_id_1's checking account */
bool TxSendPayment(SmallBank* smallbank_client,
                   uint64_t* seed,
                   coro_yield_t& yield,
                   tx_id_t tx_id,
                   TXN* txn);

/* Add $20 to acct_id's saving's account */
bool TxTransactSaving(SmallBank* smallbank_client,
                      uint64_t* seed,
                      coro_yield_t& yield,
                      tx_id_t tx_id,
                      TXN* txn);

/* Read saving and checking kBalance + update checking kBalance unconditionally */
bool TxWriteCheck(SmallBank* smallbank_client,
                  uint64_t* seed,
                  coro_yield_t& yield,
                  tx_id_t tx_id,
                  TXN* txn);

// Long-running scan transaction for hot table scanner
// Scans multiple accounts from savings and checking tables
bool TxHotTableScan(SmallBank* smallbank_client,
                    coro_yield_t& yield,
                    tx_id_t tx_id,
                    TXN* txn,
                    uint64_t& current_user_start,
                    uint64_t& current_user,
                    int& scan_table,
                    bool& in_scan);
/******************** The business logic (Transaction) end ********************/