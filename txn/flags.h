// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

/*********************** Configure workload **********************/
#define WORKLOAD_TPCC 1
#define WORKLOAD_TATP 0
#define WORKLOAD_SmallBank 0
#define WORKLOAD_MICRO 0

#if WORKLOAD_TPCC
#define MAX_VALUE_SIZE 664
#define MAX_VCELL_NUM 4

#elif WORKLOAD_TATP
#define MAX_VALUE_SIZE 40
#define MAX_VCELL_NUM 2

#elif WORKLOAD_SmallBank
#define MAX_VALUE_SIZE 8
#define MAX_VCELL_NUM 3

#elif WORKLOAD_MICRO
#define MAX_VALUE_SIZE 40
#define MAX_VCELL_NUM 4
#endif

#define bitmap_t uint8_t

#define LargeAttrBar 0

/*********************** Some limits, change them as needed **********************/
#define MAX_REMOTE_NODE_NUM 100  
#define MAX_TNUM_PER_CN 100
#define MAX_CLIENT_NUM_PER_MN 64  // Must be ≥ max_client_num_per_mn in mn_config.json
#define BACKUP_NUM 2  // Backup memory node number. **NOT** 0

#define MAX_DB_TABLE_NUM 15 
#define MAX_ATTRIBUTE_NUM_PER_TABLE 20

/*********************** Options **********************/
#define EARLY_ABORT 1
#define PRINT_HASH_META 0
// #define PRINT_RDMA_OPS 1  // Print detailed RDMA operation info for debugging
#define OUTPUT_EVENT_STAT 0
#define OUTPUT_KEY_STAT 0

/*********************** Memory optimization **********************/
// Set to 1 to save memory by not allocating unnecessary tables on each MN
// WARNING: This causes offset mismatches between primary and backup nodes!
// Replica writes use primary offsets, which corrupt backup data with different layouts.
// MUST keep this at 0 to ensure uniform memory layout across all nodes.
#define OPTIMIZE_TABLE_ALLOCATION 0

/*********************** Crash test only **********************/
#define PROBE_TP 0  // Probing throughput during execution
#define HAVE_COORD_CRASH 0
#define HAVE_PRIMARY_CRASH 0
#define HAVE_BACKUP_CRASH 0
#define CRASH_TABLE_ID 2
#define PRIMARY_CRASH -33
#define BACKUP_CRASH -13

// Safety check: Prevent enabling both optimization and crash recovery
#if OPTIMIZE_TABLE_ALLOCATION && (HAVE_PRIMARY_CRASH || HAVE_BACKUP_CRASH)
#error "Cannot enable OPTIMIZE_TABLE_ALLOCATION with crash recovery! Replica addresses will mismatch."
#endif

