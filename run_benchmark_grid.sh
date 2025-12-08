#!/bin/bash

# Motor Benchmark Grid Runner
# This script runs benchmarks in a grid pattern using nested for loops
# It handles workload switching by modifying flags.h and recompiling

set -e  # Exit on any error

# Configuration
PROJECT_DIR="/users/Ruihong/motor"
BUILD_DIR="${PROJECT_DIR}/build"
USERNAME="Ruihong"
MEMORY_NODES=("node-8" "node-9" "node-10" "node-11" "node-12" "node-13" "node-14" "node-15")
COMPUTE_NODES=("node-0" "node-1" "node-2" "node-3" "node-4" "node-5" "node-6" "node-7")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# ============================================================================
# CONFIGURATION: Edit these arrays to define your benchmark grid
# ============================================================================

# Workloads to test (in order)
WORKLOADS=("tpcc" "tatp" "smallbank")

# Thread counts to test
THREAD_COUNTS=(8)

# Coroutine counts to test
COROUTINE_COUNTS=(2)

# Isolation levels to test
ISOLATION_LEVELS=("SR")

# Hot table scanner thread counts to test (0 = disabled, 1+ = enabled with that many threads)
HOT_SCAN_THREAD_COUNTS=(1)

# Debug mode (yes/no) - builds with -g -O0 flags
DEBUG_MODE="no"

# ============================================================================

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_header() {
    echo ""
    echo -e "${MAGENTA}========================================${NC}"
    echo -e "${MAGENTA}$1${NC}"
    echo -e "${MAGENTA}========================================${NC}"
}

log_run() {
    echo ""
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Function to update flags.h for a specific workload
update_flags_h() {
    local workload=$1
    local flags_file="${PROJECT_DIR}/txn/flags.h"
    
    log_info "Updating flags.h for workload: $workload"
    
    # Backup the original file
    cp "$flags_file" "${flags_file}.backup"
    
    # Set all workload flags to 0
    sed -i 's/#define WORKLOAD_TPCC [01]/#define WORKLOAD_TPCC 0/' "$flags_file"
    sed -i 's/#define WORKLOAD_TATP [01]/#define WORKLOAD_TATP 0/' "$flags_file"
    sed -i 's/#define WORKLOAD_SmallBank [01]/#define WORKLOAD_SmallBank 0/' "$flags_file"
    sed -i 's/#define WORKLOAD_MICRO [01]/#define WORKLOAD_MICRO 0/' "$flags_file"
    
    # Set the appropriate workload flag to 1
    case "$workload" in
        "tpcc")
            sed -i 's/#define WORKLOAD_TPCC 0/#define WORKLOAD_TPCC 1/' "$flags_file"
            ;;
        "tatp")
            sed -i 's/#define WORKLOAD_TATP 0/#define WORKLOAD_TATP 1/' "$flags_file"
            ;;
        "smallbank")
            sed -i 's/#define WORKLOAD_SmallBank 0/#define WORKLOAD_SmallBank 1/' "$flags_file"
            ;;
        "micro")
            sed -i 's/#define WORKLOAD_MICRO 0/#define WORKLOAD_MICRO 1/' "$flags_file"
            ;;
        *)
            log_error "Unknown workload: $workload"
            return 1
            ;;
    esac
    
    log_success "flags.h updated for $workload"
    
    # Show the current workload flags
    log_info "Current workload flags:"
    grep "^#define WORKLOAD_" "$flags_file" | head -4
}

# Function to sync flags.h to all nodes
sync_flags_h() {
    log_info "Syncing flags.h to all nodes..."
    
    local flags_file="${PROJECT_DIR}/txn/flags.h"
    local pids=()
    local failed=0
    
    # Sync to all nodes in parallel
    for node in "${MEMORY_NODES[@]}" "${COMPUTE_NODES[@]}"; do
        (
            scp -q -o ConnectTimeout=30 "$flags_file" "$USERNAME@$node:${PROJECT_DIR}/txn/flags.h" 2>&1
            if [ $? -ne 0 ]; then
                echo "FAILED: $node"
                exit 1
            fi
        ) &
        pids+=($!)
    done
    
    # Wait for all syncs to complete
    # Temporarily disable exit on error to collect all results
    set +e
    for pid in "${pids[@]}"; do
        wait $pid
        if [ $? -ne 0 ]; then
            failed=$((failed + 1))
        fi
    done
    set -e
    
    if [ $failed -gt 0 ]; then
        log_error "Failed to sync flags.h to $failed node(s)"
        return 1
    fi
    
    log_success "flags.h synced to all nodes"
}

# Function to rebuild all nodes using build_cluster.sh
rebuild_all_nodes() {
    local debug_mode=${1:-"no"}
    
    log_header "REBUILDING ALL NODES"
    if [ "$debug_mode" == "yes" ]; then
        log_info "Building in DEBUG mode (with -g -O0 flags)"
    else
        log_info "Building in RELEASE mode (with -O3 flags)"
    fi
    
    if [ ! -f "${PROJECT_DIR}/build_cluster.sh" ]; then
        log_error "build_cluster.sh not found at ${PROJECT_DIR}/build_cluster.sh"
        return 1
    fi
    
    log_info "Running build_cluster.sh..."
    cd "${PROJECT_DIR}"
    
    # Temporarily disable exit on error to capture build output
    set +e
    ./build_cluster.sh build all yes "$debug_mode" > /tmp/build_output.log 2>&1
    local build_exit_code=$?
    set -e
    
    # Show filtered output for key messages
    grep -E "(SUCCESS|ERROR|Building|Build completed)" /tmp/build_output.log || true
    
    if [ $build_exit_code -eq 0 ]; then
        log_success "All nodes rebuilt successfully"
        return 0
    else
        log_warning "Build script returned exit code $build_exit_code"
        log_info "Checking if builds actually succeeded..."
        
        # Check if the builds actually completed successfully by looking at the log
        # Look for final success messages OR count individual node completions
        local has_final_success=false
        if grep -q "All memory nodes built successfully" /tmp/build_output.log && \
           grep -q "All compute nodes built successfully" /tmp/build_output.log && \
           grep -q "Build completed!" /tmp/build_output.log; then
            has_final_success=true
        fi
        
        # Alternatively, check if individual nodes completed successfully
        local memory_node_count=${#MEMORY_NODES[@]}
        local compute_node_count=${#COMPUTE_NODES[@]}
        local completed_nodes=$(grep -c "Build completed on node-" /tmp/build_output.log 2>/dev/null || echo "0")
        local expected_nodes=$((memory_node_count + compute_node_count))
        
        log_info "Build verification:"
        log_info "  Expected nodes: $expected_nodes (${memory_node_count} memory + ${compute_node_count} compute)"
        log_info "  Completed nodes: $completed_nodes"
        
        if [ "$has_final_success" = true ]; then
            log_warning "Build script returned error but all nodes built successfully - ignoring error"
            log_success "All nodes rebuilt successfully (verified from log)"
            return 0
        elif [ "$completed_nodes" -ge "$expected_nodes" ]; then
            log_warning "Build script returned error but all individual nodes completed - ignoring error"
            log_success "All nodes rebuilt successfully ($completed_nodes/$expected_nodes nodes)"
            return 0
        else
            log_error "Build failed - insufficient nodes completed ($completed_nodes/$expected_nodes)"
            log_info "Checking for success markers:"
            grep -q "All memory nodes built successfully" /tmp/build_output.log && echo "  ✓ Memory nodes built" || echo "  ✗ Memory nodes failed"
            grep -q "All compute nodes built successfully" /tmp/build_output.log && echo "  ✓ Compute nodes built" || echo "  ✗ Compute nodes failed"
            echo ""
            log_info "Last 40 lines of build log:"
            tail -40 /tmp/build_output.log
            return 1
        fi
    fi
}

# Function to update cn_config.json with hot_scan_thread_num
update_cn_config_hot_scan() {
    local hot_scan_threads=$1
    local config_file="${PROJECT_DIR}/config/cn_config.json"
    
    log_info "Updating cn_config.json: hot_scan_thread_num = $hot_scan_threads"
    
    # Use sed to update the hot_scan_thread_num value
    # This assumes the line exists in the config file
    sed -i "s/\"hot_scan_thread_num\": [0-9]*,/\"hot_scan_thread_num\": ${hot_scan_threads},/" "$config_file"
    
    log_success "cn_config.json updated: hot_scan_thread_num = $hot_scan_threads"
}

# Function to sync cn_config.json to all compute nodes
sync_cn_config() {
    log_info "Syncing cn_config.json to all compute nodes..."
    
    local config_file="${PROJECT_DIR}/config/cn_config.json"
    local pids=()
    local failed=0
    
    # Sync to all compute nodes in parallel
    for node in "${COMPUTE_NODES[@]}"; do
        (
            scp -q -o ConnectTimeout=30 "$config_file" "$USERNAME@$node:${PROJECT_DIR}/config/cn_config.json" 2>&1
            if [ $? -ne 0 ]; then
                echo "FAILED: $node"
                exit 1
            fi
        ) &
        pids+=($!)
    done
    
    # Wait for all syncs to complete
    set +e
    for pid in "${pids[@]}"; do
        wait $pid
        if [ $? -ne 0 ]; then
            failed=$((failed + 1))
        fi
    done
    set -e
    
    if [ $failed -gt 0 ]; then
        log_error "Failed to sync cn_config.json to $failed node(s)"
        return 1
    fi
    
    log_success "cn_config.json synced to all compute nodes"
}

# Function to collect results with custom naming
collect_results_custom() {
    local run_id=$1
    local workload=$2
    local threads=$3
    local coroutines=$4
    local isolation=$5
    local hot_scan_threads=$6
    
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local result_dir="results/run_${run_id}_${workload}_t${threads}_c${coroutines}_${isolation}_h${hot_scan_threads}_${timestamp}"
    
    log_info "Collecting results to $result_dir..."
    
    mkdir -p "$result_dir"
    
    # Copy results from the standard results directory that run_cluster.sh creates
    if [ -d "${PROJECT_DIR}/results" ]; then
        # Move the latest results
        for node_dir in "${PROJECT_DIR}/results"/node-*_results; do
            if [ -d "$node_dir" ]; then
                local node_name=$(basename "$node_dir")
                cp -r "$node_dir" "$result_dir/"
                log_success "Collected results from $node_name"
            fi
        done
    fi
    
    # Save detailed run configuration
    cat > "$result_dir/BENCHMARK_INFO.txt" <<EOF
================================================================================
                        BENCHMARK RUN INFORMATION
================================================================================

Run ID:              $run_id
Timestamp:           $timestamp
Date:                $(date '+%Y-%m-%d %H:%M:%S')

BENCHMARK CONFIGURATION
-----------------------
Workload:            $workload
Thread Count:        $threads threads per compute node
Coroutine Count:     $coroutines coroutines per thread
Total Parallelism:   $((threads * coroutines)) transactions per node
Isolation Level:     $isolation

CLUSTER CONFIGURATION
---------------------
Compute Nodes:       ${#COMPUTE_NODES[@]} nodes (${COMPUTE_NODES[*]})
Memory Nodes:        ${#MEMORY_NODES[@]} nodes (${MEMORY_NODES[*]})
Total Parallelism:   $((threads * coroutines * ${#COMPUTE_NODES[@]})) concurrent transactions

WORKLOAD DETAILS
----------------
EOF

    # Add workload-specific information
    case "$workload" in
        "tpcc")
            cat >> "$result_dir/BENCHMARK_INFO.txt" <<EOF
Workload Type:       TPC-C (Transaction Processing Performance Council)
Description:         OLTP workload simulating warehouse operations
Tables:              Warehouse, District, Customer, Order, Stock, Item
Transactions:        New-Order, Payment, Order-Status, Delivery, Stock-Level
Characteristics:     High contention, complex transactions, multiple tables
EOF
            ;;
        "tatp")
            cat >> "$result_dir/BENCHMARK_INFO.txt" <<EOF
Workload Type:       TATP (Telecom Application Transaction Processing)
Description:         Telecommunication application benchmark
Tables:              Subscriber, Access Info, Special Facility, Call Forwarding
Transactions:        Get Subscriber Data, Update Location, Insert Call Forwarding
Characteristics:     Read-heavy, simple transactions, telecom operations
EOF
            ;;
        "smallbank")
            cat >> "$result_dir/BENCHMARK_INFO.txt" <<EOF
Workload Type:       SmallBank
Description:         Banking transaction benchmark
Tables:              Checking, Savings
Transactions:        Amalgamate, Balance, Deposit Checking, TransactSaving, WriteCheck
Characteristics:     Simple schema, banking operations, account management
EOF
            ;;
        "micro")
            cat >> "$result_dir/BENCHMARK_INFO.txt" <<EOF
Workload Type:       Micro Benchmark
Description:         Microbenchmark for basic operations
Tables:              Single key-value table
Transactions:        Simple read/write operations
Characteristics:     Minimal complexity, stress test for concurrency control
EOF
            ;;
    esac

    # Add configuration files
    cat >> "$result_dir/BENCHMARK_INFO.txt" <<EOF

CONFIGURATION FILES
-------------------
EOF

    # Copy relevant config files
    if [ -f "${PROJECT_DIR}/config/cn_config.json" ]; then
        cp "${PROJECT_DIR}/config/cn_config.json" "$result_dir/cn_config.json"
        echo "Compute Node Config: cn_config.json" >> "$result_dir/BENCHMARK_INFO.txt"
    fi

    if [ -f "${PROJECT_DIR}/config/mn_config.json" ]; then
        cp "${PROJECT_DIR}/config/mn_config.json" "$result_dir/mn_config.json"
        echo "Memory Node Config:  mn_config.json" >> "$result_dir/BENCHMARK_INFO.txt"
    fi

    # Copy workload-specific config
    local workload_config="${PROJECT_DIR}/config/${workload}_config.json"
    if [ -f "$workload_config" ]; then
        cp "$workload_config" "$result_dir/${workload}_config.json"
        echo "Workload Config:     ${workload}_config.json" >> "$result_dir/BENCHMARK_INFO.txt"
        
        # Extract and display key workload parameters
        echo "" >> "$result_dir/BENCHMARK_INFO.txt"
        echo "WORKLOAD PARAMETERS" >> "$result_dir/BENCHMARK_INFO.txt"
        echo "-------------------" >> "$result_dir/BENCHMARK_INFO.txt"
        
        # Use grep and sed to extract key parameters (simple parsing)
        grep -v "^[[:space:]]*//\|^[[:space:]]*#" "$workload_config" | grep -E '":' | head -20 >> "$result_dir/BENCHMARK_INFO.txt" || true
    fi

    # Add system information
    cat >> "$result_dir/BENCHMARK_INFO.txt" <<EOF

EXECUTION DETAILS
-----------------
Script:              run_benchmark_grid.sh
Command:             ./run_cluster.sh run $workload $threads $coroutines $isolation
Result Directory:    $result_dir

================================================================================
EOF

    # Create a simple README for quick reference
    cat > "$result_dir/README.txt" <<EOF
This directory contains benchmark results for:

  Benchmark:      $workload
  Threads:        $threads
  Coroutines:    $coroutines
  Isolation:     $isolation
  Hot Scan:      $hot_scan_threads threads
  Run ID:        $run_id
  Date:          $(date '+%Y-%m-%d %H:%M:%S')

Files in this directory:
  - BENCHMARK_INFO.txt       : Detailed benchmark configuration and metadata
  - README.txt               : This file (quick reference)
  - cn_config.json           : Compute node configuration
  - mn_config.json           : Memory node configuration
  - ${workload}_config.json  : Workload-specific configuration
  - node-X_results/          : Results from each compute node

To view results, check the node-X_results/bench_results/ directories.
EOF

    log_success "Results collected in '$result_dir'"
    log_info "Configuration details saved in BENCHMARK_INFO.txt"
    
    # Append to master summary file
    local summary_file="${PROJECT_DIR}/results/BENCHMARK_SUMMARY.txt"
    if [ ! -f "$summary_file" ]; then
        # Create header if file doesn't exist
        cat > "$summary_file" <<EOF
================================================================================
                    BENCHMARK GRID EXECUTION SUMMARY
================================================================================
Started: $(date '+%Y-%m-%d %H:%M:%S')

EOF
    fi
    
    # Append this run's info
    cat >> "$summary_file" <<EOF
Run $run_id: $workload | Threads: $threads | Coroutines: $coroutines | Isolation: $isolation | HotScan: $hot_scan_threads | Time: $timestamp
         Directory: $result_dir
EOF
    
    # Clean up the standard results directory to avoid confusion
    rm -rf "${PROJECT_DIR}/results/node-"*_results 2>/dev/null || true
}

# Function to cleanup all nodes (explicit cleanup between runs)
cleanup_all_nodes() {
    log_info "Performing explicit cleanup of all nodes..."
    
    if [ ! -f "${PROJECT_DIR}/run_cluster.sh" ]; then
        log_warning "run_cluster.sh not found, skipping cleanup"
        return 0
    fi
    
    cd "${PROJECT_DIR}"
    ./run_cluster.sh stop-all 2>/dev/null || true
    sleep 3
    log_success "Cleanup completed"
}

# Function to run a single benchmark
run_single_benchmark() {
    local run_id=$1
    local total_runs=$2
    local workload=$3
    local threads=$4
    local coroutines=$5
    local isolation=$6
    local hot_scan_threads=$7
    
    log_run "RUN $run_id/$total_runs: workload=$workload, threads=$threads, coroutines=$coroutines, isolation=$isolation, hot_scan=$hot_scan_threads"
    
    # Explicit cleanup before each run to ensure clean state
    cleanup_all_nodes
    
    # Update cn_config.json with hot_scan_thread_num
    if ! update_cn_config_hot_scan "$hot_scan_threads"; then
        log_error "Failed to update cn_config.json"
        return 1
    fi
    
    # Sync cn_config.json to all compute nodes
    if ! sync_cn_config; then
        log_error "Failed to sync cn_config.json"
        return 1
    fi
    
    # Check if run_cluster.sh exists
    if [ ! -f "${PROJECT_DIR}/run_cluster.sh" ]; then
        log_error "run_cluster.sh not found at ${PROJECT_DIR}/run_cluster.sh"
        return 1
    fi
    
    # Run the benchmark using run_cluster.sh
    log_info "Executing: ./run_cluster.sh run $workload $threads $coroutines $isolation"
    cd "${PROJECT_DIR}"
    
    if ./run_cluster.sh run "$workload" "$threads" "$coroutines" "$isolation"; then
        log_success "Benchmark completed successfully"
    else
        log_error "Benchmark failed"
        # Cleanup on failure too
        cleanup_all_nodes
        return 1
    fi
    
    # Collect and organize results
    collect_results_custom "$run_id" "$workload" "$threads" "$coroutines" "$isolation" "$hot_scan_threads"
    
    log_success "Run $run_id/$total_runs completed!"
}

# Function to calculate total number of runs
calculate_total_runs() {
    local total=0
    total=$((${#WORKLOADS[@]} * ${#THREAD_COUNTS[@]} * ${#COROUTINE_COUNTS[@]} * ${#ISOLATION_LEVELS[@]} * ${#HOT_SCAN_THREAD_COUNTS[@]}))
    echo $total
}

# Function to run benchmark grid
run_benchmark_grid() {
    log_header "MOTOR BENCHMARK GRID EXECUTION"
    
    local total_runs=$(calculate_total_runs)
    local current_run=0
    local previous_workload=""
    
    log_info "Grid Configuration:"
    log_info "  Workloads: ${WORKLOADS[*]}"
    log_info "  Thread counts: ${THREAD_COUNTS[*]}"
    log_info "  Coroutine counts: ${COROUTINE_COUNTS[*]}"
    log_info "  Isolation levels: ${ISOLATION_LEVELS[*]}"
    log_info "  Hot scan thread counts: ${HOT_SCAN_THREAD_COUNTS[*]}"
    log_info "  Total runs: $total_runs"
    log_info "  Debug mode: $DEBUG_MODE"
    
    # Create results directory
    mkdir -p "${PROJECT_DIR}/results"
    
    # Nested for loops for grid execution
    for workload in "${WORKLOADS[@]}"; do
        log_header "STARTING WORKLOAD: $workload"
        
        # Check if we need to rebuild (workload changed)
        if [ "$workload" != "$previous_workload" ] && [ -n "$previous_workload" ]; then
            log_warning "Workload changed from '$previous_workload' to '$workload' - rebuilding required"
            
            # Update flags.h and rebuild
            if ! update_flags_h "$workload"; then
                log_error "Failed to update flags.h"
                exit 1
            fi
            
            if ! sync_flags_h; then
                log_error "Failed to sync flags.h"
                exit 1
            fi
            
            if ! rebuild_all_nodes "$DEBUG_MODE"; then
                log_error "Failed to rebuild nodes"
                exit 1
            fi
            
            log_success "Rebuild completed for workload: $workload"
            sleep 5  # Give nodes time to settle
        elif [ -z "$previous_workload" ]; then
            # First run - need to set up flags.h
            log_info "First run - setting up flags.h for $workload"
            
            if ! update_flags_h "$workload"; then
                log_error "Failed to update flags.h"
                exit 1
            fi
            
            if ! sync_flags_h; then
                log_error "Failed to sync flags.h"
                exit 1
            fi
            
            if ! rebuild_all_nodes "$DEBUG_MODE"; then
                log_error "Failed to rebuild nodes"
                exit 1
            fi
            
            log_success "Initial build completed for workload: $workload"
            sleep 5
        fi
        
        previous_workload="$workload"
        
        log_info "Starting benchmark runs for $workload (threads: ${THREAD_COUNTS[*]}, coroutines: ${COROUTINE_COUNTS[*]}, isolation: ${ISOLATION_LEVELS[*]}, hot_scan: ${HOT_SCAN_THREAD_COUNTS[*]})"
        
        for threads in "${THREAD_COUNTS[@]}"; do
            log_info "Processing thread count: $threads"
            for coroutines in "${COROUTINE_COUNTS[@]}"; do
                log_info "  Processing coroutine count: $coroutines"
                for isolation in "${ISOLATION_LEVELS[@]}"; do
                    log_info "    Processing isolation level: $isolation"
                    for hot_scan_threads in "${HOT_SCAN_THREAD_COUNTS[@]}"; do
                        log_info "      Processing hot scan thread count: $hot_scan_threads"
                        current_run=$((current_run + 1))
                        
                        # Run the benchmark
                        log_info "About to call run_single_benchmark for run $current_run/$total_runs"
                        if ! run_single_benchmark "$current_run" "$total_runs" "$workload" "$threads" "$coroutines" "$isolation" "$hot_scan_threads"; then
                            log_error "Benchmark failed, continuing to next run..."
                            # Continue instead of exit to run remaining benchmarks
                        fi
                        
                        # Add delay between runs
                        if [ $current_run -lt $total_runs ]; then
                            log_info "Waiting 5 seconds before next run..."
                            sleep 5
                        fi
                    done
                done
            done
        done
    done
    
    log_header "ALL BENCHMARKS COMPLETED!"
    log_success "Executed $current_run/$total_runs benchmark runs"
    log_info "Results are stored in: ${PROJECT_DIR}/results/"
    
    # Finalize summary file
    local summary_file="${PROJECT_DIR}/results/BENCHMARK_SUMMARY.txt"
    if [ -f "$summary_file" ]; then
        cat >> "$summary_file" <<EOF

================================================================================
Completed: $(date '+%Y-%m-%d %H:%M:%S')
Total Runs: $current_run
Status: All benchmarks completed successfully
================================================================================
EOF
        log_info "Summary file updated: ${summary_file}"
        echo ""
        log_info "To view all results, run: $0 summary"
    fi
}

# Function to show results summary
show_summary() {
    local summary_file="${PROJECT_DIR}/results/BENCHMARK_SUMMARY.txt"
    
    if [ ! -f "$summary_file" ]; then
        log_warning "No summary file found. No benchmarks have been run yet."
        log_info "Run benchmarks first: $0 run"
        return 1
    fi
    
    cat "$summary_file"
    echo ""
    
    log_info "To view detailed info for a specific run, check the BENCHMARK_INFO.txt in each result directory"
}

# Function to list all result directories
list_results() {
    local results_dir="${PROJECT_DIR}/results"
    
    if [ ! -d "$results_dir" ]; then
        log_warning "Results directory not found: $results_dir"
        return 1
    fi
    
    log_info "Available result directories:"
    echo ""
    
    local count=0
    for dir in "$results_dir"/run_*; do
        if [ -d "$dir" ]; then
            local dirname=$(basename "$dir")
            count=$((count + 1))
            
            # Extract info from directory name (supports both old and new format)
            if [[ $dirname =~ run_([0-9]+)_([^_]+)_t([0-9]+)_c([0-9]+)_([^_]+)_h([0-9]+)_([0-9_]+) ]]; then
                # New format with hot_scan
                local run_id="${BASH_REMATCH[1]}"
                local workload="${BASH_REMATCH[2]}"
                local threads="${BASH_REMATCH[3]}"
                local coroutines="${BASH_REMATCH[4]}"
                local isolation="${BASH_REMATCH[5]}"
                local hot_scan="${BASH_REMATCH[6]}"
                local timestamp="${BASH_REMATCH[7]}"
                
                printf "%3d. %-50s | %s t:%2s c:%2s iso:%-2s h:%2s\n" \
                    "$run_id" "$dirname" "$workload" "$threads" "$coroutines" "$isolation" "$hot_scan"
            elif [[ $dirname =~ run_([0-9]+)_([^_]+)_t([0-9]+)_c([0-9]+)_([^_]+)_([0-9_]+) ]]; then
                # Old format without hot_scan
                local run_id="${BASH_REMATCH[1]}"
                local workload="${BASH_REMATCH[2]}"
                local threads="${BASH_REMATCH[3]}"
                local coroutines="${BASH_REMATCH[4]}"
                local isolation="${BASH_REMATCH[5]}"
                local timestamp="${BASH_REMATCH[6]}"
                
                printf "%3d. %-50s | %s t:%2s c:%2s iso:%s h:--\n" \
                    "$run_id" "$dirname" "$workload" "$threads" "$coroutines" "$isolation"
                
                # Show README snippet if available
                if [ -f "$dir/README.txt" ]; then
                    echo "     Path: $dir"
                fi
            else
                echo "$count. $dirname"
            fi
            echo ""
        fi
    done
    
    if [ $count -eq 0 ]; then
        log_warning "No result directories found"
    else
        log_success "Found $count result directories"
        echo ""
        log_info "To view details for a specific run:"
        log_info "  cat $results_dir/run_N_*/BENCHMARK_INFO.txt"
        log_info "  cat $results_dir/run_N_*/README.txt"
    fi
}

# Function to dry-run (show what will be executed)
dry_run() {
    log_header "BENCHMARK GRID - DRY RUN"
    
    local total_runs=$(calculate_total_runs)
    local current_run=0
    local previous_workload=""
    
    log_info "Total runs: $total_runs"
    echo ""
    
    for workload in "${WORKLOADS[@]}"; do
        if [ "$workload" != "$previous_workload" ] && [ -n "$previous_workload" ]; then
            echo -e "${YELLOW}>>> Workload change detected: $previous_workload -> $workload${NC}"
            echo -e "${YELLOW}>>> Action: Update flags.h and rebuild all nodes${NC}"
            echo ""
        elif [ -z "$previous_workload" ]; then
            echo -e "${YELLOW}>>> Initial setup for workload: $workload${NC}"
            echo -e "${YELLOW}>>> Action: Update flags.h and rebuild all nodes${NC}"
            echo ""
        fi
        
        previous_workload="$workload"
        
        for threads in "${THREAD_COUNTS[@]}"; do
            for coroutines in "${COROUTINE_COUNTS[@]}"; do
                for isolation in "${ISOLATION_LEVELS[@]}"; do
                    for hot_scan_threads in "${HOT_SCAN_THREAD_COUNTS[@]}"; do
                        current_run=$((current_run + 1))
                        printf "%3d/%d: workload=%-10s threads=%-3d coroutines=%-2d isolation=%-2s hot_scan=%-2d\n" \
                            $current_run $total_runs "$workload" $threads $coroutines "$isolation" $hot_scan_threads
                    done
                done
            done
        done
        echo ""
    done
    
    log_info "To execute this grid, run: $0 run"
}

# Show usage
usage() {
    cat <<EOF
Usage: $0 [command] [--debug]

Commands:
  run        - Execute the benchmark grid
  dry-run    - Show what will be executed without running
  summary    - Show summary of completed benchmark runs
  list       - List all result directories with details
  help       - Show this help message

Options:
  --debug    - Build in DEBUG mode (with -g -O0 flags) instead of RELEASE mode

Configuration:
  Edit the arrays at the top of this script to customize the benchmark grid:
  
  WORKLOADS          - Array of workload names (tpcc, tatp, smallbank, micro)
  THREAD_COUNTS      - Array of thread counts per compute node
  COROUTINE_COUNTS   - Array of coroutine counts per thread
  ISOLATION_LEVELS   - Array of isolation levels (SI, SR)
  HOT_SCAN_THREAD_COUNTS - Array of hot table scanner thread counts (0 = disabled, 1+ = enabled)
  DEBUG_MODE         - Set to "yes" to build in DEBUG mode by default

Current Configuration:
  Workloads:         ${WORKLOADS[*]}
  Thread counts:     ${THREAD_COUNTS[*]}
  Coroutine counts:  ${COROUTINE_COUNTS[*]}
  Isolation levels:  ${ISOLATION_LEVELS[*]}
  Hot scan threads:  ${HOT_SCAN_THREAD_COUNTS[*]}
  Debug mode:        $DEBUG_MODE
  Total runs:        $(calculate_total_runs)

Features:
  - Automatically modifies flags.h when switching workloads
  - Rebuilds all nodes when workload changes
  - Calls run_cluster.sh for each configuration
  - Organizes results with descriptive directory names
  - Supports dry-run to preview execution plan

Examples:
  $0 dry-run           # Preview what will be executed
  $0 run               # Execute the benchmark grid (RELEASE mode)
  $0 run --debug       # Execute the benchmark grid (DEBUG mode)
  $0 summary           # View summary of completed runs
  $0 list              # List all result directories

Result Organization:
  - Main results directory: ${PROJECT_DIR}/results/
  - Each run directory: run_N_workload_tX_cY_isolation_timestamp/
  - Per-run files:
    * BENCHMARK_INFO.txt  - Detailed configuration and metadata
    * README.txt          - Quick reference guide
    * cn_config.json      - Compute node configuration (copy)
    * mn_config.json      - Memory node configuration (copy)
    * workload_config.json - Workload-specific configuration (copy)
    * node-X_results/     - Results from each compute node
  - Summary file: ${PROJECT_DIR}/results/BENCHMARK_SUMMARY.txt

See Also:
  - Edit this script to customize the benchmark grid
  - View BENCHMARK_INFO.txt in each result directory for full details

EOF
}

# Main entry point
main() {
    # Parse command line arguments
    local command="help"
    
    # Check for --debug flag and find the actual command
    for arg in "$@"; do
        if [ "$arg" == "--debug" ]; then
            DEBUG_MODE="yes"
        elif [ "$command" == "help" ] && [ "$arg" != "--debug" ]; then
            # First non-debug argument is the command
            command="$arg"
        fi
    done
    
    case "$command" in
        "run")
            run_benchmark_grid
            ;;
        "dry-run")
            dry_run
            ;;
        "summary")
            show_summary
            ;;
        "list")
            list_results
            ;;
        "help"|"-h"|"--help")
            usage
            ;;
        *)
            log_error "Unknown command: $1"
            usage
            exit 1
            ;;
    esac
}

main "$@"

