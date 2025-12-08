#!/bin/bash

# Motor Run Script for CloudLab c6220
# This script starts memory nodes and runs benchmarks on compute nodes

set -e  # Exit on any error

# Configuration
PROJECT_DIR="/users/Ruihong/motor"
BUILD_DIR="${PROJECT_DIR}/build"
USERNAME="Ruihong"
MEMORY_NODES=("node-8" "node-9" "node-10" "node-11" "node-12" "node-13" "node-14" "node-15")
COMPUTE_NODES=("node-0" "node-1" "node-2" "node-3" "node-4" "node-5" "node-6" "node-7")
# MEMORY_NODES=("node-8" "node-9")
# COMPUTE_NODES=("node-0" "node-1")

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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

# Function to start memory node
start_memory_node() {
    local node=$1
    local node_id=$2
    local workload=$3
    
    log_info "Starting memory node on $node (ID: $node_id) for workload: $workload..."
    
    # Convert workload to uppercase
    local workload_upper=$(echo "$workload" | tr '[:lower:]' '[:upper:]')
    
    # Setup core dumps and update config on memory node
    log_info "Updating machine_id to $node_id and workload to $workload_upper on $node..."
    ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "
        echo '/mnt/core_dump/core.%e.%p.%h.%t' | sudo tee /proc/sys/kernel/core_pattern > /dev/null
        rm -f /mnt/core_dump/core*
        # Update config - machine_id and workload
        cd $PROJECT_DIR && \
        sed -i.bak 's/\"machine_id\": [0-9]*/\"machine_id\": $node_id/' config/mn_config.json && \
        sed -i 's/\"workload\": \"[A-Z]*\"/\"workload\": \"$workload_upper\"/' config/mn_config.json && \
        rm -f config/mn_config.json.bak
    "
    
    if [ $? -ne 0 ]; then
        log_error "Failed to update config on $node"
        return 1
    fi
    log_info "Config updated successfully on $node (machine_id=$node_id, workload=$workload_upper)"
    
    # Start the memory node in background
    # Put SSH itself in background - simpler and doesn't hang
    log_info "Launching motor_mempool on $node..."
    ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "cd $BUILD_DIR/memory_node/server && nohup ./motor_mempool --no-interactive" &
    
    # Wait for SSH to complete (it will return immediately since it's backgrounded)
    sleep 1
    log_success "Memory node started on $node"
    
    # Wait a bit for the node to initialize
    sleep 2
}

# Function to stop all memory nodes
stop_memory_nodes() {
    log_info "Stopping all memory nodes..."
    for node in "${MEMORY_NODES[@]}"; do
        log_info "Stopping memory node on $node..."
        
        # First, try graceful shutdown with SIGTERM
        ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "
            sudo pkill -TERM -f motor_mempool 2>/dev/null || true
        " || true
        
        # Wait a bit for graceful shutdown
        sleep 2
        
        # Check if processes are still running
        if ssh -o ConnectTimeout=10 -o ServerAliveInterval=10 $USERNAME@$node "pgrep -f motor_mempool" >/dev/null 2>&1; then
            log_warning "Memory node $node still has processes running after SIGTERM, using aggressive kill..."
            # Only use strict measures if first attempt failed
            ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "
                # Force kill with SIGKILL
                sudo pkill -KILL -f motor_mempool 2>/dev/null || true
                # Also try killall as backup
                sudo killall -9 motor_mempool 2>/dev/null || true
                # Kill by process name pattern (more aggressive)
                for pid in \$(pgrep -f motor_mempool 2>/dev/null); do
                    sudo kill -9 \$pid 2>/dev/null || true
                done
            " || true
            
            # Verify again after aggressive kill
            sleep 1
            if ssh -o ConnectTimeout=10 -o ServerAliveInterval=10 $USERNAME@$node "pgrep -f motor_mempool" >/dev/null 2>&1; then
                log_error "Memory node $node still has processes running after aggressive kill!"
            fi
        fi
    done
    
    log_success "All memory nodes stopped"
}

# Function to check if benchmark process is running (specific pattern matching)
check_benchmark_running() {
    local node=$1
    # Match ./run followed by a workload name (tpcc, tatp, smallbank, micro)
    ssh -o ConnectTimeout=10 -o ServerAliveInterval=10 $USERNAME@$node \
        "pgrep -f './run (tpcc|tatp|smallbank|micro) ' >/dev/null 2>&1"
}

# Function to stop all compute nodes
stop_compute_nodes() {
    log_info "Stopping all compute nodes..."
    for node in "${COMPUTE_NODES[@]}"; do
        log_info "Stopping compute node on $node..."
        
        # First, try graceful shutdown with SIGTERM (using specific pattern)
        ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "
            sudo pkill -TERM -f './run (tpcc|tatp|smallbank|micro) ' 2>/dev/null || true
        " || true
        
        # Wait a bit for graceful shutdown
        sleep 2
        
        # Check if benchmark processes are still running (using specific pattern)
        if check_benchmark_running $node; then
            log_warning "Compute node $node still has benchmark processes running after SIGTERM, using aggressive kill..."
            # Only use strict measures if first attempt failed
            ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "
                # Force kill with SIGKILL (using specific pattern)
                sudo pkill -KILL -f './run (tpcc|tatp|smallbank|micro) ' 2>/dev/null || true
                # Kill by process name pattern (more aggressive, but still specific)
                for pid in \$(pgrep -f './run (tpcc|tatp|smallbank|micro) ' 2>/dev/null); do
                    sudo kill -9 \$pid 2>/dev/null || true
                done
            " || true
            
            # Verify again after aggressive kill
            sleep 1
            if check_benchmark_running $node; then
                log_error "Compute node $node still has benchmark processes running after aggressive kill!"
            fi
        fi
    done
    log_success "All compute nodes stopped"
}

# Function to verify all nodes are stopped (for cleanup verification)
verify_all_nodes_stopped() {
    log_info "Verifying all nodes are stopped..."
    local memory_running=0
    local compute_running=0
    
    for node in "${MEMORY_NODES[@]}"; do
        if ssh -o ConnectTimeout=10 -o ServerAliveInterval=10 $USERNAME@$node "pgrep -f motor_mempool" >/dev/null 2>&1; then
            log_warning "Memory node $node is still running"
            memory_running=$((memory_running + 1))
        fi
    done
    
    for node in "${COMPUTE_NODES[@]}"; do
        if check_benchmark_running $node; then
            log_warning "Compute node $node is still running"
            compute_running=$((compute_running + 1))
        fi
    done
    
    if [ $memory_running -eq 0 ] && [ $compute_running -eq 0 ]; then
        log_success "All nodes verified stopped"
        return 0
    else
        log_warning "$memory_running memory node(s) and $compute_running compute node(s) still running"
        return 1
    fi
}

# Function to check if memory nodes are ready
check_memory_nodes_ready() {
    log_info "Checking if memory nodes are ready..."
    local ready_count=0
    
    for i in "${!MEMORY_NODES[@]}"; do
        local node="${MEMORY_NODES[$i]}"
        local node_id=$i
        
        if ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "pgrep -f motor_mempool" >/dev/null 2>&1; then
            log_success "Memory node $node (ID: $node_id) is running"
            ((ready_count++))
        else
            log_warning "Memory node $node (ID: $node_id) is not running"
        fi
    done
    
    if [ $ready_count -eq ${#MEMORY_NODES[@]} ]; then
        log_success "All memory nodes are ready!"
        return 0
    else
        log_warning "Only $ready_count/${#MEMORY_NODES[@]} memory nodes are ready"
        return 1
    fi
}

# Function to run benchmark on compute node
run_benchmark() {
    local node=$1
    local node_id=$2
    local workload=$3
    local threads=$4
    local coroutines=$5
    local isolation=$6
    
    log_info "Starting benchmark on $node (ID: $node_id): $workload with $threads threads, $coroutines coroutines, isolation: $isolation"
    
    # Update machine_id in cn_config.json and run benchmark
    # SSH command runs synchronously - caller will background if needed
    ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "
        # Setup core dumps
        ulimit -c unlimited
        echo '/mnt/core_dump/core.%e.%p.%h.%t' | sudo tee /proc/sys/kernel/core_pattern > /dev/null
        rm -f /mnt/core_dump/core*
        # Update compute node config with correct machine_id
        cd $PROJECT_DIR && sed -i.bak 's/\"machine_id\": [0-9]*/\"machine_id\": $node_id/' config/cn_config.json && rm -f config/cn_config.json.bak
        # Run benchmark
        cd $BUILD_DIR/compute_node/run && ./run $workload $threads $coroutines $isolation
    "
    
    local exit_code=$?
    if [ $exit_code -eq 0 ]; then
        log_success "Benchmark completed successfully on $node"
    else
        log_error "Benchmark failed on $node with exit code $exit_code"
    fi
    return $exit_code
}

# Function to start memory nodes
start_memory_nodes() {
    local workload=$1
    
    log_info "Starting memory nodes for workload: $workload..."
    
    for i in "${!MEMORY_NODES[@]}"; do
        local node="${MEMORY_NODES[$i]}"
        local node_id=$i
        start_memory_node $node $node_id $workload
    done
    
    # Wait for memory nodes to be ready
    log_info "Waiting for memory nodes to initialize..."
    local max_wait=60
    local wait_time=0
    
    while [ $wait_time -lt $max_wait ]; do
        if check_memory_nodes_ready; then
            return 0
        fi
        log_info "Waiting for memory nodes... (${wait_time}s/${max_wait}s)"
        sleep 5
        ((wait_time += 5))
    done
    
    log_error "Memory nodes did not start within $max_wait seconds"
    return 1
}

# Function to run benchmarks on all compute nodes (always in parallel)
run_benchmarks() {
    local workload=$1
    local threads=$2
    local coroutines=$3
    local isolation=$4
    
    log_info "Starting benchmarks on compute nodes in parallel..."
    log_info "Note: Compute nodes will synchronize start time using TCP-based barrier"
    
    local pids=()
    local nodes_copy=("${COMPUTE_NODES[@]}")
    
    # Run benchmarks in parallel on all compute nodes
    # They will synchronize internally using the built-in barrier
    for i in "${!nodes_copy[@]}"; do
        local node="${nodes_copy[$i]}"
        local node_id=$i
        run_benchmark $node $node_id $workload $threads $coroutines $isolation &
        pids+=($!)
    done
    
    # Wait for all benchmarks to complete
    log_info "Waiting for all ${#pids[@]} benchmark(s) to complete..."
    local failed=0
    for i in "${!pids[@]}"; do
        local pid=${pids[$i]}
        local node=${nodes_copy[$i]}
        
        log_info "Waiting for benchmark on $node (PID: $pid)..."
        if wait $pid; then
            log_success "Benchmark on $node completed (exit code 0)"
        else
            local exit_code=$?
            log_error "Benchmark on $node failed (exit code $exit_code)"
            failed=$((failed + 1))
        fi
    done
    
    if [ $failed -gt 0 ]; then
        log_error "$failed out of ${#pids[@]} benchmark(s) failed"
        return 1
    fi
    
    log_success "All ${#pids[@]} benchmarks completed successfully!"
}

# Function to collect results
collect_results() {
    log_info "Collecting results from compute nodes..."
    
    mkdir -p results
    
    for node in "${COMPUTE_NODES[@]}"; do
        log_info "Collecting results from $node..."
        scp -r $USERNAME@$node:$PROJECT_DIR/bench_results results/${node}_results 2>/dev/null || log_warning "No results found on $node"
    done
    
    log_success "Results collected in 'results/' directory"
    
    # Aggregate throughput results from all nodes
    aggregate_results
}

# Function to aggregate throughput results from all nodes
aggregate_results() {
    log_info "Aggregating throughput results from all nodes..."
    
    # Find all workload directories
    local workloads=()
    for node_dir in results/*_results/bench_results/*/; do
        if [ -d "$node_dir" ]; then
            local workload=$(basename "$node_dir")
            if [[ ! " ${workloads[@]} " =~ " ${workload} " ]]; then
                workloads+=("$workload")
            fi
        fi
    done
    
    if [ ${#workloads[@]} -eq 0 ]; then
        log_warning "No workload results found to aggregate"
        return
    fi
    
    # Aggregate results for each workload
    for workload in "${workloads[@]}"; do
        log_info "Aggregating results for workload: $workload"
        
        local aggregated_file="results/${workload}_aggregated_result.txt"
        mkdir -p "results"
        
        # Initialize aggregated values (using awk for floating point arithmetic)
        local total_attempted_tp=0
        local total_committed_tp=0
        local sum_median_lat=0
        local max_tail_lat=0
        local node_count=0
        local system_name=""
        
        # Process each node's result file
        for node in "${COMPUTE_NODES[@]}"; do
            local node_result_file="results/${node}_results/bench_results/${workload}/result.txt"
            
            if [ -f "$node_result_file" ]; then
                log_info "  Processing results from $node..."
                
                # Read the last line from each node's result file (most recent result)
                local line=$(tail -n 1 "$node_result_file" 2>/dev/null)
                
                if [ -n "$line" ]; then
                    # Parse: system_name attempted_tp committed_tp median_lat tail_lat
                    # Use awk to parse and extract values
                    local sys_name=$(echo "$line" | awk '{print $1}')
                    local attempted_tp=$(echo "$line" | awk '{print $2}')
                    local committed_tp=$(echo "$line" | awk '{print $3}')
                    local median_lat=$(echo "$line" | awk '{print $4}')
                    local tail_lat=$(echo "$line" | awk '{print $5}')
                    
                    # Set system_name from first node (should be same for all)
                    if [ -z "$system_name" ]; then
                        system_name="$sys_name"
                    fi
                    
                    # Sum throughputs and latencies using awk for floating point
                    total_attempted_tp=$(awk "BEGIN {printf \"%.2f\", $total_attempted_tp + $attempted_tp}")
                    total_committed_tp=$(awk "BEGIN {printf \"%.2f\", $total_committed_tp + $committed_tp}")
                    sum_median_lat=$(awk "BEGIN {printf \"%.2f\", $sum_median_lat + $median_lat}")
                    
                    # Track max tail latency (use awk for comparison)
                    local is_greater=$(awk "BEGIN {print ($tail_lat > $max_tail_lat) ? 1 : 0}")
                    if [ "$is_greater" = "1" ]; then
                        max_tail_lat=$tail_lat
                    fi
                    
                    ((node_count++))
                fi
            fi
        done
        
        if [ $node_count -eq 0 ]; then
            log_warning "  No valid results found for workload $workload"
            continue
        fi
        
        # Calculate average median latency
        local avg_median_lat=$(awk "BEGIN {printf \"%.2f\", $sum_median_lat / $node_count}")
        
        # Write aggregated result
        {
            echo "# Aggregated results from $node_count compute nodes"
            echo "# Format: system_name attempted_throughput(K_txn/sec) committed_throughput(K_txn/sec) avg_median_latency(us) max_tail_latency(us)"
            echo "# Generated: $(date '+%Y-%m-%d %H:%M:%S')"
            echo ""
            printf "%-20s %12.2f %12.2f %12.2f %12.2f\n" "$system_name" "$total_attempted_tp" "$total_committed_tp" "$avg_median_lat" "$max_tail_lat"
        } > "$aggregated_file"
        
        log_success "  Aggregated results written to: $aggregated_file"
        log_info "    Total attempted throughput: ${total_attempted_tp} K txn/sec"
        log_info "    Total committed throughput: ${total_committed_tp} K txn/sec"
        log_info "    Average median latency: ${avg_median_lat} us"
        log_info "    Max tail latency: ${max_tail_lat} us"
    done
    
    log_success "Throughput aggregation completed!"
}

# Function to show memory node logs
show_memory_logs() {
    local node_id=${1:-"all"}
    
    if [ "$node_id" == "all" ]; then
        for i in "${!MEMORY_NODES[@]}"; do
            local node="${MEMORY_NODES[$i]}"
            echo ""
            log_info "=== Memory Node $node (ID: $i) ==="
            ssh -o ConnectTimeout=10 $USERNAME@$node "tail -n 20 /tmp/motor_mn_${i}.log 2>&1" || log_warning "No log found on $node"
        done
    else
        local node="${MEMORY_NODES[$node_id]}"
        log_info "=== Memory Node $node (ID: $node_id) ==="
        ssh -o ConnectTimeout=10 $USERNAME@$node "cat /tmp/motor_mn_${node_id}.log 2>&1" || log_warning "No log found on $node"
    fi
}

# Function to show status
show_status() {
    log_info "Checking cluster status..."
    
    echo ""
    log_info "=== Memory Nodes Status ==="
    check_memory_nodes_ready
    
    echo ""
    log_info "=== Compute Nodes Status ==="
    local running_count=0
    for node in "${COMPUTE_NODES[@]}"; do
        if check_benchmark_running $node; then
            log_success "$node: Running benchmark"
            ((running_count++))
        else
            log_info "$node: Idle"
        fi
    done
    
    if [ $running_count -gt 0 ]; then
        log_info "$running_count compute node(s) currently running benchmarks"
    fi
}

# Main run function
run_cluster() {
    local workload=${1:-"tpcc"}
    local threads=${2:-"8"}
    local coroutines=${3:-"2"}
    local isolation=${4:-"SR"}
    
    log_info "Starting Motor cluster execution..."
    log_info "Workload: $workload, Threads: $threads, Coroutines: $coroutines, Isolation: $isolation (parallel mode)"
    
    # Stop any existing memory nodes
    stop_memory_nodes
    
    # Stop any existing compute nodes
    stop_compute_nodes
    
    # Verify all nodes are stopped before proceeding
    if ! verify_all_nodes_stopped; then
        log_warning "Some nodes may still be running, but proceeding anyway..."
        sleep 3
    fi
    
    # Start memory nodes with workload configuration
    if ! start_memory_nodes $workload; then
        log_error "Failed to start memory nodes"
        exit 1
    fi
    
    # Run benchmarks (always in parallel)
    run_benchmarks $workload $threads $coroutines $isolation
    
    # Collect results
    collect_results
    
    # Clean up: stop all nodes to ensure clean state for next run
    log_info "Cleaning up after benchmark run..."
    stop_compute_nodes
    sleep 1
    stop_memory_nodes
    sleep 2
    
    log_success "Execution completed! Results are in the 'results' directory."
    log_info "Aggregated throughput results are in 'results/<workload>_aggregated_result.txt'"
}

# Show usage
usage() {
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  run [workload] [threads] [coroutines] [isolation]"
    echo "    - Start memory nodes and run benchmarks (parallel mode)"
    echo "    workload: tpcc|smallbank|tatp|micro (default: tpcc)"
    echo "    threads: Number of threads per compute node (default: 8)"
    echo "    coroutines: Number of coroutines per thread (default: 1)"
    echo "    isolation: SI|SR (default: SR)"
    echo ""
    echo "  start-memory [workload] - Start only memory nodes (default: tpcc)"
    echo "  stop-memory     - Stop all memory nodes"
    echo "  stop-all        - Stop all memory and compute nodes"
    echo "  status          - Show cluster status"
    echo "  logs [node_id]  - Show memory node logs (default: all)"
    echo "  collect         - Collect results from compute nodes"
    echo "  aggregate       - Aggregate throughput results from all nodes"
    echo "  help            - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 run                              # Run TPCC with defaults (parallel)"
    echo "  $0 run tpcc 16 2 SR                # Run TPCC with 16 threads, 2 coroutines"
    echo "  $0 run smallbank 8 1 SI            # Run SmallBank with SI isolation"
    echo "  $0 start-memory tpcc                # Start memory nodes for TPCC"
    echo "  $0 start-memory smallbank           # Start memory nodes for SmallBank"
    echo "  $0 status                           # Check status"
    echo "  $0 logs 0                           # Show logs for memory node 0"
    echo "  $0 collect                          # Collect results from all compute nodes"
    echo "  $0 aggregate                        # Aggregate throughput from all nodes"
    echo "  $0 stop-memory                      # Stop all memory nodes"
}

# Main entry point
main() {
    case "${1:-run}" in
        "run")
            run_cluster "${2:-tpcc}" "${3:-8}" "${4:-2}" "${5:-SR}" "${6:-yes}"
            ;;
        "start-memory")
            # Default to tpcc if no workload specified
            local workload="${2:-tpcc}"
            log_info "Starting memory nodes with workload: $workload"
            stop_memory_nodes
            sleep 2
            start_memory_nodes $workload
            ;;
        "stop-memory")
            stop_memory_nodes
            ;;
        "stop-all")
            log_info "Stopping all nodes..."
            stop_compute_nodes
            stop_memory_nodes
            verify_all_nodes_stopped
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_memory_logs "${2:-all}"
            ;;
        "collect")
            collect_results
            ;;
        "aggregate")
            aggregate_results
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

