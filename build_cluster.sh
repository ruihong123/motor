#!/bin/bash

# Motor Build Script for CloudLab c6220
# This script builds Motor on all compute and memory nodes via SSH

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

# Function to check if a node is reachable
check_node_connectivity() {
    local node=$1
    if ssh -o ConnectTimeout=10 -o BatchMode=yes $USERNAME@$node "echo 'Connected'" >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to build the project on a node
build_on_node() {
    local node=$1
    local build_type=$2  # "server" for memory nodes, "client" for compute nodes
    local debug_mode=$3  # "yes" for Debug build, "no" for Release build (default)
    
    if [ "$debug_mode" == "yes" ]; then
        log_info "Building Motor on $node (type: $build_type, mode: DEBUG)..."
    else
        log_info "Building Motor on $node (type: $build_type, mode: RELEASE)..."
    fi
    
    local build_cmd="./build.sh"
    if [ "$build_type" == "server" ]; then
        build_cmd="$build_cmd -s"
    fi
    if [ "$debug_mode" == "yes" ]; then
        build_cmd="$build_cmd -d"
    fi
    
    ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "cd $PROJECT_DIR && $build_cmd" 2>&1 | tail -n 5
    local build_exit_code=${PIPESTATUS[0]}
    
    if [ $build_exit_code -eq 0 ]; then
        log_success "Build completed on $node"
        return 0
    else
        log_error "Build failed on $node"
        return 1
    fi
}

# Function to clean build directory on a node
clean_on_node() {
    local node=$1
    
    log_info "Cleaning build directory on $node..."
    ssh -o ConnectTimeout=30 -o ServerAliveInterval=10 $USERNAME@$node "cd $PROJECT_DIR && rm -rf build" || true
    log_success "Clean completed on $node"
}

# Function to build all nodes in parallel
build_all_parallel() {
    local build_type=$1  # "server" or "client"
    local debug_mode=$2  # "yes" or "no" for debug build
    local nodes=("${@:3}")  # All remaining arguments are nodes
    
    local pids=()
    local failed_nodes=()
    
    # Start builds in parallel
    for node in "${nodes[@]}"; do
        build_on_node "$node" "$build_type" "$debug_mode" &
        pids+=($!)
    done
    
    # Wait for all builds to complete
    # Temporarily disable exit on error to collect all results
    set +e
    local idx=0
    for pid in "${pids[@]}"; do
        wait $pid
        if [ $? -ne 0 ]; then
            failed_nodes+=("${nodes[$idx]}")
        fi
        ((idx++))
    done
    set -e
    
    if [ ${#failed_nodes[@]} -gt 0 ]; then
        log_error "Build failed on nodes: ${failed_nodes[*]}"
        return 1
    fi
    
    return 0
}

# Main build function
build_cluster() {
    local mode=${1:-"all"}
    local parallel=${2:-"yes"}
    local debug_mode=${3:-"no"}  # Default: "no" = Release mode, "yes" = Debug mode
    
    log_info "Starting Motor build on cluster..."
    if [ "$debug_mode" == "yes" ]; then
        log_info "Mode: $mode, Parallel: $parallel, Build Type: DEBUG (with -g -O0)"
    else
        log_info "Mode: $mode, Parallel: $parallel, Build Type: RELEASE (with -O3)"
    fi
    
    # Check connectivity to all nodes
    if [ "$mode" == "all" ] || [ "$mode" == "memory" ]; then
        log_info "Checking connectivity to memory nodes..."
        local failed_nodes=()
        
        for node in "${MEMORY_NODES[@]}"; do
            if ! check_node_connectivity $node; then
                failed_nodes+=($node)
                log_error "Cannot connect to $node"
            fi
        done
        
        if [ ${#failed_nodes[@]} -gt 0 ]; then
            log_error "Cannot connect to memory nodes: ${failed_nodes[*]}"
            exit 1
        fi
    fi
    
    if [ "$mode" == "all" ] || [ "$mode" == "compute" ]; then
        log_info "Checking connectivity to compute nodes..."
        local failed_nodes=()
        
        for node in "${COMPUTE_NODES[@]}"; do
            if ! check_node_connectivity $node; then
                failed_nodes+=($node)
                log_error "Cannot connect to $node"
            fi
        done
        
        if [ ${#failed_nodes[@]} -gt 0 ]; then
            log_error "Cannot connect to compute nodes: ${failed_nodes[*]}"
            exit 1
        fi
    fi
    
    # Build memory nodes
    if [ "$mode" == "all" ] || [ "$mode" == "memory" ]; then
        log_info "Building memory nodes..."
        
        if [ "$parallel" == "yes" ]; then
            build_all_parallel "server" "$debug_mode" "${MEMORY_NODES[@]}"
        else
            for node in "${MEMORY_NODES[@]}"; do
                build_on_node $node "server" "$debug_mode"
            done
        fi
        
        log_success "All memory nodes built successfully!"
    fi
    
    # Build compute nodes
    if [ "$mode" == "all" ] || [ "$mode" == "compute" ]; then
        log_info "Building compute nodes..."
        
        if [ "$parallel" == "yes" ]; then
            build_all_parallel "client" "$debug_mode" "${COMPUTE_NODES[@]}"
        else
            for node in "${COMPUTE_NODES[@]}"; do
                build_on_node $node "client" "$debug_mode"
            done
        fi
        
        log_success "All compute nodes built successfully!"
    fi
    
    log_success "Build completed!"
}

# Clean function
clean_cluster() {
    local mode=${1:-"all"}
    
    log_info "Cleaning build directories on cluster..."
    
    if [ "$mode" == "all" ] || [ "$mode" == "memory" ]; then
        log_info "Cleaning memory nodes..."
        for node in "${MEMORY_NODES[@]}"; do
            clean_on_node $node &
        done
        wait
    fi
    
    if [ "$mode" == "all" ] || [ "$mode" == "compute" ]; then
        log_info "Cleaning compute nodes..."
        for node in "${COMPUTE_NODES[@]}"; do
            clean_on_node $node &
        done
        wait
    fi
    
    log_success "Clean completed!"
}

# Show usage
usage() {
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  build [mode] [parallel] [debug]  - Build Motor on cluster nodes"
    echo "    mode: all|memory|compute (default: all)"
    echo "    parallel: yes|no (default: yes)"
    echo "    debug: yes|no (default: no) - Build in Debug mode with -g -O0"
    echo "  clean [mode]             - Clean build directories"
    echo "    mode: all|memory|compute (default: all)"
    echo "  help                     - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 build                 # Build all nodes in parallel (Release)"
    echo "  $0 build all yes yes     # Build all nodes in parallel (Debug)"
    echo "  $0 build all no          # Build all nodes sequentially (Release)"
    echo "  $0 build memory          # Build only memory nodes (Release)"
    echo "  $0 build compute yes yes # Build only compute nodes in parallel (Debug)"
    echo "  $0 clean                 # Clean all nodes"
    echo "  $0 clean memory          # Clean only memory nodes"
}

# Main entry point
main() {
    case "${1:-build}" in
        "build")
            build_cluster "${2:-all}" "${3:-yes}" "${4:-no}"
            ;;
        "clean")
            clean_cluster "${2:-all}"
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

