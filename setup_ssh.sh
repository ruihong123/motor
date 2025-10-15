#!/bin/bash

# SSH Setup Script for CloudLab c6220 Motor Deployment
# This script helps set up SSH keys and test connectivity

set -e

# Configuration
USERNAME="Ruihong"  # Change this to your CloudLab username
MEMORY_NODES=("node-8" "node-9" "node-10" "node-11" "node-12" "node-13" "node-14" "node-15")
COMPUTE_NODES=("node-0" "node-1" "node-2" "node-3" "node-4" "node-5" "node-6" "node-7")
ALL_NODES=("${COMPUTE_NODES[@]}" "${MEMORY_NODES[@]}")

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

# Function to generate SSH key if it doesn't exist
generate_ssh_key() {
    if [ ! -f ~/.ssh/id_rsa ]; then
        log_info "Generating SSH key..."
        ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa -N ""
        log_success "SSH key generated"
    else
        log_info "SSH key already exists"
    fi
}

# Function to copy SSH key to all nodes
copy_ssh_keys() {
    log_info "Copying SSH keys to all nodes..."
    
    for node in "${ALL_NODES[@]}"; do
        log_info "Copying SSH key to $node..."
        if ssh-copy-id $USERNAME@$node; then
            log_success "SSH key copied to $node"
        else
            log_warning "Failed to copy SSH key to $node (may already be configured)"
        fi
    done
}

# Function to test SSH connectivity
test_connectivity() {
    log_info "Testing SSH connectivity to all nodes..."
    
    local failed_nodes=()
    
    for node in "${ALL_NODES[@]}"; do
        log_info "Testing $node..."
        if ssh -o ConnectTimeout=10 -o BatchMode=yes $USERNAME@$node "echo 'Connected to $node'" >/dev/null 2>&1; then
            log_success "$node: Connected"
        else
            log_error "$node: Failed to connect"
            failed_nodes+=($node)
        fi
    done
    
    if [ ${#failed_nodes[@]} -eq 0 ]; then
        log_success "All nodes are reachable!"
        return 0
    else
        log_error "Failed to connect to: ${failed_nodes[*]}"
        return 1
    fi
}

# Function to check if nodes are running
check_node_status() {
    log_info "Checking if nodes are running and accessible..."
    
    for node in "${ALL_NODES[@]}"; do
        log_info "Checking $node..."
        if ping -c 1 -W 5 $node >/dev/null 2>&1; then
            log_success "$node: Ping successful"
        else
            log_error "$node: Ping failed"
        fi
    done
}

# Function to show usage
usage() {
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  setup     - Generate SSH key and copy to all nodes"
    echo "  test      - Test SSH connectivity to all nodes"
    echo "  ping      - Test basic network connectivity"
    echo "  all       - Run setup, ping, and test"
    echo "  help      - Show this help message"
    echo ""
    echo "Before running this script:"
    echo "1. Make sure you can SSH to all nodes manually"
    echo "2. Update the USERNAME variable in this script"
    echo "3. Ensure all nodes are powered on and accessible"
}

# Main function
main() {
    case "${1:-help}" in
        "setup")
            generate_ssh_key
            copy_ssh_keys
            ;;
        "test")
            test_connectivity
            ;;
        "ping")
            check_node_status
            ;;
        "all")
            generate_ssh_key
            copy_ssh_keys
            check_node_status
            test_connectivity
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
