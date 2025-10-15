#!/bin/bash

# Motor Repository Parallel Sync Script
# Synchronizes to all nodes in parallel for faster deployment

# Configuration
LOCAL_MOTOR_DIR="/users/Ruihong/motor"
REMOTE_MOTOR_DIR="/users/Ruihong/motor"
USERNAME="$USER"

# Node configuration
START_NODE=0
END_NODE=17
NODE_PREFIX="node-"

# Rsync options
RSYNC_OPTS="-azh --progress"
EXCLUDE_OPTS=""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "======================================================"
echo "  Motor Parallel Sync (node-${START_NODE} to node-${END_NODE})"
echo "======================================================"
echo ""

# Temporary directory for logs
TMP_DIR="/tmp/motor_sync_$$"
mkdir -p "$TMP_DIR"

# Start all syncs in parallel
PIDS=()
for node in $(seq $START_NODE $END_NODE); do
    node_name="${NODE_PREFIX}${node}"
    log_file="${TMP_DIR}/${node_name}.log"
    
    # Start background sync job
    {
        echo "[$(date '+%H:%M:%S')] Starting sync to ${node_name}"
        
        # Rsync (automatically creates remote directory)
        rsync $RSYNC_OPTS $EXCLUDE_OPTS \
            "$LOCAL_MOTOR_DIR/" \
            "${USERNAME}@${node_name}:${REMOTE_MOTOR_DIR}/"
        
        if [ $? -eq 0 ]; then
            echo "Clearing motor logs in /tmp on ${node_name}..."
            ssh -o ConnectTimeout=10 -o ServerAliveInterval=10 "${USERNAME}@${node_name}" \
                "rm -f /tmp/motor_mn_*.log /tmp/motor_cn_*.log 2>/dev/null || true" 2>/dev/null
            
            if [ $? -eq 0 ]; then
                echo "Logs cleared on ${node_name}"
            else
                echo "Warning: Could not clear logs on ${node_name} (non-critical)"
            fi
            
            echo "SUCCESS: ${node_name}"
            exit 0
        else
            echo "FAILED: ${node_name}"
            exit 1
        fi
    } > "$log_file" 2>&1 &
    
    # Store the PID
    PIDS+=($!)
    echo -e "${YELLOW}Started sync to ${node_name} (PID: $!)${NC}"
done

echo ""
echo "Waiting for all syncs to complete..."
echo ""

# Wait for all background jobs
SUCCESS_COUNT=0
FAIL_COUNT=0
FAILED_NODES=()

for i in "${!PIDS[@]}"; do
    pid=${PIDS[$i]}
    node=$((START_NODE + i))
    node_name="${NODE_PREFIX}${node}"
    
    wait $pid
    exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ ${node_name} completed${NC}"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        echo -e "${RED}✗ ${node_name} failed${NC}"
        FAIL_COUNT=$((FAIL_COUNT + 1))
        FAILED_NODES+=("$node_name")
    fi
done

echo ""
echo "======================================================"
echo "  Summary"
echo "======================================================"
echo "Total nodes:  $((END_NODE - START_NODE + 1))"
echo -e "Successful:   ${GREEN}${SUCCESS_COUNT}${NC}"
echo -e "Failed:       ${RED}${FAIL_COUNT}${NC}"

if [ ${#FAILED_NODES[@]} -gt 0 ]; then
    echo ""
    echo "Failed nodes: ${FAILED_NODES[*]}"
    echo ""
    echo "Check logs in: $TMP_DIR/"
else
    echo -e "${GREEN}All nodes synchronized successfully!${NC}"
    rm -rf "$TMP_DIR"
fi

echo "======================================================"

exit $FAIL_COUNT

