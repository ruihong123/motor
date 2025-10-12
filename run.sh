workload=$1
tn=$2
cn=$3
iso=$4


# Enable core dumps (unlimited size)
ulimit -c unlimited

# Set core dump pattern (requires sudo)
echo "${CORE_DUMP_DIR}/core.%e.%p.%h.%t" | sudo tee /proc/sys/kernel/core_pattern

echo "Core dumps enabled: $(cat /proc/sys/kernel/core_pattern)"
echo "Core dump size limit: $(ulimit -c)"

cd /users/Ruihong/motor/build/compute_node/run/
./run ${workload} ${tn} ${cn} ${iso}

