// Author: Ming Zhang
// Copyright (c) 2023

#pragma once

#include <cstdio>
#include <list>

#include "base/common.h"
#include "rlib/logging.hpp"
#include "rlib/rdma_ctrl.hpp"
#include "scheduler/coroutine.h"

using namespace rdmaio;

// Scheduling coroutines. Each txn thread only has ONE scheduler
class CoroutineScheduler {
 public:
  // The coro_num includes all the coroutines
  CoroutineScheduler(t_id_t thread_id, coro_id_t coro_num, node_id_t node_id = -1) {
    t_id = thread_id;
    n_id = node_id;
    pending_counts = new int[coro_num];
    for (coro_id_t c = 0; c < coro_num; c++) {
      pending_counts[c] = 0;
    }
    coro_array = new Coroutine[coro_num];
  }

  ~CoroutineScheduler() {
    if (pending_counts) {
      delete[] pending_counts;
    }

    if (coro_array) {
      delete[] coro_array;
    }
  }

  // For RDMA requests
  void AddPendingQP(coro_id_t coro_id, RCQP* qp);

  void AddPendingLogQP(coro_id_t coro_id, RCQP* qp);

  void RDMABatch(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num);

  bool RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num);

  bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size);

  bool RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr);

  void RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr);

  bool RDMAReadInv(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size);

  bool RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap);

  bool RDMAMaskedCAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap, uint64_t compare_mask, uint64_t swap_mask);

  // For polling
  void PollCompletion(t_id_t tid);  // There is a coroutine polling ACKs

  // Link coroutines in a loop manner
  void LoopLinkCoroutine(coro_id_t coro_num);

  // For coroutine yield, used by transactions
  void Yield(coro_yield_t& yield, coro_id_t cid);

  // Append this coroutine to the tail of the yield-able coroutine list
  // Used by coroutine 0
  void AppendCoroutine(Coroutine* coro);

  // Start this coroutine. Used by coroutine 0
  void RunCoroutine(coro_yield_t& yield, Coroutine* coro);

 public:
  Coroutine* coro_array;

  Coroutine* coro_head;

  Coroutine* coro_tail;

 private:
  t_id_t t_id;
  node_id_t n_id;

  std::list<RCQP*> pending_qps;

  // number of pending qps (i.e., the ack has not received) per coroutine
  int* pending_counts;
};

ALWAYS_INLINE
void CoroutineScheduler::AddPendingQP(coro_id_t coro_id, RCQP* qp) {
  pending_qps.push_back(qp);
  pending_counts[coro_id] += 1;
}

ALWAYS_INLINE
void CoroutineScheduler::RDMABatch(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num) {
  // piggyback_num should be 1 less than the total number of batched reqs
#if PRINT_RDMA_OPS
    if (n_id != 0) {
        printf("[RDMA BATCH] node=%d tid=%lu coro=%d num_ops=%d\n", n_id, t_id, coro_id, piggyback_num + 1);
        for (int i = 0; i <= piggyback_num; i++) {
            if (send_sr[i].opcode == IBV_WR_RDMA_READ) {
                printf("  [%d] READ  offset=%lu size=%u\n", i, send_sr[i].wr.rdma.remote_addr, send_sr[i].sg_list[0].length);
            } else if (send_sr[i].opcode == IBV_WR_RDMA_WRITE) {
                printf("  [%d] WRITE offset=%lu size=%u\n", i, send_sr[i].wr.rdma.remote_addr, send_sr[i].sg_list[0].length);
            } else if (send_sr[i].opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
                printf("  [%d] CAS   offset=%lu size=8\n", i, send_sr[i].wr.atomic.remote_addr);
            } else if (send_sr[i].opcode == IBV_WR_ATOMIC_FETCH_AND_ADD) {
                printf("  [%d] FAA   offset=%lu size=8\n", i, send_sr[i].wr.atomic.remote_addr);
            }
        }
        fflush(stdout);
    }
#endif
  send_sr[piggyback_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(FATAL) << "client: post batch fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
  }
  AddPendingQP(coro_id, qp);
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMABatchSync(coro_id_t coro_id, RCQP* qp, ibv_send_wr* send_sr, ibv_send_wr** bad_sr_addr, int piggyback_num) {
#if PRINT_RDMA_OPS
  printf("[RDMA BATCH_SYNC] node=%d tid=%lu coro=%d num_ops=%d\n", n_id, t_id, coro_id, piggyback_num + 1);
  for (int i = 0; i <= piggyback_num; i++) {
    if (send_sr[i].opcode == IBV_WR_RDMA_READ) {
      printf("  [%d] READ  offset=%lu size=%u\n", i, send_sr[i].wr.rdma.remote_addr, send_sr[i].sg_list[0].length);
    } else if (send_sr[i].opcode == IBV_WR_RDMA_WRITE) {
      printf("  [%d] WRITE offset=%lu size=%u\n", i, send_sr[i].wr.rdma.remote_addr, send_sr[i].sg_list[0].length);
    } else if (send_sr[i].opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
      printf("  [%d] CAS   offset=%lu size=8\n", i, send_sr[i].wr.atomic.remote_addr);
    } else if (send_sr[i].opcode == IBV_WR_ATOMIC_FETCH_AND_ADD) {
      printf("  [%d] FAA   offset=%lu size=8\n", i, send_sr[i].wr.atomic.remote_addr);
    }
  }
  fflush(stdout);
#endif
  send_sr[piggyback_num].wr_id = coro_id;
  auto rc = qp->post_batch(send_sr, bad_sr_addr);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post batch fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll batch fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size) {
#if PRINT_RDMA_OPS
  printf("[RDMA WRITE] node=%d tid=%lu coro=%d offset=%lu size=%zu\n", n_id, t_id, coro_id, remote_offset, size);
  fflush(stdout);
#endif
  auto rc = qp->post_send(IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAWrite(coro_id_t coro_id, RCQP* qp, char* wt_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr) {
#if PRINT_RDMA_OPS
  printf("[RDMA WRITE_MR] node=%d tid=%lu coro=%d offset=%lu size=%zu\n", n_id, t_id, coro_id, remote_offset, size);
  fflush(stdout);
#endif
  auto rc = qp->post_send_to_mr(local_mr, remote_mr, IBV_WR_RDMA_WRITE, wt_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
void CoroutineScheduler::RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
#if PRINT_RDMA_OPS
  if (n_id != 0) {  
    printf("[RDMA READ] node=%d tid=%lu coro=%d offset=%lu size=%zu\n", n_id, t_id, coro_id, remote_offset, size);
    fflush(stdout);
  }
#endif
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(FATAL) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
  }
  AddPendingQP(coro_id, qp);
    // auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
    // if (rc != SUCC) {
    //     RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    //     return;
    // }
    // ibv_wc wc{};
    // rc = qp->poll_till_completion(wc, no_timeout);
    // if (rc != SUCC) {
    //     RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    //     return;
    // }
    // return;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMARead(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size, MemoryAttr& local_mr, MemoryAttr& remote_mr) {
#if PRINT_RDMA_OPS
  printf("[RDMA READ_MR] node=%d tid=%lu coro=%d offset=%lu size=%zu\n", n_id, t_id, coro_id, remote_offset, size);
  fflush(stdout);
#endif
  auto rc = qp->post_send_to_mr(local_mr, remote_mr, IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadInv(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
#if PRINT_RDMA_OPS
  printf("[RDMA READ_INV] node=%d tid=%lu coro=%d offset=%lu size=%zu\n", n_id, t_id, coro_id, remote_offset, size);
  fflush(stdout);
#endif
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, 0, 0);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAReadSync(coro_id_t coro_id, RCQP* qp, char* rd_data, uint64_t remote_offset, size_t size) {
#if PRINT_RDMA_OPS
  printf("[RDMA READ_SYNC] node=%d tid=%lu coro=%d offset=%lu size=%zu\n", n_id, t_id, coro_id, remote_offset, size);
  fflush(stdout);
#endif
  auto rc = qp->post_send(IBV_WR_RDMA_READ, rd_data, size, remote_offset, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  ibv_wc wc{};
  rc = qp->poll_till_completion(wc, no_timeout);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMACAS(coro_id_t coro_id, RCQP* qp, char* local_buf, uint64_t remote_offset, uint64_t compare, uint64_t swap) {
#if PRINT_RDMA_OPS
  printf("[RDMA CAS] node=%d tid=%lu coro=%d offset=%lu compare=%lx swap=%lx\n", n_id, t_id, coro_id, remote_offset, compare, swap);
  fflush(stdout);
#endif
  auto rc = qp->post_cas(local_buf, remote_offset, compare, swap, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

ALWAYS_INLINE
bool CoroutineScheduler::RDMAMaskedCAS(coro_id_t coro_id,
                                       RCQP* qp,
                                       char* local_buf,
                                       uint64_t remote_offset,
                                       uint64_t compare,
                                       uint64_t swap,
                                       uint64_t compare_mask,
                                       uint64_t swap_mask) {
#if PRINT_RDMA_OPS
  printf("[RDMA MASKED_CAS] node=%d tid=%lu coro=%d offset=%lu compare=%lx swap=%lx\n", n_id, t_id, coro_id, remote_offset, compare, swap);
  fflush(stdout);
#endif
  auto rc = qp->post_masked_cas(local_buf, remote_offset, compare, swap, compare_mask, swap_mask, IBV_SEND_SIGNALED, coro_id);
  if (rc != SUCC) {
    RDMA_LOG(ERROR) << "client: post cas fail. rc=" << rc << ", tid = " << t_id << ", coroid = " << coro_id;
    return false;
  }
  AddPendingQP(coro_id, qp);
  return true;
}

// Link coroutines in a loop manner
ALWAYS_INLINE
void CoroutineScheduler::LoopLinkCoroutine(coro_id_t coro_num) {
  // The coroutines are maintained in an array,
  // but linked via pointers for efficient yield scheduling
  for (uint i = 0; i < coro_num; ++i) {
    coro_array[i].prev_coro = coro_array + i - 1;
    coro_array[i].next_coro = coro_array + i + 1;
  }
  coro_head = &(coro_array[0]);
  coro_tail = &(coro_array[coro_num - 1]);
  coro_array[0].prev_coro = coro_tail;
  coro_array[coro_num - 1].next_coro = coro_head;
}

// For coroutine yield, used by transactions
ALWAYS_INLINE
void CoroutineScheduler::Yield(coro_yield_t& yield, coro_id_t cid) {
  if (unlikely(pending_counts[cid] == 0)) {
    return;
  }
  // 1. Remove this coroutine from the yield-able coroutine list
  Coroutine* coro = &coro_array[cid];
  assert(coro->is_wait_poll == false);
  Coroutine* next = coro->next_coro;
  coro->prev_coro->next_coro = next;
  next->prev_coro = coro->prev_coro;
  if (coro_tail == coro) coro_tail = coro->prev_coro;
  coro->is_wait_poll = true;
  // 2. Yield to the next coroutine
  // RDMA_LOG(DBG) << "coro: " << cid << " yields to coro " << next->coro_id;
  RunCoroutine(yield, next);
}

// Start this coroutine. Used by coroutine 0 and Yield()
ALWAYS_INLINE
void CoroutineScheduler::RunCoroutine(coro_yield_t& yield, Coroutine* coro) {
  // RDMA_LOG(DBG) << "yield to coro: " << coro->coro_id;
  coro->is_wait_poll = false;
  yield(coro->func);
}

// Append this coroutine to the tail of the yield-able coroutine list. Used by coroutine 0
ALWAYS_INLINE
void CoroutineScheduler::AppendCoroutine(Coroutine* coro) {
  if (!coro->is_wait_poll) return;
  Coroutine* prev = coro_tail;
  prev->next_coro = coro;
  coro_tail = coro;
  coro_tail->next_coro = coro_head;
  coro_tail->prev_coro = prev;
}

ALWAYS_INLINE
void CoroutineScheduler::PollCompletion(t_id_t tid) {
  for (auto it = pending_qps.begin(); it != pending_qps.end();) {
    RCQP* qp = *it;
    struct ibv_wc wc;
    auto poll_result = qp->poll_send_completion(wc);  // The qp polls its own wc
    if (poll_result == 0) {
      it++;
      continue;
    }
    if (unlikely(wc.status != IBV_WC_SUCCESS)) {
      // Get transaction context
      extern __thread const char* current_tx_name[];
      extern __thread uint64_t current_tx_id[];
      extern __thread table_id_t last_accessed_table[];
      extern __thread itemkey_t last_accessed_key[];
      extern __thread uint64_t stat_attempted_tx_total;
      extern __thread uint64_t stat_committed_tx_total;
      
      fprintf(stderr, "\n=======================================\n");
      fprintf(stderr, "RDMA COMPLETION ERROR (Thread %u)\n", (unsigned)tid);
      fprintf(stderr, "=======================================\n");
      fprintf(stderr, "Status: %d (%s)\n", wc.status, ibv_wc_status_str(wc.status));
      fprintf(stderr, "Vendor Error: %u", wc.vendor_err);
      if (wc.vendor_err == 129) {
        fprintf(stderr, " (Remote responder NAK/not ready)");
      }
      fprintf(stderr, "\n");
      
      fprintf(stderr, "Opcode: %d ", wc.opcode);
      if (wc.opcode == IBV_WC_SEND || wc.opcode == IBV_WC_RDMA_WRITE) {
        fprintf(stderr, "(RDMA_WRITE)");
      } else if (wc.opcode == IBV_WC_RDMA_READ) {
        fprintf(stderr, "(RDMA_READ)");
      } else if (wc.opcode == IBV_WC_COMP_SWAP) {
        fprintf(stderr, "(CAS/ATOMIC)");
      } else if (wc.opcode == IBV_WC_FETCH_ADD) {
        fprintf(stderr, "(FETCH_ADD)");
      } else {
        fprintf(stderr, "(UNKNOWN)");
      }
      fprintf(stderr, "\n");
      
      fprintf(stderr, "WR ID (coro_id): %llu\n", (unsigned long long)wc.wr_id);
      fprintf(stderr, "Remote Memory Node: %d\n", qp->idx_.node_id);
      fprintf(stderr, "Local QP num: %u\n", wc.qp_num);
      fprintf(stderr, "Pending QPs in list: %zu\n", pending_qps.size());
      if (wc.wr_id < 100) {
        fprintf(stderr, "Pending count for coro: %d\n", pending_counts[wc.wr_id]);
      }
      fprintf(stderr, "---------------------------------------\n");
      fprintf(stderr, "Transaction Context:\n");
      if (wc.wr_id < 10 && current_tx_name[wc.wr_id]) {
        fprintf(stderr, "  TX Name: %s\n", current_tx_name[wc.wr_id]);
        fprintf(stderr, "  TX ID: %llu\n", (unsigned long long)current_tx_id[wc.wr_id]);
        fprintf(stderr, "  Last Table ID: %llu ", (unsigned long long)last_accessed_table[wc.wr_id]);
        
        // Print table name
        const char* table_names[] = {
          "Warehouse", "District", "Customer", "History", "NewOrder",
          "Order", "OrderLine", "Item", "Stock", "CustomerIndex", "OrderIndex"
        };
        if (last_accessed_table[wc.wr_id] < 11) {
          fprintf(stderr, "(%s)", table_names[last_accessed_table[wc.wr_id]]);
        }
        fprintf(stderr, "\n");
        fprintf(stderr, "  Last Key: 0x%llx\n", (unsigned long long)last_accessed_key[wc.wr_id]);
      }
      fprintf(stderr, "Transaction Stats (This Thread):\n");
      fprintf(stderr, "  Attempted: %llu\n", (unsigned long long)stat_attempted_tx_total);
      fprintf(stderr, "  Committed: %llu\n", (unsigned long long)stat_committed_tx_total);
      fprintf(stderr, "  Abort Rate: %.2f%%\n", 
              stat_attempted_tx_total > 0 ? 
              100.0 * (stat_attempted_tx_total - stat_committed_tx_total) / stat_attempted_tx_total : 0.0);
      fprintf(stderr, "=======================================\n");
      fflush(stderr);
      abort();
    }
    auto coro_id = wc.wr_id;
    if (coro_id == 0) continue;
    assert(pending_counts[coro_id] > 0);
    pending_counts[coro_id] -= 1;
    if (pending_counts[coro_id] == 0) {
      AppendCoroutine(&coro_array[coro_id]);
    }
    it = pending_qps.erase(it);
  }
}
