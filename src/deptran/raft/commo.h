#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"

namespace janus {

class TxData;

class RaftCommo : public Communicator {

 public:
  RaftCommo() = delete;
  RaftCommo(PollMgr*);

  void SendRequestVote(
                        parid_t par_id,
                        uint64_t term,
                        siteid_t candidateId,
                        uint64_t lastLogIndex,
                        uint64_t lastLogTerm,
                        uint64_t *max_return_term,
                        uint64_t *total_votes_granted);

  void SendEmptyAppendEntries(
                              parid_t par_id,
                              uint64_t term,
                              siteid_t candidateId,
                              uint64_t *maxReturnTerm);

  void SendAppendEntries(parid_t par_id,
                         siteid_t site_id,
                         shared_ptr<Marshallable> cmd);

  shared_ptr<IntEvent> 
  SendString( parid_t par_id, 
              siteid_t site_id, 
              const string& msg, 
              string* res);

  /* Do not modify this class below here */

  friend class FpgaRaftProxy;
 public:
#ifdef RAFT_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
#endif
};

} // namespace janus

