#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"
#define Log_info Log_debug

namespace janus {

class TxData;

class RaftCommo : public Communicator {

 public:
  RaftCommo() = delete;
  RaftCommo(PollMgr*);

  shared_ptr<IntEvent> 
  SendRequestVote(
                    parid_t par_id,
                    siteid_t site_id,
                    uint64_t term,
                    //siteid_t candidateId,
                    uint64_t lastLogIndex,
                    uint64_t lastLogTerm,
                    uint64_t *return_term,
                    bool_t *vote_granted);

  shared_ptr<IntEvent>
  SendAppendEntriesCombined(
                            const parid_t& par_id,
                            const siteid_t& site_id,
                            const uint64_t& prevLogIndex,
                            const uint64_t& prevLogTerm,
                            const uint64_t& logTerm,
                            const uint64_t& leaderCurrentTerm,
                            const uint64_t& leaderCommitIndex,
                            const uint64_t& isHeartbeat,
                            shared_ptr<Marshallable> cmd,
                            uint64_t* followerLogSize,
                            uint64_t* returnTerm,
                            bool_t* followerAppendOK);

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

