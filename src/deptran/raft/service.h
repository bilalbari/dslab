#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "server.h"
#include "macros.h"

class SimpleCommand;
namespace janus {

class TxLogServer;
class RaftServer;
class RaftServiceImpl : public RaftService {
 public:
  RaftServer* svr_;
  RaftServiceImpl(TxLogServer* sched);

  /*
  For each RPC, assign default handlers, 
  which means assigning default values in case of failed RPC
  and the return values are used to idetify a failed RPC

  RpcHandler usage: RpcHandler(RPC_NAME, N_PARAMS, PARAMS...) { DEFAULTLOGIC }

  RPC_NAME: should match the name of the RPC declared in raft_rpc.rpc
  N_PARAMS: number of RPC arguments + number of RPC return values
  PARAMS: the RPC arguments and return values in the same order as in 
          raft_rpc.rpc, with comma separations between the type and name.
  DEFAULTLOGIC: write code to assign default values to the RPC’s return 
                value in these brackets. This code will get invoked when
                the server is disconnected from the network to simulate a 
                failed RPC. It is important that your RPC sender code recognizes 
                the default values and ignores them when they happen.
  */

  RpcHandler( RequestVote, 6,
              const uint64_t&, term,
              const siteid_t&, candidateId,
              const uint64_t&, lastLogIndex,
              const uint64_t&, lastLogTerm,
                    uint64_t*, returnTerm,
                    bool_t*, vote_granted) {
    *returnTerm = 0;
    *vote_granted = 0;
  }

  RpcHandler( AppendEntriesCombined, 11,
              const siteid_t&, candidateId,
              const uint64_t&, prevLogIndex,
              const uint64_t&, prevLogTerm,
              const uint64_t&, logTerm,
              const uint64_t&, currentTerm,
              const uint64_t&, leaderCommitIndex,
              const uint64_t&, isHeartbeat,
              const MarshallDeputy&, md_cmd,
              uint64_t*, followerLogSize,
              uint64_t*, returnTerm,
              bool_t*, followerAppendOK) {
    *followerLogSize = 1;
    *returnTerm = 0;
    *followerAppendOK = 0;
  }

  RpcHandler(HelloRpc, 2, const string&, req, string*, res) {
    *res = "error"; 
  };

  int ReturnLastLogTerm();

};

} // namespace janus
