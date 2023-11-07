#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"
#include "persister.h"
#include <chrono>
#include <ctime>
#include <thread>
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "macros.h"
#include "../communicator.h"

using namespace std;

namespace janus {

#define HEARTBEAT_INTERVAL 100000

struct LogEntry{
  shared_ptr<Marshallable> cmd;
  uint64_t term;
  LogEntry();
  LogEntry(shared_ptr<Marshallable> getCmd,uint64_t getTerm)
  {
    cmd = getCmd;
    term = getTerm;
  }
};

class RaftServer : public TxLogServer {
 public:
   shared_ptr<Persister> persister;
  

  uint64_t currentTerm;
  uint64_t votedFor;
  vector<LogEntry> stateLog;
  string state;
  chrono::time_point<chrono::system_clock> lastStartTime;
  uint64_t commitIndex;
  uint64_t lastApplied;
  vector<uint64_t> nextIndex;
  vector<uint64_t> matchIndex;

  public:
    RaftServer(Frame *frame, shared_ptr<Persister> persister) ;
    ~RaftServer() ;

    bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
    void GetState(bool *is_leader, uint64_t *term);
  void Persist();
  void ReadPersist();

  private:
    bool disconnected_ = false;
    void Setup();
  void Shutdown();

  public:
    void SyncRpcExample();
    void Disconnect(const bool disconnect = true);
    void Reconnect() {
      Disconnect(false);
    }
    int generateElectionTimeout();
    void HandleAppendEntriesCombined(
                              const siteid_t& candidateId,
                              const uint64_t& prevLogIndex,
                              const uint64_t& prevLogTerm,
                              const uint64_t& logTerm,
                              const uint64_t& currentTerm,
                              const uint64_t& leaderCommitIndex,
                              const uint64_t& isHeartBeat,
                              const MarshallDeputy& md_cmd,
                              uint64_t* followerLogSize,
                              uint64_t* returnTerm,
                              bool_t* followerAppendOK);
                              
    void HandleEmptyAppendEntries(
                              const uint64_t& term,
                              const siteid_t& candidateId,
                              const uint64_t& leaderCommitIndex,
                              uint64_t* returnTerm);
    void convertToFollower(const uint64_t& term);
    void runFollowerTimeout();
    void becomeLeader();
    void HandleRequestVote(
                        const uint64_t& term,
                        const siteid_t& candidateId,
                        const uint64_t& lastLogIndex,
                        const uint64_t& lastLogTerm,
                        uint64_t* returnTerm,
                        bool_t* vote_granted);
    void HandleAppendEntries(
                      const siteid_t& candidateId,
                      const uint64_t& prevLogIndex,
                      const uint64_t& prevLogTerm,
                      const uint64_t& term,
                      const uint64_t& leaderCommitIndex,
                      const MarshallDeputy& md_cmd,
                      uint64_t* returnTerm,
                      bool_t* followerAppendOK
                      );
    void becomeCandidate();
    bool IsDisconnected();
    void startConsensus();
    bool checkMoreUpdated(uint64_t lastLogIndex,
                          uint64_t lastLogTerm);

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
  RaftCommo* commo() {
    return (RaftCommo*)commo_;
  }
};
} // namespace janus
