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

struct StoringLogEntry{
  MarshallDeputy cmd;
  uint64_t term;
  StoringLogEntry();
  StoringLogEntry(MarshallDeputy getCmd,uint64_t getTerm)
  {
    cmd = getCmd;
    term = getTerm;
  }
};

class StateMarshallable : public Marshallable {
 public:
  StateMarshallable() : Marshallable(MarshallDeputy::CMD_STATE) {} 
  uint64_t persistedTerm;
  uint64_t persistedVotedFor;
  uint64_t persistedCommitIndex;
  uint64_t persistedLastApplied;
  // vector<StoringLogEntry> persistedLogs;
  //vector<uint64_t> persistedLogTerms{};
  //vector<MarshallDeputy> persistedLogs{};
  vector<LogEntry> persistedLogs;
  Marshal& ToMarshal(Marshal& m) const override {
    Log_info("Inside ToMarshall");
    m << persistedTerm;
    Log_info("Term persisted");
    m << persistedVotedFor;
    Log_info("VotedFor persisted");
    m << persistedCommitIndex;
    Log_info("Commit Index persisted");
    m << persistedLastApplied;
    Log_info("Last Applied persisted");
    int32_t sz = persistedLogs.size();
    m << sz;
    int c = 0;
    for(auto& x: persistedLogs)
    {
      Log_info("Found term as %lu",x.term);
      if(c==0)
      {
        auto cmdptr = std::make_shared<TpcCommitCommand>();
        auto vpd_p = std::make_shared<VecPieceData>();
        vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
        cmdptr->tx_id_ = 100;
        cmdptr->cmd_ = vpd_p;
        auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
        MarshallDeputy marshallD(cmdptr_m);
        m << marshallD;
        Log_info("command persisted");
        m << x.term;
        Log_info("Term persisted");
        c++;
      }
      else
      {
        MarshallDeputy marshallD(x.cmd);
        m << marshallD;
        Log_info("command persisted");
        m << x.term;
        Log_info("Term persisted");
      }
    }
    return m;
  }

  Marshal& FromMarshal(Marshal& m) override {
    Log_info("Inside from marshal");
    m >> persistedTerm;
    Log_info("got term");
    m >> persistedVotedFor;
    Log_info("got voted for");
    m >> persistedCommitIndex;
    Log_info("got persisted");
    m >> persistedLastApplied;
    Log_info("got last applied");
    int32_t sz;
    m >> sz;
    for(int i=0;i<sz;i++)
    {
      uint64_t term;
      MarshallDeputy marshallD;
      m >> marshallD;
      m >> term;
      //persistedLogs.push_back(marshallD);
      std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(marshallD).sp_data_;
      LogEntry newEntry(cmd,term);
      Log_info("Created entry");
      persistedLogs.push_back(newEntry);
      //persistedLogTerms.push_back(term);
      Log_info("Entry pushed");
    }
    return m;
  }
};

class RaftServer : public TxLogServer {
 public:
   shared_ptr<Persister> persister;
  
  uint64_t aboutToDie;
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
    vector<MarshallDeputy> convertToDeputy(vector<LogEntry> a);
    vector<uint64_t> getAllLogTerms(vector<LogEntry> a);
    void convertBackFromPersisted(vector<uint64_t> termVector,vector<MarshallDeputy> commandVector);
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
    void becomeCandidate();
    bool IsDisconnected();
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
