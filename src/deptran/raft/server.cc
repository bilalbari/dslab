#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

static int volatile x1 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_STATE,
                                     [] () -> Marshallable* {
                                       return new StateMarshallable;
                                     });

RaftServer::RaftServer(Frame * frame, shared_ptr<Persister> persister_) {
  Log_info("Server %lu -> Inside constructor call", loc_id_);
  frame_ = frame ;
  persister = persister_;
  mtx_.lock();
  stateLog = vector<LogEntry>();
  if(persister->RaftStateSize() != -1)
  {
    // stateLog = vector<LogEntry>();
    // Marshallable* m = new CmdData();
    // std::shared_ptr<Marshallable> my_shared(m);
    // stateLog.push_back(LogEntry(my_shared,0));
    ReadPersist();
  }
  else
  {
    Log_info("Server %lu -> Initialising for the first time",loc_id_);
    currentTerm = 1;
    votedFor = 6;
    auto cmdptr = std::make_shared<TpcCommitCommand>();
    auto vpd_p = std::make_shared<VecPieceData>();
    vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
    cmdptr->tx_id_ = 0;
    cmdptr->cmd_ = vpd_p;
    auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
    stateLog.push_back(LogEntry(cmdptr_m,0));  
    //MarshallDeputy md(cmdptr_m);
    // Marshallable* m = new CmdData();
    // std::shared_ptr<Marshallable> my_shared(m);
    // stateLog.push_back(LogEntry(cmdptr_m,0));
    commitIndex = 0;
    lastApplied = 0;
  }
  aboutToDie = 0;
  state = "follower";
  lastStartTime = chrono::system_clock::now();
  nextIndex = vector<uint64_t>{1,1,1,1,1};
  matchIndex = vector<uint64_t>(5,0);
  mtx_.unlock();
  Log_info("Server initialization completed for %lli",loc_id_);
}

RaftServer::~RaftServer() {
  Log_info("Server %lu -> Inside destructor call",loc_id_);
}

int RaftServer::generateElectionTimeout(){
    srand(loc_id_);
    return 800+(rand()%400);
}

void RaftServer::convertToFollower(const uint64_t& term){

  Log_info("Starting as follower for server %lli",loc_id_);

  mtx_.lock();
  
  votedFor = 6;
  lastStartTime = std::chrono::system_clock::now();
  currentTerm = term;
  state = "follower";
  mtx_.unlock();

  runFollowerTimeout();

}

void RaftServer::runFollowerTimeout(){

  int electionTimeout = generateElectionTimeout();

  chrono::milliseconds endTimeout(electionTimeout);

  mtx_.lock();
  chrono::duration<double,milli> time_spent = chrono::system_clock::now() - lastStartTime;
  mtx_.unlock();
  
  while(time_spent < endTimeout && aboutToDie==0)
  {
    Coroutine::Sleep(electionTimeout*1000);
    mtx_.lock();
    if(state != "follower")
    {
      mtx_.unlock();
      break;
    }
    time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
  }

  mtx_.lock();

  state = "candidate";

  mtx_.unlock();
}


void RaftServer::becomeCandidate()
{
    
  int electionTimeout = generateElectionTimeout();
  
  chrono::milliseconds endTimeout(electionTimeout);
  Log_info("Server %lu-> Candidate timeout is %i ",loc_id_,electionTimeout);
  
  mtx_.lock();
  state = "candidate";
  currentTerm++;
  lastStartTime = chrono::system_clock::now();
  votedFor = loc_id_;
  chrono::duration<double,milli> time_spent = 
                  chrono::system_clock::now() - lastStartTime;
  auto proxies = commo()->rpc_par_proxies_[0];
  mtx_.unlock();

  mtx_.lock();
  uint64_t total_votes_granted = 1;
  uint64_t max_return_term = 0;
  uint64_t tempCurrentTerm = currentTerm;
  uint64_t tempLastLogIndex = stateLog.size()-1;
  uint64_t tempLastLogTerm = stateLog[tempLastLogIndex].term;
  mtx_.unlock();

  Log_info("Server %lu -> Contesting election in term %lu",loc_id_,currentTerm);
  for(int i=0;i<proxies.size();i++)
  {
    if(proxies[i].first != loc_id_)
    {
      uint64_t return_term = 0;
      bool_t vote_granted = 0;
      Log_info("Server %lu -> Sending request vote to %lu with lastLogIndex as %lu and lastLogTerm as %lu",loc_id_,proxies[i].first, tempLastLogIndex, tempLastLogTerm);
      auto event = commo()->SendRequestVote(
                            0,
                            proxies[i].first,
                            tempCurrentTerm, // sending my term
                            loc_id_,  //sending my id
                            tempLastLogIndex, //my last log index
                            tempLastLogTerm, // my last log term
                            &return_term,
                            &vote_granted
                          );
      
      event->Wait(10000);
      mtx_.lock();
      if(event->status_ == Event::TIMEOUT)
      {
        Log_info("Server %lu -> Timeout happened for send request votes to %lu",loc_id_,proxies[i].first);
      }
      else
      {
        Log_info("Server %lu -> Got reply from server %lu with vote count %d and term %lu",loc_id_,proxies[i].first,vote_granted,return_term);
        if(return_term > 0)
        { 
          max_return_term = max(return_term,max_return_term);
          if(max_return_term > currentTerm)
          {
            mtx_.unlock();
            break;
          }
          if(vote_granted == 1)
          {
            total_votes_granted++;
          }
          if(total_votes_granted >= 3)
          {
            mtx_.unlock();
            break;
          }
        }
      }
      mtx_.unlock();
    }
  }
  mtx_.lock();
  if(state == "candidate")
  {
    if(max_return_term > currentTerm)
    {
      Log_info("Server %lu -> Received bigger term after requestVote. Current term is %lu and got %lu",loc_id_,currentTerm,max_return_term);
      state = "follower";
      currentTerm = max_return_term;
    }
    else if(total_votes_granted >= 3)
    {
      Log_info("Server %lu -> Election supremacy. Won election in the term %lu",loc_id_,currentTerm);
      state = "leader";
    }
  }
  
  if(state != "candidate") 
  {
    mtx_.unlock();
  }
  else
  {
    time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
    Coroutine::Sleep((endTimeout-time_spent).count()*1000);
  }
  mtx_.unlock();
}

bool RaftServer::checkMoreUpdated(
                                  uint64_t lastLogIndex,
                                  uint64_t lastLogTerm)
{
  uint64_t tempLastLogIndex = stateLog.size()-1;
  pair<uint64_t,uint64_t> my_pair = {tempLastLogIndex,stateLog[tempLastLogIndex].term};
  if(lastLogTerm < my_pair.second)
  {
    Log_info("Server %lu -> Found last log term smaller. Have %lu, checked %lu",loc_id_,my_pair.second,lastLogTerm);
    return false;
  }
  else if(lastLogTerm > my_pair.second)
  {
    Log_info("Server %lu -> Found last term greater. Giving vote", loc_id_);
    return true;
  }
  else
  {
    if(lastLogIndex>= my_pair.first)
    {
      Log_info("Server %lu -> Have %lu, Found %lu. Giving vote",loc_id_,my_pair.first,lastLogIndex);
      return true;
    }
    else
    {
      Log_info("Server %lu -> Have %lu, Found %lu. Not Giving vote",loc_id_,my_pair.first,lastLogIndex);
      return false;
    }
    //return lastLogIndex >= my_pair.first;
  }
}

void RaftServer::HandleAppendEntriesCombined(
                            const siteid_t& candidateId,
                            const uint64_t& prevLogIndex,
                            const uint64_t& prevLogTerm,
                            const uint64_t& logTerm,
                            const uint64_t& leaderCurrentTerm,
                            const uint64_t& leaderCommitIndex,
                            const uint64_t& isHeartbeat,
                            const MarshallDeputy& md_cmd,
                            uint64_t* followerLogSize,
                            uint64_t* returnTerm,
                            bool_t* followerAppendOK)
{
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *followerLogSize = stateLog.size();
  *returnTerm = currentTerm;
  *followerAppendOK = 0;
  if(leaderCurrentTerm < currentTerm)
  {
    *followerAppendOK = 0;
  }
  else if(currentTerm <= leaderCurrentTerm)
  {
    if(state != "follower")
    {
      votedFor = 6;
      lastStartTime = std::chrono::system_clock::now();
      currentTerm = leaderCurrentTerm;
      state = "follower";
    }
    {
      currentTerm = leaderCurrentTerm;
      lastStartTime = std::chrono::system_clock::now();
      uint64_t currentMaxLogIndex = stateLog.size()-1;
      if(currentMaxLogIndex < prevLogIndex || stateLog[prevLogIndex].term != prevLogTerm)
      {
        Log_info("Server %lu -> Previous log term does not match for append entry from %lu. Sending false",loc_id_,candidateId);
        *followerAppendOK = 0;
      }
      else
      {
        if(isHeartbeat == 0)
        {
          if(currentMaxLogIndex >= prevLogIndex+1)
          {
            if(stateLog[prevLogIndex+1].term != logTerm)
            {
              stateLog.erase(stateLog.begin()+prevLogIndex+1,stateLog.end());
              std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
              stateLog.push_back(LogEntry(cmd,logTerm));
            }
          }
          else
          {
            std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
            stateLog.push_back(LogEntry(cmd,logTerm));
          }
        }
        *followerAppendOK = 1;
        if(leaderCommitIndex > commitIndex)
          commitIndex = min(leaderCommitIndex,prevLogIndex+1);
        while((lastApplied+1) <= commitIndex)
        {
          app_next_(*(stateLog[lastApplied+1].cmd));
          lastApplied++;
        }
      }
    }
  }
}



void RaftServer::HandleRequestVote(
                      const uint64_t& term,
                      const siteid_t& candidateId,
                      const uint64_t& lastLogIndex,
                      const uint64_t& lastLogTerm,
                      uint64_t* returnTerm,
                      bool_t* vote_granted)
{
  Log_info("Server %lu -> Got request vote call for %lu. With lastlogIndex as %lu and lastLogTerm as %lu",loc_id_,candidateId,lastLogIndex,lastLogTerm);
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *vote_granted = 0;
  *returnTerm = currentTerm;
  Log_info("Server %lu -> Received request vote call",loc_id_);
  if(term > currentTerm)
  {
    votedFor = 6;
    currentTerm = term;
    if(state != "follower")
    {
      Log_info("Server %lu -> Found state as follower. Converting to follower",loc_id_);
      state = "follower";
    }
    if(votedFor == 6 || votedFor == candidateId)
    {
      if(checkMoreUpdated(lastLogIndex, lastLogTerm))
      {
        Log_info("Server %lu -> Found logs updated, Giving vote",loc_id_);
        *vote_granted = 1;
        votedFor = candidateId;
        lastStartTime = std::chrono::system_clock::now();
      }
      else
      {
        Log_info("Server %lu -> Found logs as not updated, Not giving vote",loc_id_);
      }
    }
  }
  else
    Log_info("Server %lu -> Found smaller term. Not giving vote",loc_id_);
}


void RaftServer::becomeLeader()
{
  
  Log_info("Server %lu -> Starting as leader",loc_id_);
  mtx_.lock();
  state = "leader";
  chrono::time_point<chrono::system_clock> last_heartbeat_time;
  chrono::duration<double,milli> time_since_heartbeat;
  uint64_t tempLogIndex = stateLog.size();
  nextIndex = vector<uint64_t>(5,tempLogIndex);
  matchIndex = vector<uint64_t>(5,0);
  matchIndex[loc_id_] = stateLog.size()-1;
  auto proxies = commo()->rpc_par_proxies_[0];
  mtx_.unlock();
  
  while(true && aboutToDie == 0)
  {
    last_heartbeat_time = chrono::system_clock::now();
    for(int i=0;i<proxies.size();i++)
    {
      mtx_.lock();
      if(state != "leader")
      {
        Log_info("Server %lu -> State changed while sending heartbeats, breaking out");
        mtx_.unlock();
        break;
      }
      mtx_.unlock();
      Log_info("Server %lu -> Sending global append entries to %lu",loc_id_,proxies[i].first);
      if(proxies[i].first != loc_id_)
      {
        mtx_.lock();
        Log_info("Server %lu -> tempNextIndex is %lu and lastLogIndex is %lu",loc_id_,nextIndex[i],stateLog.size()-1);
        if(nextIndex[i] <= (stateLog.size()-1))
        {
          Log_info("Server %lu -> Sending append entry to %lu",loc_id_,proxies[i].first);
          uint64_t returnTerm = 0;
          uint64_t prevLogIndex = nextIndex[i]-1;
          uint64_t prevLogTerm = stateLog[prevLogIndex].term;
          uint64_t currentNextIndex= nextIndex[i];
          uint64_t currentLogTerm = stateLog[currentNextIndex].term;
          uint64_t followerLogSize = stateLog.size();
          std::shared_ptr<Marshallable> my_shared = stateLog[currentNextIndex].cmd;
          bool_t followerAppendOK = 0;
          uint64_t isHeartbeat = 0;
          Log_info("Server %lu -> Inside start concensus before calling SendAppendEntry for %lu",loc_id_,proxies[i].first);
          mtx_.unlock();
          auto event = commo()->SendAppendEntriesCombined(
                                      0,
                                      proxies[i].first,
                                      loc_id_,
                                      prevLogIndex,
                                      prevLogTerm,
                                      currentLogTerm,
                                      currentTerm,
                                      commitIndex,
                                      isHeartbeat,
                                      my_shared,
                                      &followerLogSize,
                                      &returnTerm,
                                      &followerAppendOK
                                    );
          
          event->Wait(20000);
          
          mtx_.lock();
          //Log_info("Server %lu -> Inside start consensus after calling sleep after wait for %lu",loc_id_,proxies[i].first);
          Log_info("Server %lu -> Got back return term %lu from %lu",loc_id_,returnTerm,proxies[i].first);
          if(event->status_ == Event::TIMEOUT)
          {
            Log_info("Server %lu -> Append entry to %lu timed out",loc_id_,proxies[i].first);
          }
          else
          {
            //Log_info("AppendEntry from %lu to %lu timed out",proxies[i].first,loc_id_);
            Log_info("Server %lu -> Got response from %lu -> %lu as term, %d as didAppend",loc_id_,proxies[i].first,returnTerm,followerAppendOK);
            if(returnTerm > currentTerm)
            {
              Log_info("Server %lu -> Received greater term from append entry response",loc_id_);
              state = "follower";
              lastStartTime = std::chrono::system_clock::now();
              votedFor = 6;
              currentTerm = returnTerm;
            }
            else
            {
              if(followerAppendOK == 1)
              {
                Log_info("Server %lu -> Append entry for %lu accepted",loc_id_,proxies[i].first);
                matchIndex[i] = nextIndex[i];
                nextIndex[i]++;
                Log_info("Server %lu -> Increased nextIndex value to %lu and match index to %lu",loc_id_,nextIndex[i],matchIndex[i]);
              }

              else if( returnTerm != 0 )
              {
                Log_info("Server %lu -> Append entry for %lu rejected",loc_id_,proxies[i].first);
                if(nextIndex[i]>1)
                {
                  Log_info("Server %lu -> First append entry failed, retrying",loc_id_);
                  nextIndex[i] = min(nextIndex[i]-1,followerLogSize);
                }
              }
            }
          }
          mtx_.unlock();
        }
        else
        {
          Log_info("Server %lu -> Sending heartbeats to %lu",loc_id_,proxies[i].first);
          uint64_t returnTerm = 0;
          uint64_t prevLogIndex = nextIndex[i]-1;
          uint64_t prevLogTerm = stateLog[prevLogIndex].term;
          uint64_t currentNextIndex= nextIndex[i];
          Marshallable* m = new CmdData();
          uint64_t followerLogSize = stateLog.size();
          std::shared_ptr<Marshallable> my_shared(m);
          bool_t followerAppendOK = 0;
          uint64_t isHeartbeat = 1;
          Log_info("Server %lu -> Inside start concensus before calling SendAppendEntry as heartbeat",loc_id_);
          mtx_.unlock();
          auto event = commo()->SendAppendEntriesCombined(
                                      0,
                                      proxies[i].first,
                                      loc_id_,
                                      prevLogIndex,
                                      prevLogTerm,
                                      currentTerm,
                                      currentTerm,
                                      commitIndex,
                                      isHeartbeat,
                                      my_shared,
                                      &followerLogSize,
                                      &returnTerm,
                                      &followerAppendOK
                                    );
          event->Wait(20000);
          mtx_.lock();
          if(event->status_ == Event::TIMEOUT)
          {
            Log_info("Server %lu -> Append entry to %lu timed out",loc_id_,proxies[i].first);
          }
          else
          {
            Log_info("Server %lu -> Got response from %lu -> %lu as term, %d as didAppend",loc_id_,proxies[i].first,returnTerm,followerAppendOK);
            if(returnTerm > currentTerm)
            {
              Log_info("Server %lu -> Received greater term from append entry response",loc_id_);
              state = "follower";
              currentTerm = returnTerm;
            }
            else
            {
              if(followerAppendOK)
              {
                matchIndex[i] = stateLog.size()-1;
                Log_info("Sever %lu -> Increased nextIndex value to %lu and match index to %lu",loc_id_,nextIndex[i],matchIndex[i]);
              }
              else if(returnTerm != 0)
              {
                if(nextIndex[i] > 1)
                {
                  nextIndex[i] = min(nextIndex[i]-1,followerLogSize);
                }
              }
            }
          }
          mtx_.unlock();
        }
        mtx_.unlock();
      }
      mtx_.lock();
      uint64_t j=commitIndex+1;
      while(true)
      {
        uint64_t total_agreement = 0;
        Log_info("Server %lu -> Checking majority for commitIndex %lu",loc_id_,j);
        for(int i=0;i<5;i++)
        {
          if(j <= matchIndex[i])
          {
            Log_info("Server %lu -> Found matchIndex %lu for server %d",loc_id_, matchIndex[i],i);
            total_agreement++;
          }
        }
        if(total_agreement >= 3 && stateLog[j].term == currentTerm)
        {
          Log_info("Server %lu -> Found majority for commit index %lu",loc_id_,j);
          commitIndex=j;
        }
        j++;
        if(j>(stateLog.size()-1))
          break;
      }
      while((lastApplied+1)<=commitIndex)
      {
          app_next_(*(stateLog[lastApplied+1].cmd));
          lastApplied++;
      }
      mtx_.unlock();
    }
    
    mtx_.lock();
    if(state != "leader")
    {
      mtx_.unlock();
      Log_info("Server %lu -> Found state as not leader, stepping down",loc_id_);
      break;
    }
    mtx_.unlock();
    time_since_heartbeat = chrono::system_clock::now() - last_heartbeat_time;
    uint64_t sleep_time = 100-time_since_heartbeat.count();
    
    if(sleep_time>0 && sleep_time<100)
    {
      Log_info("Server %lu -> Sleeping for %d",loc_id_,sleep_time);
      mtx_.unlock();
      Coroutine::Sleep(sleep_time*1000);
      Log_info("Server %lu -> Inside leader after consensus after coroutine sleep 2",loc_id_);
    }

    mtx_.lock();
    if(state!="leader")
    {
      mtx_.unlock();
      break;
    }
    mtx_.unlock();
    Log_info("Server %lu -> Got about to die as %d inside leader",loc_id_,aboutToDie);
  }
}

void RaftServer::Setup() {
  
  
  Coroutine::CreateRun([this](){
    while(true && aboutToDie == 0)
    {
      mtx_.lock();
      if(state == "follower" && aboutToDie == 0)
      {
        uint64_t temp_term = currentTerm;
        mtx_.unlock();
        convertToFollower(temp_term);
      }
      if(state == "candidate" && aboutToDie == 0)
      {
        mtx_.unlock();
        becomeCandidate();
      }
      if(state == "leader" && aboutToDie == 0)
      {
        mtx_.unlock();
        becomeLeader();
      }
      mtx_.unlock();
      Log_info("Server %lu -> Got about to die inside setup as %lu",loc_id_,aboutToDie);
    }
  });

}



void RaftServer::Shutdown() {
  Log_info("Server %lu -> Received shutdown call",loc_id_);
  mtx_.lock();
  aboutToDie = 1;
  mtx_.unlock();
  Coroutine::Sleep(1000000);
  mtx_.lock();
  Persist();
  mtx_.unlock();
}

vector<MarshallDeputy> RaftServer::convertToDeputy(vector<LogEntry> a)
{
  // auto cmdptr = std::make_shared<TpcCommitCommand>();
  // auto vpd_p = std::make_shared<VecPieceData>();
  // vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
  // cmdptr->tx_id_ = 0;
  // cmdptr->cmd_ = vpd_p;
  // auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
  // MarshallDeputy md(cmdptr_m);
  vector<MarshallDeputy> myEntries;
  int x = a.size();
  Log_info("Server %lu -> Found size of logs to be %d",loc_id_,x);
  for(int i=0;i<a.size();i++)
  {
    MarshallDeputy md(a[i].cmd);
    //shared_ptr<MarshallDeputy> sharedDeputy(md);  
    //.push_back(StoringLogEntry(md,a[i].term));
    myEntries.push_back(md);
  }
  return myEntries;
}

vector<uint64_t> RaftServer::getAllLogTerms(vector<LogEntry> a)
{
  vector<uint64_t> myEntries(a.size(),0);
  for(int i=0;i<a.size();i++)
  {
    // std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(a[i].cmd).sp_data_;
    // myEntries.push_back(LogEntry(cmd,a[i].term));
    Log_info("Server %lu -> Retrieving terms %d",loc_id_,i);
    Log_info("Server %lu -> term in entry is %lu",loc_id_,a[i].term);
    myEntries[i] = a[i].term;
    Log_info("Server %lu -> Stored entry is %lu",loc_id_,myEntries[i]);
  }
  Log_info("Server %lu -> Completed",loc_id_);
  return myEntries;
}

void RaftServer::convertBackFromPersisted(vector<uint64_t> termVector,vector<MarshallDeputy> commandVector)
{
  // vector<LogEntry> myEntries;
  // Marshallable* m = new CmdData();
  // std::shared_ptr<Marshallable> my_shared(m);
  // myEntries.push_back(LogEntry(my_shared,0));
  for(int i=0;i<termVector.size();i++)
  {
    //std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(commandVector[i]).sp_data_;
    stateLog.push_back(LogEntry(commandVector[i].sp_data_,termVector[i]));
    Log_info("Server %lu -> While build for %d, got term %lu",loc_id_,i,termVector[i]);
    // myEntries.push_back();
  }
  Log_info("Server %lu -> Updated state last log index as %lu and last log term as %lu",loc_id_,stateLog.size()-1,stateLog[stateLog.size()-1].term);
  // return myEntries;
}



void RaftServer::Persist() {
  Log_info("Server %lu -> Received persist call",loc_id_);
  auto myCurrentState = make_shared<StateMarshallable>();
  myCurrentState->persistedTerm = currentTerm;
  Log_info("Server %lu -> Stored current term",loc_id_);
  myCurrentState->persistedVotedFor = votedFor;
  Log_info("Server %lu -> Stored voted for",loc_id_);
  myCurrentState->persistedCommitIndex = commitIndex;
  Log_info("Server %lu -> Stored commit index",loc_id_);
  myCurrentState->persistedLastApplied = lastApplied;
  Log_info("Server %lu -> Stored last applied",loc_id_);
  // myCurrentState->persistedLogTerms = getAllLogTerms(stateLog);
  myCurrentState->persistedLogs = stateLog;
  // Log_info("Server %lu -> Stored log terms",loc_id_);
  // myCurrentState->persistedLogs = convertToDeputy(stateLog);
  // Log_info("Server %lu -> Stored Logs",loc_id_);
  // MarshallDeputy md;
  // vector<MarshallDeputy> myDeputyVector(stateLog.size()-1,md);
  // Log_info("Server %lu -> Stored values till 3",loc_id_);
  // for(int i=1;i<stateLog.size();i++)
  // {
  //   myTermVector[i-1] = stateLog[i].term;
  //   MarshallDeputy md1(stateLog[i].cmd);
  //   myDeputyVector[i-1] = md1;
  // }
  // for(int i=0;i<myTermVector.size();i++)
  //   Log_info("Server %lu -> Storing terms %lu",loc_id_,myTermVector[i]);
  // myCurrentState -> persistedTerms = myTermVector;
  // myCurrentState -> persistedCommands = myDeputyVector;
  Log_info("Server %lu -> Stored values till 4",loc_id_);
  auto myMarshallable = dynamic_pointer_cast<Marshallable>(myCurrentState);
  persister->SaveRaftState(myMarshallable);
  Log_info("Server %lu -> Persist ends",loc_id_);
}

void RaftServer::ReadPersist() {
  mtx_.lock();
  Log_info("Server %lu -> Found existing persisted state",loc_id_);
  auto tempState = persister->ReadRaftState();
  auto myStoredState =  (StateMarshallable*)(&(*tempState));
  Log_info("Server %lu -> State retrieved",loc_id_);
  currentTerm = myStoredState -> persistedTerm;
  Log_info("Server %lu -> Got old term as %lu",loc_id_,currentTerm);
  votedFor = myStoredState -> persistedVotedFor;
  Log_info("Server %lu -> Got old votedFor as %lu",loc_id_,votedFor);
  commitIndex = myStoredState -> persistedCommitIndex;
  Log_info("Server %lu -> Got commit index as %lu",loc_id_,commitIndex);
  lastApplied = myStoredState -> persistedLastApplied;
  Log_info("Server %lu -> Got last applied as %lu",loc_id_,lastApplied);
  stateLog = myStoredState -> persistedLogs;
  // for(int i=0;i<myStoredState->persistedTerms.size();i++)
  //   Log_info("Server %lu -> Found term %lu",loc_id_, myStoredState->persistedTerms[i]);
  // convertBackFromPersisted(myStoredState->persistedLogTerms,myStoredState->persistedLogs);
  Log_info("Server %lu -> Got back state logs",loc_id_);
  // for(int i=0;i<myVector.size();i++)
  // {
  //   //std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(myStoredState->persistedCommands[i]).sp_data_;
  //   stateLog.push_back(myVector[i]);
  // }
  //Log_info("Server %lu -> Retrieved till 2",loc_id_);
  // Marshallable* m = new CmdData();
  // std::shared_ptr<Marshallable> my_shared(m);
  // stateLog.push_back(LogEntry(my_shared,0));
  //Log_info("Server %lu -> Retrieved till 3",loc_id_);
  // vector<LogEntry> newVector(myStoredState->persistedLogs.begin(),myStoredState->persistedLogs.end());
  // for(auto x: newVector)
  // {
  //   Log_info("Server %lu -> Retrieved till here",loc_id_);
  //   stateLog.push_back(x);
  // }
  mtx_.unlock();
}

bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term)
{
  /* Your code here. This function can be called from another OS thread. */
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *term = currentTerm;
  if(state != "leader")
  {
    //Log_info("Server %lu -> Not the leader, returning false from start",loc_id_);
    return false;
  }
  else
  {
    Log_info("Server %lu -> Beginning start logic",loc_id_);
    stateLog.push_back(LogEntry(cmd,currentTerm));
    matchIndex[loc_id_] = stateLog.size()-1;
    uint64_t getLastLogIndex = (stateLog.size()-1);
    *index = getLastLogIndex;
    return true;
  }
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  
  mtx_.lock();

  if(state == "leader")
    *is_leader = true;
  else
    *is_leader = false;
  *term = currentTerm;

  mtx_.unlock();

  Log_info("Server %lu -> replying with values as %d and term %lu ",loc_id_,*is_leader,*term);
}

void RaftServer::SyncRpcExample() {
  /* This is an example of synchronous RPC using coroutine; feel free to 
     modify this function to dispatch/receive your own messages. 
     You can refer to the other function examples in commo.h/cc on how 
     to send/recv a Marshallable object over RPC. */
  Coroutine::CreateRun([this](){
    string res;

    auto event = commo()->SendString(0, /* partition id is always 0 for lab1 */
                                     0, "hello", &res);
    event->Wait(1000000); //timeout after 1000000us=1s
    if (event->status_ == Event::TIMEOUT) 
    {
      Log_info("timeout happens");
    } 
    else 
    {
      Log_info("rpc response is: %s", res.c_str()); 
    }
  });
}

/* Do not modify any code below here */

void RaftServer::Disconnect(const bool disconnect) {
  Log_info("Server %lu -> Inside connect/disconnect",loc_id_);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  RaftCommo *c = (RaftCommo*) commo();
  if (disconnect) {
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() > 0);
    auto sz = c->rpc_par_proxies_.size();
    _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
    c->rpc_par_proxies_ = {};
    verify(_proxies[partition_id_][loc_id_].size() == sz);
    verify(c->rpc_par_proxies_.size() == 0);
  } else {
    verify(_proxies[partition_id_][loc_id_].size() > 0);
    auto sz = _proxies[partition_id_][loc_id_].size();
    c->rpc_par_proxies_ = {};
    c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
    _proxies[partition_id_][loc_id_] = {};
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() == sz);
  }
  disconnected_ = disconnect;
}

bool RaftServer::IsDisconnected() 
{
  Log_info("Server %lu -> Checking is disconnected. Found %d",loc_id_, disconnected_);
  return disconnected_;
}

} // namespace janus
