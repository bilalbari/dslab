#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"


namespace janus {

RaftServer::RaftServer(Frame * frame) {
  frame_ = frame ;
  mtx_.lock();

  currentTerm = 1;
  votedFor = 6;
  state = "follower";
  lastStartTime = chrono::system_clock::now();
  stateLog = vector<LogEntry>();
  Marshallable* m = new CmdData();
  std::shared_ptr<Marshallable> my_shared(m);
  stateLog.push_back(LogEntry(my_shared,0));
  commitIndex = 0;
  lastApplied = 0;
  nextIndex = vector<uint64_t>{1,1,1,1,1};
  matchIndex = vector<pair<uint64_t,uint64_t>>{ {0,currentTerm},
                                                {0,currentTerm},
                                                {0,currentTerm},
                                                {0,currentTerm},
                                                {0,currentTerm}
                                              };

  mtx_.unlock();
  Log_info("Server initialization completed for %lli",loc_id_);
}

RaftServer::~RaftServer() {
  /* Your code here for server teardown */

}

int RaftServer::generateElectionTimeout(){
    srand(loc_id_);
    return 250+(rand()%200);
}

void RaftServer::HandleEmptyAppendEntries(
                            const uint64_t& term,
                            const siteid_t& candidateId,
                            const uint64_t& leaderCommitIndex,
                            uint64_t* returnTerm
                            )
{
  mtx_.lock();

  *returnTerm = currentTerm;

  if(term > currentTerm)
  {
    Log_info("%lli -> Received term is higher than current term, becoming a follower",loc_id_);
    votedFor = 6;
    lastStartTime = std::chrono::system_clock::now();
    currentTerm = term;
    if(state != "follower")
    {
      state = "follower";
      mtx_.unlock();
      return;
    }
    mtx_.unlock();
  }
  if(currentTerm <= term)
  {
    if(state != "follower")
    {
      Log_info("%lli -> State is not follower, switching to follower",loc_id_);
      votedFor = 6;
      lastStartTime = std::chrono::system_clock::now();
      currentTerm = term;
      state = "follower";
    }
    else
    {
      lastStartTime = std::chrono::system_clock::now();
    }
  }
  if(leaderCommitIndex > commitIndex)
  {
    uint64_t getLastLogIndex = stateLog.size()-1;
    commitIndex = min(leaderCommitIndex,getLastLogIndex);
  }
  while((lastApplied+1) <= commitIndex)
  {
    Log_info("Server %lu -> Found last applied behind, increasing commit index",loc_id_);
    app_next_(*(stateLog[lastApplied+1].cmd));
    lastApplied++;
  }
  mtx_.unlock();
  Log_info("%lli -> Returning my term -> %lli from empty append entry",loc_id_,*returnTerm);
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

  //Log_info("Timeout for server %lli is %i",loc_id_,electionTimeout);
  chrono::milliseconds endTimeout(electionTimeout);

  mtx_.lock();
  chrono::duration<double,milli> time_spent = chrono::system_clock::now() - lastStartTime;
  mtx_.unlock();
  
  while(time_spent < endTimeout)
  {
      
    //Log_info("Server %lu -> Inside runFollowerTimeout before coroutine sleep",loc_id_);
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

  //Log_info("Server %lu ->Timeout completed as follower. Switching to candidate",loc_id_);
  state = "candidate";

  mtx_.unlock();
}


void RaftServer::becomeCandidate()
{
    
  /*
    Getting random timeout for the sever
  */
  
  int electionTimeout = generateElectionTimeout();
  
  chrono::milliseconds endTimeout(electionTimeout);
  Log_info("Server %lu-> Candidate timeout is %i ",loc_id_,electionTimeout);
  
  /*
    Locking the mutex to get state variables
    before sending off RPC for request vote
  */

  mtx_.lock();
  state = "candidate";
  currentTerm++;
  lastStartTime = chrono::system_clock::now();
  votedFor = loc_id_;
  chrono::duration<double,milli> time_spent = 
                  chrono::system_clock::now() - lastStartTime;
  mtx_.unlock();

  auto ev = Reactor::CreateSpEvent<IntEvent>();
  //Log_info("Server %lu -> Inside become candidate, going for request vote",loc_id_);
  Coroutine::CreateRun([=](){

    uint64_t max_return_term=0;
    uint64_t total_votes_received=1;

    mtx_.lock();
    uint64_t tempCurrentTerm = currentTerm;
    uint64_t tempLastLogIndex = stateLog.size()-1;
    uint64_t tempLastLogTerm = stateLog[tempLastLogIndex].term;
    mtx_.unlock();

    //Log_info("Server %lu -> Inside become candidate, before sending request vote",loc_id_);
    auto event = commo()->SendRequestVote(
                            0,
                            tempCurrentTerm, // sending my term
                            loc_id_,  //sending my id
                            tempLastLogIndex, //my last log index
                            tempLastLogTerm, // my last log term
                            &max_return_term,
                            &total_votes_received
                          );
    //Log_info("Server %lu -> Inside become candidate, after send request vote before wait",loc_id_);                      
    Coroutine::Sleep(5000);
    //if(event->get() == 0)
    if(event->status_ == Event::INIT)
      event->Wait(5000);
    //Log_info("Server %lu -> Inside become candidate, after send request vote after wait",loc_id_);                      
    if(event->status_ == Event::TIMEOUT)
    {
      Log_info("Server %lu -> Timeout happened for all send request votes",loc_id_);
    }
    else
    {
      
      Log_info("Server %lu -> Got reply from all servers with total vote count %d and max term %lu",loc_id_,total_votes_received,max_return_term);
      
      mtx_.lock();
      
      if(state != "candidate")
      {
        //Log_info("Server %lu -> Changed state to %s while waiting for votes",loc_id_,state.c_str());
        if(state == "leader")
        {
          nextIndex = vector<uint64_t>{1,1,1,1,1};
          matchIndex = vector<pair<uint64_t,uint64_t>>{
                                                      {0,currentTerm},
                                                      {0,currentTerm},
                                                      {0,currentTerm},
                                                      {0,currentTerm},
                                                      {0,currentTerm}
                                                    };
        }
        mtx_.unlock();
        //return;
      }
      else if(max_return_term != 0)
      { 
      
        /*
          Checking for the return term
          and breaking in case it is more than current
          term
        */
        //Log_info("Server %lu -> Max return term is not zero",loc_id_);
        if(max_return_term > currentTerm)
        {
          Log_info("Server %lu -> Received bigger term after requestVote. Current term is %lu and got %lu",loc_id_,currentTerm,max_return_term);
          
          state = "follower";
          currentTerm = max_return_term;
          mtx_.unlock();
          //return;               
        }
        else
        {
          /*
          Checking for majority of votes received
          and changing to a leader in case that happens
          */
          if(total_votes_received >= (commo()->rpc_par_proxies_[0].size()+1)/2)
          {
            Log_info("Server %lu -> Election supremacy. Won election in the term %lu",loc_id_,currentTerm);
            state = "leader";
            // nextIndex = vector<uint64_t>{1,1,1,1,1};
            // matchIndex = vector<pair<uint64_t,uint64_t>>{
            //                                           {0,currentTerm},
            //                                           {0,currentTerm},
            //                                           {0,currentTerm},
            //                                           {0,currentTerm},
            //                                           {0,currentTerm}
            //                                         };
            mtx_.unlock();
          }
          else
          {
            //Log_info("Server %lu -> Did not received majority votes",loc_id_);
          }
        }
      }
      mtx_.unlock();
    }
    //{
      //std::lock_guard<std::recursive_mutex> guard(mtx_);
    //Log_info("Server %lu -> Calling set on outer ev inside become candidate smart pointer global %p",loc_id_,ev.get());
    //if(ev->get() == 0 )
    Log_info("Server %lu -> Setting request vote ev");
    if(ev->status_ == Event::INIT)
      ev->Set(1);
    //}
  });

  //Log_info("Server %lu -> Calling wait on outer ev inside become candidate smart pointer global %p",loc_id_,ev.get());
  //Coroutine::Sleep(5000);
  //if(ev->get() == 0)
  Log_info("Server %lu -> Before wait on request vote ev");
  if(ev->status_ == Event::INIT)
    ev->Wait(10000);
  Log_info("Server %lu -> After Wait on request vote ev");
  //Log_info("Server %lu -> Inside becomeCandidate after coroutine sleep",loc_id_);
  
  mtx_.lock();

  if(state != "candidate") 
  {
    mtx_.unlock();
    return;
  }
  else
  {
    time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
    //Log_info("Server %lu -> Will send votes after %f",loc_id_,(endTimeout-time_spent).count());
    Coroutine::Sleep((endTimeout-time_spent).count()*1000);
  }
  mtx_.unlock();
}

void RaftServer::HandleAppendEntries(
                      const siteid_t& candidateId,
                      const uint64_t& prevLogIndex,
                      const uint64_t& prevLogTerm,
                      const uint64_t& term,
                      const uint64_t& leaderCommitIndex,
                      const MarshallDeputy& md_cmd,
                      uint64_t* returnTerm,
                      bool_t* followerAppendOK
                      )
{
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *returnTerm = currentTerm;
  std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  Log_info("Server %lu -> Received append entry with leader commitIndex %lu",leaderCommitIndex);
  if(term < currentTerm)
  {
    Log_info("Server %lu -> Received append entry with less term, sending false",loc_id_);
    *followerAppendOK = false;
    return;
  }
  if(stateLog.size() >= prevLogIndex)
  {
    if(stateLog[prevLogIndex].term != prevLogTerm)
    {
      Log_info("Server %lu -> Previous log term does not match for append entry from %lu. Sending false",loc_id_,candidateId);
      *followerAppendOK = false;
      return;
    }
    stateLog.erase(std::next(stateLog.begin(),prevLogIndex+1),stateLog.end());
    stateLog.push_back(LogEntry(cmd,term));
    *followerAppendOK = true;
    Log_info("Server %lu -> Appended log and updated commit index to %lu",loc_id_,commitIndex);
  }
  else
  {
    Log_info("Server %lu -> Log size is smaller than sent logs. Sending false",loc_id_);
    *followerAppendOK = false;
    return;
  }
  if(leaderCommitIndex > commitIndex)
      commitIndex = min(leaderCommitIndex,stateLog.size()-1);
  while((lastApplied+1) <= commitIndex)
  {
    Log_info("Server %lu -> Found last applied behind, increasing commit index",loc_id_);
    app_next_(*(stateLog[lastApplied+1].cmd));
    lastApplied++;
  }
  Log_info("Server %lu -> Append entry from %lu processed",loc_id_,candidateId);
}

bool RaftServer::checkMoreUpdated(
                                  uint64_t lastLogIndex,
                                  uint64_t lastLogTerm)
{
  //std::lock_guard<std::recursive_mutex> guard(mtx_);
  uint64_t tempLastLogIndex = stateLog.size()-1;
  pair<uint64_t,uint64_t> my_pair = {tempLastLogIndex,stateLog[tempLastLogIndex].term};
  if(lastLogTerm < my_pair.second)
    return false;
  else if(lastLogTerm > my_pair.second)
    return true;
  else
    return lastLogIndex >= my_pair.first;
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
  uint64_t lastLogIndex = stateLog.size()-1;
  std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  //Log_info("Server %lu -> Received append entry with leader commitIndex %lu",leaderCommitIndex);
  if(leaderCurrentTerm < currentTerm)
  {
    //Log_info("Server %lu -> Received append entry with less term, sending false",loc_id_);
    //votedFor = 6;
    //lastStartTime
    *followerAppendOK = 0;
    return;
  }
  if(currentTerm <= leaderCurrentTerm)
  {
    if(state != "follower")
    {
      //Log_info("%lli -> State is not follower, switching to follower",loc_id_);
      votedFor = 6;
      lastStartTime = std::chrono::system_clock::now();
      currentTerm = leaderCurrentTerm;
      state = "follower";
      return;
    }
    else
    {
      //votedFor = 6;
      //lastStartTime = std::chrono::system_clock::now();
      currentTerm = leaderCurrentTerm;
      //state = "follower";
      lastStartTime = std::chrono::system_clock::now();
    }
  }
  if(lastLogIndex >= prevLogIndex)
  {
    if(stateLog[prevLogIndex].term != prevLogTerm)
    {
      Log_info("Server %lu -> Previous log term does not match for append entry from %lu. Sending false",loc_id_,candidateId);
      *followerAppendOK = 0;
      //return;
    }
    else
    {
      if(isHeartbeat == 0)
      {
        //Log_info("Sever %lu -> Received heartbeat",loc_id_);
        stateLog.erase(stateLog.begin()+prevLogIndex+1,stateLog.end());
        stateLog.push_back(LogEntry(cmd,logTerm));
      }
      *followerAppendOK = 1;
    }
   
    //Log_info("Server %lu -> Appended log and updated commit index to %lu",loc_id_,commitIndex);
  }
  else
  {
    //Log_info("Server %lu -> Log size is smaller than sent logs. Sending false",loc_id_);
    *followerAppendOK = 0;
  }

  if(leaderCommitIndex > commitIndex)
      commitIndex = min(leaderCommitIndex,lastLogIndex);
  while((lastApplied+1) <= commitIndex)
  {
    //Log_info("Server %lu -> Found last applied behind, increasing commit index",loc_id_);
    app_next_(*(stateLog[lastApplied+1].cmd));
    lastApplied++;
  }
  //Log_info("Server %lu -> Append entry from %lu processed",loc_id_,candidateId);
}


void RaftServer::HandleRequestVote(
                      const uint64_t& term,
                      const siteid_t& candidateId,
                      const uint64_t& lastLogIndex,
                      const uint64_t& lastLogTerm,
                      uint64_t* returnTerm,
                      bool_t* vote_granted)
{
  
  mtx_.lock();

  *vote_granted = 0;
  *returnTerm = currentTerm;

  //Log_info("Server %lu -> Current term while serving request vote for %lu is %lli",loc_id_,candidateId,currentTerm);
  
  if(term > currentTerm)
  {
    votedFor = 6;
    lastStartTime = std::chrono::system_clock::now();
    currentTerm = term;
    if(state != "follower")
    {
      state = "follower";
      mtx_.unlock();
      return;
    }
    //Log_info("Server %lu -> Current term is less than request vote term, becoming follower",loc_id_);
  }
  if(currentTerm <= term)
  {
    if(votedFor == 6 || votedFor == candidateId)
    {
      if(checkMoreUpdated(lastLogIndex, lastLogTerm))
      {
        //Log_info("Server %lu -> Vote granted to %lli",loc_id_,candidateId);
        *vote_granted = 1;
        votedFor = candidateId;
        lastStartTime = std::chrono::system_clock::now();
      }
      else
      {
        //Log_info("Server %lu -> Logs of %lu not updated enough. Not granting vote",loc_id_,candidateId);
      }
    }
    else
    {
      //Log_info("Server %lu -> Not granting vote to %lu as already voted for %d",loc_id_,candidateId,votedFor);
    }
  }
  else
  {
      //Log_info("Server %lu -> Vote requested term is less than current term, not giving vote",loc_id_);
      *vote_granted = 0;
  }
  mtx_.unlock();
  //Log_info("Server %lu -> Reply to request vote of %lu completed. Sending %d as vote and %lli as return term",loc_id_,candidateId,*vote_granted,*returnTerm);
}


void RaftServer::becomeLeader()
{
  
  mtx_.lock();
  state = "leader";
  chrono::time_point<chrono::system_clock> last_heartbeat_time = chrono::system_clock::now();
  chrono::duration<double,milli> time_since_heartbeat;
  uint64_t tempTerm = currentTerm;
  nextIndex = vector<uint64_t>{1,1,1,1,1};
  matchIndex = vector<pair<uint64_t,uint64_t>>{
                                                {0,currentTerm},
                                                {0,currentTerm},
                                                {0,currentTerm},
                                                {0,currentTerm},
                                                {0,currentTerm}
                                              };
  last_heartbeat_time = chrono::system_clock::now();
  auto proxies = commo()->rpc_par_proxies_[0];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  mtx_.unlock();
  
  while(true)
  {
    last_heartbeat_time = chrono::system_clock::now();
    mtx_.lock();
    uint64_t lastLogIndex = stateLog.size()-1;
    mtx_.unlock();
    auto ev_global = Reactor::CreateSpEvent<IntEvent>();
    Coroutine::CreateRun([=](){
      for(int i=0;i<proxies.size();i++)
      {
        mtx_.lock();
        if(state != "leader")
        {
          mtx_.unlock();
          Log_info("Server %lu -> State changed while sending request votes, breaking out");
          break;
        }
        auto ev_individual = Reactor::CreateSpEvent<IntEvent>();
        mtx_.unlock();
      
          Log_info("Server %lu -> Sending global append entries to %lu",loc_id_,proxies[i].first);
          //Log_info("Server %lu -> Just before entering outer coroutine for sending entries",loc_id_);
        Coroutine::CreateRun([=]
        {
          if(proxies[i].first != loc_id_)
          {
        
            //Log_info("Server %lu -> Entered coroutine block for sending append entries with current nextIndex %d and matchIndex %d",loc_id_,nextIndex[i],matchIndex[i]);
            mtx_.lock();
            uint64_t tempNextIndex = nextIndex[i];
            mtx_.unlock();
            Log_info("Server %lu -> tempNextIndex is %lu and lastLogIndex is %lu",loc_id_,tempNextIndex,lastLogIndex);
            if(tempNextIndex <= lastLogIndex)
            {
              Log_info("Server %lu -> Sending append entry to %lu",loc_id_,proxies[i].first);
              uint64_t iteration = 1;
              while(tempNextIndex <= lastLogIndex)
              {
                Log_info("Server %lu -> Inside while itr %lu for entry %lu to server %lu",loc_id_,iteration,tempNextIndex,proxies[i].first);
                mtx_.lock();
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
                Log_info("Server %lu -> Inside start consensus for while itr %lu before calling sleep for %lu",loc_id_,iteration,proxies[i].first);                        
                Coroutine::Sleep(5000);
                Log_info("Server %lu -> Inside start consensus for while itr %lu after calling sleep before wait for %lu",loc_id_,iteration,proxies[i].first);                        
                mtx_.lock();
                if(event->status_ == Event::INIT)
                {
                  mtx_.unlock();
                  event->Wait(3000);
                }
                mtx_.unlock();
                Log_info("Server %lu -> Inside start consensus for while itr %lu after calling sleep after wait for %lu",loc_id_,iteration,proxies[i].first);
                mtx_.lock();
                //Log_info("Server %lu -> Inside start consensus after calling individual wait",loc_id_);
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
                    mtx_.unlock();
                    //return;
                  }
                  else
                  {
                    if(followerAppendOK == 1)
                    {
                      Log_info("Server %lu -> Append entry for %lu accepted",loc_id_,proxies[i].first);
                      matchIndex[i] = {currentNextIndex,currentTerm};
                      Log_info("Server %lu -> Able to increase match index value",loc_id_);
                      currentNextIndex++;
                      nextIndex[i] = currentNextIndex;
                      Log_info("Sever %lu -> Increased nextIndex value to %lu and match index to %lu",nextIndex[i],matchIndex[i].first);
                    }
                    else if( returnTerm != 0)
                    {
                      Log_info("Server %lu -> Append entry for %lu rejected",loc_id_,proxies[i].first);
                      if(currentNextIndex>1)
                      {
                        Log_info("Server %lu -> First append entry failed, retrying",loc_id_);
                        currentNextIndex--;
                        nextIndex[i] = min(currentNextIndex,followerLogSize);
                      }
                    }
                  }
                }
                tempNextIndex = nextIndex[i];
                if(state != "leader")
                {
                  mtx_.unlock();
                  break;
                }
                iteration++;
                mtx_.unlock();
              }
            }
            else
            {
              //Log_info("Server %lu -> Sending heartbeats to %lu",loc_id_,proxies[i].first);
              mtx_.lock();
              uint64_t returnTerm = 0;
              uint64_t prevLogIndex = nextIndex[i]-1;
              uint64_t prevLogTerm = stateLog[prevLogIndex].term;
              uint64_t currentNextIndex= nextIndex[i];
              Marshallable* m = new CmdData();
              uint64_t followerLogSize = stateLog.size();
              std::shared_ptr<Marshallable> my_shared(m);
              bool_t followerAppendOK = 0;
              uint64_t isHeartbeat = 1;
              mtx_.unlock();
              //Log_info("Server %lu -> Inside start concensus before calling SendAppendEntry as heartbeat",loc_id_);
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
              //Log_info("Server %lu -> After calling append entry as heartbeat before individual wait",loc_id_);
              Coroutine::Sleep(5000);
              mtx_.lock();
              if(event->status_ == Event::INIT)
              {
                mtx_.unlock();
                event->Wait(5000);
              }
              //Log_info("Server %lu -> Got back return term %lu from %lu",loc_id_,returnTerm,proxies[i].first);
              if(event->status_ == Event::TIMEOUT)
              {
                Log_info("Server %lu -> Append entry to %lu timed out",loc_id_,proxies[i].first);
              }
              else
              {
                //Log_info("AppendEntry from %lu to %lu timed out",proxies[i].first,loc_id_);
                //Log_info("Server %lu -> Got response from %lu -> %lu as term, %d as didAppend",loc_id_,proxies[i].first,returnTerm,followerAppendOK);
                mtx_.lock();
                if(returnTerm != 0 && returnTerm > currentTerm)
                {
                  Log_info("Server %lu -> Received greater term from append entry response",loc_id_);
                  state = "follower";
                  currentTerm = returnTerm;
                  mtx_.unlock();
                  //return;
                }
                else
                {
                  if(followerAppendOK)
                  {
                    //Log_info("Server %lu -> Append entry for %lu accepted",loc_id_,proxies[i].first);
                    matchIndex[i] = {lastLogIndex,currentTerm};
                    //Log_info("Server %lu -> Able to increase match index value",loc_id_);
                    //nextIndex[i] = currentNextIndex + 1;
                    //Log_info("Sever %lu -> Increased nextIndex value to %d and match index to %d",nextIndex[i],matchIndex[i]);
                  }
                  else if(returnTerm != 0)
                  {
                    //Log_info("Server %lu -> Append entry for %lu rejected",loc_id_,proxies[i].first);
                    if(currentNextIndex>1)
                    {
                      //Log_info("Server %lu -> First append entry failed, retrying",loc_id_);
                      nextIndex[i] = min(currentNextIndex-1,followerLogSize);
                    }
                  }
                }
                mtx_.unlock();
              }
            }
          }
          Log_info("Server %lu -> Before set in ev individual",loc_id_);
          mtx_.lock();
          if(ev_individual->status_ == Event::INIT)
          {
            mtx_.unlock();
            ev_individual->Set(1);
          }
          mtx_.unlock();
        });
        mtx_.lock();
        if(ev_individual->status_ == Event::INIT)
        {
          mtx_.unlock();
          Log_info("Server %lu -> Before wait on ev individual for %lu",loc_id_,proxies[i].first);
          ev_individual->Wait(40000);
        }
        mtx_.unlock();
        Log_info("Server %lu -> After wait on ev individual for %lu",loc_id_,proxies[i].first);                        
        if(ev_individual->status_ == Event::TIMEOUT)
        {
          Log_info("Server %lu -> Log replication to %lu failed, will try later, moving to next one",loc_id_,proxies[i].first);
        }
        else
        {
          Log_info("Server %lu -> Log replication to %lu succeeded",loc_id_,proxies[i].first);
        }
      }
      if(ev_global->status_ == Event::INIT)
      {
        Log_info("Server %lu -> Before set on ev_global");
        ev_global->Set(1);
      }
    });
    Coroutine::Sleep(10000);
    mtx_.lock();
    if(ev_global->status_ == Event::INIT)
    {
      mtx_.unlock();
      Log_info("Server %lu -> Before wait on ev global",loc_id_);
      ev_global->Wait(150000);
      Log_info("Server %lu -> After ev global",loc_id_);                        
    }
    mtx_.unlock();
    if(ev_global->status_ == Event::TIMEOUT)
    {
      Log_info("Server %lu -> The entire request vote operation could not complete, will try later after timeout",loc_id_);
    }
    else
    {
      Log_info("Server %lu -> The entire request vote operation succeeded",loc_id_);
    }
    mtx_.lock();
    if(state != "leader")
    {
      mtx_.unlock();
      Log_info("Server %lu -> Found state as not leader, stepping down",loc_id_);
      break;
    }
    while(true)
    {
      uint64_t j=commitIndex+1;
      uint64_t total_agreement = 1;
      //Log_info("Server %lu -> Checking majority for commitIndex %lu",loc_id_,j);
      for(int i=0;i<5;i++)
      {
        if(j <= matchIndex[i].first && matchIndex[i].second == currentTerm)
        {
          total_agreement++;
        }
      }

      //Log_info("Server %lu -> Received agreement count %d",loc_id_,total_agreement);
      if(total_agreement >= 3)
      {
        //Log_info("Server %lu -> Received majority agreement for commitIndex %lu",loc_id_,j);
        commitIndex=j;
      }
      else
      {
        //Log_info("Server %lu -> Could not get majority for commitIndex %lu",loc_id_,j);
        break;
      }
    }
    while((lastApplied+1)<=commitIndex)
    {
        //Log_info("Server %lu -> Found last applied less than commit index, passing to app next",loc_id_);
        app_next_(*(stateLog[lastApplied+1].cmd));
        lastApplied++;
    }
    time_since_heartbeat = chrono::system_clock::now() - last_heartbeat_time;
    uint64_t sleep_time = 140-time_since_heartbeat.count();
    if(sleep_time>0 && sleep_time<150)
    {
      //Log_info("Server %lu -> Sleeping for %d",loc_id_,sleep_time);
      mtx_.unlock();
      Coroutine::Sleep(sleep_time*1000);
      //Log_info("Server %lu -> Inside leader after consensus after coroutine sleep 2",loc_id_);
    }
    mtx_.unlock();
    mtx_.lock();
    if(state!="leader")
    {
      mtx_.unlock();
      break;
    }
    mtx_.unlock();
  }
}

void RaftServer::Setup() {
  
  
  //Log_info("Server %lu -> Calling setup",loc_id_);
  Coroutine::CreateRun([this](){
    while(true)
    {
      //Log_info("Server %lu -> Inside setup starting",loc_id_);
      mtx_.lock();
      if(state == "follower")
      {
        uint64_t temp_term = currentTerm;
        mtx_.unlock();
        //Log_info("Server %lu ->Calling convert to follower",loc_id_);
        convertToFollower(temp_term);
        //Log_info("Server %lu ->After convert to follower",loc_id_);
      }
      if(state == "candidate")
      {
        mtx_.unlock();
        //Log_info("Server %lu ->Calling become candidate",loc_id_);
        becomeCandidate();
        //Log_info("Server %lu ->After become candidate",loc_id_);
      }
      if(state == "leader")
      {
        mtx_.unlock();
        //Log_info("Server %lu ->Calling become leader",loc_id_);
        becomeLeader();
        //Log_info("Server %lu ->After leader",loc_id_);
      }
      mtx_.unlock();
      //Log_info("Server %lu -> Inside setup before couroutine sleep",loc_id_);
      //Coroutine::Sleep(5000);
      //Log_info("Server %lu -> Inside setup after couroutine sleep",loc_id_);
    }

  });

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
    uint64_t getLastLogIndex = (stateLog.size()-1);
    *index = getLastLogIndex;
    //Log_info("Server %lu -> Appended Log. Starting consensus from leader",loc_id_);
    //Log_info("Server %lu -> End of start. Returning true");
    return true;
  }
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  
  //Log_info("Server %lu -> Inside get state",loc_id_);
  
  mtx_.lock();

  if(state == "leader")
    *is_leader = true;
  else
    *is_leader = false;
  *term = currentTerm;

  mtx_.unlock();

  //Log_info("Server %lu -> replying with values as %d and term %lu ",loc_id_,*is_leader,*term);
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
  return disconnected_;
}

} // namespace janus
