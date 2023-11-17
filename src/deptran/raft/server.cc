#include "server.h"
// #include "paxos_worker.h"
//#define Log_info Log_debug
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
  Log_info("Server %lu -> Inside constructor call", site_id_);
  frame_ = frame ;
  persister = persister_;
  mtx_.lock();
  stateLog = vector<LogEntry>();
  if(persister->RaftStateSize() != -1)
  {
    /*
      In case there is a previous persist, reading old state
    */
    ReadPersist();
  }
  else
  {
    Log_info("Server %lu -> Initialising for the first time",site_id_);
    currentTerm = 1;
    votedFor = 100;
    auto cmdptr = std::make_shared<TpcCommitCommand>();
    auto vpd_p = std::make_shared<VecPieceData>();
    vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
    cmdptr->tx_id_ = 0;
    cmdptr->cmd_ = vpd_p;
    auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
    stateLog.push_back(LogEntry(cmdptr_m,0));  
    commitIndex = 0;
    lastApplied = 0;
  }
  aboutToDie = 0;
  state = "follower";
  lastStartTime = chrono::system_clock::now();
  nextIndex = vector<uint64_t>{1,1,1,1,1};
  matchIndex = vector<uint64_t>(5,0);
  mtx_.unlock();
  Log_info("Server initialization completed for %lli",site_id_);
}

RaftServer::~RaftServer() {
  Log_info("Server %lu -> Inside destructor call",site_id_);
}

int RaftServer::generateElectionTimeout(){
    srand(site_id_);
    return 800+(rand()%400);
}

void RaftServer::convertToFollower(const uint64_t& term){

  /*
    This function is the starting call to a follower.
    It executes a timeout to stay follower
  */
  Log_info("Server %lu -> Starting as follower",site_id_);

  mtx_.lock();
  
  votedFor = 100;
  lastStartTime = std::chrono::system_clock::now();
  currentTerm = term;
  state = "follower";
  mtx_.unlock();

  runFollowerTimeout();

}

void RaftServer::runFollowerTimeout(){

  /*
    This function is the timeout execution for a follower.
    It stays as a follower till the time the timeout is
    not run out or a shutdown call is not in execution
  */

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
      //In case state changed from follower, break from the timer loop
      break;
    }
    time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
  }

  mtx_.lock();
  //In case state was follower and timer has run out, become a candidate
  if(state == "follower")
    state = "candidate";

  mtx_.unlock();
}

void RaftServer::becomeCandidate()
{
   /*
    This function is the entry to candidate logic.
    Become candidate and ask for votes and depending
    on votes either contest election again or start
    as a leader
   */ 
  int electionTimeout = generateElectionTimeout();
  
  chrono::milliseconds endTimeout(electionTimeout);
  Log_info("Server %lu-> Candidate timeout is %i ",site_id_,electionTimeout);
  
  mtx_.lock();
  state = "candidate";
  currentTerm++;
  lastStartTime = chrono::system_clock::now();
  votedFor = site_id_;
  chrono::duration<double,milli> time_spent = chrono::system_clock::now() - lastStartTime;
  //Get proxies for all servers in the same partition
  auto proxies = commo()->rpc_par_proxies_[partition_id_];
  mtx_.unlock();

  mtx_.lock();
  //Set initial variables states for counting
  uint64_t total_votes_granted = 1;
  uint64_t max_return_term = 0;
  uint64_t tempCurrentTerm = currentTerm;
  uint64_t tempLastLogIndex = stateLog.size()-1;
  uint64_t tempLastLogTerm = stateLog[tempLastLogIndex].term;
  mtx_.unlock();
  Log_info("Server %lu -> Contesting election in term %lu",site_id_,currentTerm);
  for(int i=0;i<proxies.size();i++)
  {
    //Skip calls to same server as vote is already counted
    if(proxies[i].first != site_id_)
    {
      uint64_t return_term = 0;
      bool_t vote_granted = 0;
      Log_info("Server %lu -> Sending request vote to %lu with lastLogIndex as %lu and lastLogTerm as %lu",site_id_,proxies[i].first, tempLastLogIndex, tempLastLogTerm);
      auto event = commo()->SendRequestVote(
                            partition_id_,    //sending my partition
                            proxies[i].first, //sending my site_id
                            tempCurrentTerm,  //sending my term
                            tempLastLogIndex, //sending my last log index
                            tempLastLogTerm,  //sending my last log term
                            &return_term,
                            &vote_granted
                          );
      //Waiting for 10ms
      event->Wait(10000);
      mtx_.lock();
      if(event->status_ == Event::TIMEOUT)
      {
        //In case of timeout do nothing
        Log_info("Server %lu -> Timeout happened for send request votes to %lu",site_id_,proxies[i].first);
      }
      else
      {
        //In case of a reply
        Log_info("Server %lu -> Got reply from server %lu with vote count %d and term %lu",site_id_,proxies[i].first,vote_granted,return_term);
        //Check if reply is not default return term
        if(return_term > 0)
        { 
          //Check max return term across all replies
          max_return_term = max(return_term,max_return_term);
          if(max_return_term > currentTerm)
          {
            mtx_.unlock();
            //Break if you see a higher term
            break;
          }
          if(vote_granted == 1)
          {
            //Increase vote count in case you receive a vote
            total_votes_granted++;
          }
          if(total_votes_granted >= 3)
          {
            mtx_.unlock();
            //In case of majority, break from loop
            break;
          }
        }
      }
      mtx_.unlock();
    }
  }
  mtx_.lock();
  //Check if state is still candidate after processing replies
  if(state == "candidate")
  {
    //Check if you have seen a bigger term
    if(max_return_term > currentTerm)
    {
      //Become follower and update term in this case
      Log_info("Server %lu -> Received bigger term after requestVote. Current term is %lu and got %lu",site_id_,currentTerm,max_return_term);
      state = "follower";
      currentTerm = max_return_term;
    }
    else if(total_votes_granted >= 3)
    {
      //In case of winning, update state as leader
      Log_info("Server %lu -> Election supremacy. Won election in the term %lu",site_id_,currentTerm);
      state = "leader";
    }
  }
  
  if(state != "candidate") 
  {
    //In case contested election and changed state
    mtx_.unlock();
  }
  else
  {
    //In case contested election and state did not change
    //Contest election again after timeout
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
  //This checks who has more updated logs based on log checking criteria
  uint64_t tempLastLogIndex = stateLog.size()-1;
  pair<uint64_t,uint64_t> my_pair = {tempLastLogIndex,stateLog[tempLastLogIndex].term};
  if(lastLogTerm < my_pair.second)
  {
    //In case requesting server has not seen latest term
    Log_info("Server %lu -> Found last log term smaller. Have %lu, checked %lu",site_id_,my_pair.second,lastLogTerm);
    return false;
  }
  else if(lastLogTerm > my_pair.second)
  {
    //In case requesting server has seen lastest term
    Log_info("Server %lu -> Found last term greater. Giving vote", site_id_);
    return true;
  }
  else
  {
    if(lastLogIndex>= my_pair.first)
    {
      //Requesting server logs size updated
      Log_info("Server %lu -> Have %lu, Found %lu. Giving vote",site_id_,my_pair.first,lastLogIndex);
      return true;
    }
    else
    {
      //Requesting server has smaller logs
      Log_info("Server %lu -> Have %lu, Found %lu. Not Giving vote",site_id_,my_pair.first,lastLogIndex);
      return false;
    }
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
  /*
    This function deals with all incoming appendEntries + heartbeats
  */
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *followerLogSize = stateLog.size();
  *returnTerm = currentTerm;
  *followerAppendOK = 0;
  //In case incoming term is smaller, don't process
  if(leaderCurrentTerm < currentTerm)
  {
    *followerAppendOK = 0;
  }
  else if(currentTerm <= leaderCurrentTerm)
  {
    //Become follower in case not a follower
    if(state != "follower")
    {
      votedFor = 100;
      lastStartTime = std::chrono::system_clock::now();
      currentTerm = leaderCurrentTerm;
      state = "follower";
    }
    {
      currentTerm = leaderCurrentTerm;
      lastStartTime = std::chrono::system_clock::now();
      uint64_t currentMaxLogIndex = stateLog.size()-1;
      //Check if have enough logs and incase have, match previous entry term
      if(currentMaxLogIndex < prevLogIndex || stateLog[prevLogIndex].term != prevLogTerm)
      {
        Log_info("Server %lu -> Previous log term does not match for append entry from %lu. Sending false",site_id_,candidateId);
        *followerAppendOK = 0;
      }
      else
      {
        //In case not a heartbeat
        if(isHeartbeat == 0)
        {
          //Update logs
          if(currentMaxLogIndex >= prevLogIndex+1)
          {
            //In case have extra logs that do not match, delete and insert
            if(stateLog[prevLogIndex+1].term != logTerm)
            {
              stateLog.erase(stateLog.begin()+prevLogIndex+1,stateLog.end());
              std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
              stateLog.push_back(LogEntry(cmd,logTerm));
            }
          }
          else
          {
            //If no mismatch, insert directly
            std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
            stateLog.push_back(LogEntry(cmd,logTerm));
          }
        }
        *followerAppendOK = 1;
        //Update commit index and commit in case commit index changed
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
  /*
    This function deals with all incoming request votes
  */
  Log_info("Server %lu -> Got request vote call for %lu. With lastlogIndex as %lu and lastLogTerm as %lu",site_id_,candidateId,lastLogIndex,lastLogTerm);
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  //Set deafult values to vote and term to return
  *vote_granted = 0;
  *returnTerm = currentTerm;
  Log_info("Server %lu -> Received request vote call",loc_id_);
  //If received term is higher, then only serve vote
  if(term > currentTerm)
  {
    votedFor = 100;
    currentTerm = term;
    if(state != "follower")
    {
      //If state is not follower, become follower
      Log_info("Server %lu -> Found state as follower. Converting to follower",site_id_);
      state = "follower";
    }
    //If have not voted for anyone else
    if(votedFor == 100 || votedFor == candidateId)
    {
      //Check if logs are updated
      if(checkMoreUpdated(lastLogIndex, lastLogTerm))
      {
        Log_info("Server %lu -> Found logs updated, Giving vote",site_id_);
        *vote_granted = 1;
        votedFor = candidateId;
        lastStartTime = std::chrono::system_clock::now();
      }
      else
      {
        //In case vote not given
        Log_info("Server %lu -> Found logs as not updated, Not giving vote",site_id_);
      }
    }
  }
  else
    Log_info("Server %lu -> Found smaller term. Not giving vote",site_id_);
}

void RaftServer::canBeCommitted(uint64_t toBeCommitted,uint64_t totalAgreement)
{
  /*
    This function checks if the value can be committed through log matching
    property
  */
  if(toBeCommitted>=300)
  {
    if(totalAgreement >= 3 && stateLog[toBeCommitted].term == currentTerm)
    {
      Log_info("Server %lu -> Found majority for commit index %lu",site_id_,toBeCommitted);
      commitIndex=toBeCommitted;
    }
  }
  else
  {
    if(totalAgreement >= 3)
    {
      Log_info("Server %lu -> Found majority for commit index %lu",site_id_,toBeCommitted);
      commitIndex=toBeCommitted;
    }
  }
}

void RaftServer::becomeLeader()
{
  /*
    This code handles the leader logic part.
    Sends append entries or heartbeats depending on
    how many logs have been replicated
  */
  Log_info("Server %lu -> Starting as leader",site_id_);
  mtx_.lock();
  //Resetting variables to default values
  state = "leader";
  chrono::time_point<chrono::system_clock> last_heartbeat_time;
  chrono::duration<double,milli> time_since_heartbeat;
  uint64_t tempLogIndex = stateLog.size();
  nextIndex = vector<uint64_t>(5,tempLogIndex);
  matchIndex = vector<uint64_t>(5,0);
  matchIndex[loc_id_] = stateLog.size()-1;
  auto proxies = commo()->rpc_par_proxies_[partition_id_];
  mtx_.unlock();
  
  while(true && aboutToDie == 0)
  {
    last_heartbeat_time = chrono::system_clock::now();
    for(int i=0;i<proxies.size();i++)
    {
      mtx_.lock();
      //This condition checks if while sending heartbeat, someone else changed state from leader
      if(state != "leader")
      {
        Log_info("Server %lu -> State changed while sending heartbeats, breaking out",site_id_);
        mtx_.unlock();
        break;
      }
      mtx_.unlock();
      Log_info("Server %lu -> Sending global append entries to %lu",site_id_,proxies[i].first);
      //Skipping calls to yourself
      if(proxies[i].first != site_id_)
      {
        mtx_.lock();
        Log_info("Server %lu -> tempNextIndex is %lu and lastLogIndex is %lu",site_id_,nextIndex[i],stateLog.size()-1);
        //In case nextIndex is less than current log size, send append entry
        if(nextIndex[i] <= (stateLog.size()-1))
        {
          Log_info("Server %lu -> Sending append entry to %lu",site_id_,proxies[i].first);
          uint64_t returnTerm = 0;
          uint64_t prevLogIndex = nextIndex[i]-1;
          uint64_t prevLogTerm = stateLog[prevLogIndex].term;
          uint64_t currentNextIndex= nextIndex[i];
          uint64_t currentLogTerm = stateLog[currentNextIndex].term;
          uint64_t followerLogSize = stateLog.size();
          std::shared_ptr<Marshallable> my_shared = stateLog[currentNextIndex].cmd;
          bool_t followerAppendOK = 0;
          uint64_t isHeartbeat = 0;
          Log_info("Server %lu -> Inside start concensus before calling SendAppendEntry for %lu",site_id_,proxies[i].first);
          mtx_.unlock();
          auto event = commo()->SendAppendEntriesCombined(
                                      partition_id_,    //partition id
                                      proxies[i].first, //candidate site id
                                      prevLogIndex,     //previous log index
                                      prevLogTerm,      //previous log term
                                      currentLogTerm,   //to be replicated log term
                                      currentTerm,      //current term
                                      commitIndex,      //leader commit index
                                      isHeartbeat,      //whether call is heartbeat
                                      my_shared,        //log to be replicated
                                      &followerLogSize, //this variable brings follower log size
                                      &returnTerm,      //term sent by follower
                                      &followerAppendOK //did append or not
                                    );
          //Wait for 20ms
          event->Wait(20000);
          
          mtx_.lock();
          Log_info("Server %lu -> Got back return term %lu from %lu",site_id_,returnTerm,proxies[i].first);
          if(event->status_ == Event::TIMEOUT)
          {
            //Event timedout without response
            Log_info("Server %lu -> Append entry to %lu timed out",site_id_,proxies[i].first);
          }
          else
          {
            Log_info("Server %lu -> Got response from %lu -> %lu as term, %d as didAppend",site_id_,proxies[i].first,returnTerm,followerAppendOK);
            //In case got a higher term become follower
            if(returnTerm > currentTerm)
            {
              Log_info("Server %lu -> Received greater term from append entry response",site_id_);
              state = "follower";
              lastStartTime = std::chrono::system_clock::now();
              votedFor = 100;
              currentTerm = returnTerm;
            }
            else
            {
              if(followerAppendOK == 1)
              {
                //If the follower appended logs, updated nextIndex and matchIndex
                Log_info("Server %lu -> Append entry for %lu accepted",site_id_,proxies[i].first);
                matchIndex[i] = nextIndex[i];
                nextIndex[i]++;
                Log_info("Server %lu -> Increased nextIndex value to %lu and match index to %lu",site_id_,nextIndex[i],matchIndex[i]);
              }
              else if( returnTerm != 0 )
              {
                Log_info("Server %lu -> Append entry for %lu rejected",site_id_,proxies[i].first);
                //In case did not append, reduce nextIndex but not beyond minimum value
                if(nextIndex[i]>1)
                {
                  Log_info("Server %lu -> First append entry failed, retrying",site_id_);
                  //This condition optimises calls in case follower log size is too small
                  nextIndex[i] = min(nextIndex[i]-1,followerLogSize);
                }
              }
            }
          }
          mtx_.unlock();
        }
        else
        {
          //Send heartbeat is size is upto current size
          Log_info("Server %lu -> Sending heartbeats to %lu",site_id_,proxies[i].first);
          uint64_t returnTerm = 0;
          uint64_t prevLogIndex = nextIndex[i]-1;
          uint64_t prevLogTerm = stateLog[prevLogIndex].term;
          uint64_t currentNextIndex= nextIndex[i];
          Marshallable* m = new CmdData();
          uint64_t followerLogSize = stateLog.size();
          std::shared_ptr<Marshallable> my_shared(m);
          bool_t followerAppendOK = 0;
          uint64_t isHeartbeat = 1;
          Log_info("Server %lu -> Inside start concensus before calling SendAppendEntry as heartbeat",site_id_);
          mtx_.unlock();
          auto event = commo()->SendAppendEntriesCombined(
                                      partition_id_,
                                      proxies[i].first,
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
            //In case heartbeat timed out
            Log_info("Server %lu -> Append entry to %lu timed out",site_id_,proxies[i].first);
          }
          else
          {
            Log_info("Server %lu -> Got response from %lu -> %lu as term, %d as didAppend",site_id_,proxies[i].first,returnTerm,followerAppendOK);
            if(returnTerm > currentTerm)
            {
              Log_info("Server %lu -> Received greater term from append entry response",site_id_);
              state = "follower";
              lastStartTime = std::chrono::system_clock::now();
              votedFor = 100;
              currentTerm = returnTerm;
            }
            else
            {
              if(followerAppendOK)
              {
                matchIndex[i] = stateLog.size()-1;
                Log_info("Server %lu -> Increased nextIndex value to %lu and match index to %lu",site_id_,nextIndex[i],matchIndex[i]);
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
      //Check if is there any command to be committed
      uint64_t toBeCommitted=commitIndex+1;
      while(true)
      {
        uint64_t totalAgreement = 0;
        Log_info("Server %lu -> Checking majority for commitIndex %lu",site_id_,toBeCommitted);
        for(int i=0;i<5;i++)
        {
          //Find if replicated on machine
          if(toBeCommitted <= matchIndex[i])
          {
            Log_info("Server %lu -> Found matchIndex %lu for server %d",site_id_, matchIndex[i],i);
            //Increase total agreement if found match
            totalAgreement++;
          }
        }
        //Check if this value can be committed 
        canBeCommitted(toBeCommitted,totalAgreement);
        toBeCommitted++;
        //Break in case log size checked
        if(toBeCommitted>(stateLog.size()-1))
          break;
      }
      //In case commitIndex has been updated, pass committed values to app_next
      while((lastApplied+1)<=commitIndex)
      {
          Log_info("Server %lu -> Sending entry at %lu to app next",site_id_,lastApplied+1);
          app_next_(*(stateLog[lastApplied+1].cmd));
          lastApplied++;
      }
      mtx_.unlock();
    }
    
    mtx_.lock();
    //Break in case state has been changed
    if(state != "leader")
    {
      mtx_.unlock();
      Log_info("Server %lu -> Found state as not leader, stepping down",site_id_);
      break;
    }
    mtx_.unlock();
    //Check time since last heartbeat and sleep for remaining time
    time_since_heartbeat = chrono::system_clock::now() - last_heartbeat_time;
    uint64_t sleep_time = 100-time_since_heartbeat.count();
    //If sleep between valid range, go to sleep
    if(sleep_time>0 && sleep_time<100)
    {
      Log_info("Server %lu -> Sleeping for %d",site_id_,sleep_time);
      mtx_.unlock();
      Coroutine::Sleep(sleep_time*1000);
      Log_info("Server %lu -> Inside leader after consensus after coroutine sleep 2",site_id_);
    }

    mtx_.lock();
    //Wake up and check if state changed
    if(state!="leader")
    {
      mtx_.unlock();
      break;
    }
    mtx_.unlock();
    Log_info("Server %lu -> Got about to die as %d inside leader",site_id_,aboutToDie);
  }
}

void RaftServer::Setup() {
  /*
    This function is the first function called after
    initialisation. This serves as a scope holder for any
    running state for the server. At any point of time
    a server can be leader, candidate or a follower.
    Depending on that, the function executes the required function.
  */
  Coroutine::CreateRun([this](){
    while(true && aboutToDie == 0)
    {
      mtx_.lock();
      if(state == "follower" && aboutToDie == 0)
      {
        //In case a follower and there is no shutdown executed
        uint64_t temp_term = currentTerm;
        mtx_.unlock();
        convertToFollower(temp_term);
      }
      if(state == "candidate" && aboutToDie == 0)
      {
        mtx_.unlock();
        //In case a candidate and there is no shutdown executed
        becomeCandidate();
      }
      if(state == "leader" && aboutToDie == 0)
      {
        mtx_.unlock();
        //In case a leader and there is no shutdown executed
        becomeLeader();
      }
      mtx_.unlock();
      Log_info("Server %lu -> Got about to die inside setup as %lu",site_id_,aboutToDie);
    }
  });

}



void RaftServer::Shutdown() {
  /*
    This function serves as an interface to shutdown the server.
    Any calls to this function will first stpo any running process
    and gracefully persists and shut down
  */
  Log_info("Server %lu -> Received shutdown call",site_id_);
  mtx_.lock();
  aboutToDie = 1;
  mtx_.unlock();
  Coroutine::Sleep(1000000);
  mtx_.lock();
  Persist();
  mtx_.unlock();
}

void RaftServer::Persist() {
  /*
    This function saves all state before exiting
  */
  Log_info("Server %lu -> Received persist call",site_id_);
  auto myCurrentState = make_shared<StateMarshallable>();
  myCurrentState->persistedTerm = currentTerm;
  Log_info("Server %lu -> Stored current term",site_id_);
  myCurrentState->persistedVotedFor = votedFor;
  Log_info("Server %lu -> Stored voted for",loc_id_);
  myCurrentState->persistedCommitIndex = commitIndex;
  Log_info("Server %lu -> Stored commit index",site_id_);
  myCurrentState->persistedLastApplied = lastApplied;
  Log_info("Server %lu -> Stored last applied",site_id_);
  myCurrentState->persistedLogs = stateLog;
  Log_info("Server %lu -> Stored values till 4",site_id_);
  auto myMarshallable = dynamic_pointer_cast<Marshallable>(myCurrentState);
  persister->SaveRaftState(myMarshallable);
  Log_info("Server %lu -> Persist ends",site_id_);
}

void RaftServer::ReadPersist() {
  /*
    This function, creates a StateMarshallable object to store state
    and then copies term, voted for, commit index, last applied and logs to
    the marshallable object
  */
  mtx_.lock();
  Log_info("Server %lu -> Found existing persisted state",site_id_);
  auto tempState = persister->ReadRaftState();
  auto myStoredState =  (StateMarshallable*)(&(*tempState));
  Log_info("Server %lu -> State retrieved",site_id_);
  currentTerm = myStoredState -> persistedTerm;
  Log_info("Server %lu -> Got old term as %lu",site_id_,currentTerm);
  votedFor = myStoredState -> persistedVotedFor;
  Log_info("Server %lu -> Got old votedFor as %lu",site_id_,votedFor);
  commitIndex = myStoredState -> persistedCommitIndex;
  Log_info("Server %lu -> Got commit index as %lu",site_id_,commitIndex);
  lastApplied = myStoredState -> persistedLastApplied;
  Log_info("Server %lu -> Got last applied as %lu",site_id_,lastApplied);
  stateLog = myStoredState -> persistedLogs;
  Log_info("Server %lu -> Got back state logs",site_id_);
  mtx_.unlock();
}

bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term)
{
  /*
    This function serves as an interface to the raft server.
    It appends new entries and returns the term in which it was appended.
  */
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *term = currentTerm;
  if(state != "leader")
  {
    //Log_info("Server %lu -> Not the leader, returning false from start",loc_id_);
    return false;
  }
  else
  {
    Log_info("Server %lu -> Beginning start logic",site_id_);
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

  Log_info("Server %lu -> replying with values as %d and term %lu ",site_id_,*is_leader,*term);
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
  Log_info("Server %lu -> Inside connect/disconnect",site_id_);
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
  Log_info("Server %lu -> Checking is disconnected. Found %d",site_id_, disconnected_);
  return disconnected_;
}

} // namespace janus
