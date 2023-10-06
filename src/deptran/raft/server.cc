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
  //app_next_(*my_shared);
  commitIndex = 0;
  lastApplied = 0;
  //lastApplied = 0;
  nextIndex = vector<int>{1,1,1,1,1};
  matchIndex = vector<int>{0,0,0,0,0};

  mtx_.unlock();
  Log_info("Server initialization completed for %lli",loc_id_);
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

}

RaftServer::~RaftServer() {
  /* Your code here for server teardown */

}

int RaftServer::generateElectionTimeout(){
    // Selecting timeout bw 500ms to 1s as recommended by
    // professor
    srand(loc_id_);
    return 300+(rand()%500);
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
    state = "follower";
    mtx_.unlock();
    return;
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
      commitIndex = min(leaderCommitIndex,stateLog.size()-1);
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
  //Log_info("Ran the follower timeout on a coroutine for %lli",loc_id_);
}

void RaftServer::runFollowerTimeout(){

  int electionTimeout = generateElectionTimeout();

  Log_info("Timeout for server %lli is %i",loc_id_,electionTimeout);
  chrono::milliseconds endTimeout(electionTimeout);

  mtx_.lock();
  chrono::duration<double,milli> time_spent = chrono::system_clock::now() - lastStartTime;
  mtx_.unlock();
  
  while(time_spent < endTimeout)
  {
      
    Log_info("Server %lu -> Inside runFollowerTimeout before coroutine sleep",loc_id_);
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

    //Log_info("Server %lu -> Inside runFollowerTimeout after coroutine sleep",loc_id_);
  //}
  mtx_.lock();
  //time_spent = chrono::system_clock::now() - lastStartTime;
  //if(time_spent.count() > electionTimeout)
  
  Log_info("Server %lu ->Timeout completed as follower. Switching to candidate",loc_id_);
  state = "candidate";
  //}
  mtx_.unlock();
  //Log_info("Server %lu ->Timeout completed as follower. Switching to candidate",loc_id_);
  
  /* 
    Election timeout finished as a follower
    Changing to candidate
  */
  //becomeCandidate();
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
  //Log_info("Server %lli  started as candidate with term %lli",loc_id_,currentTerm);
  lastStartTime = chrono::system_clock::now();
  votedFor = loc_id_;
  chrono::duration<double,milli> time_spent = 
                  chrono::system_clock::now() - lastStartTime;
  mtx_.unlock();

  //while(time_spent < endTimeout)
  //{
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    Log_info("Server %lu -> Inside become candidate, going for request vote",loc_id_);
    Coroutine::CreateRun([=](){

      uint64_t max_return_term=0;
      uint64_t total_votes_received=1;

      mtx_.lock();
      uint64_t tempCurrentTerm = currentTerm;
      uint64_t tempLastLogIndex = stateLog.size()-1;
      uint64_t tempLastLogTerm = stateLog[tempLastLogIndex].term;
      mtx_.unlock();

      Log_info("Server %lu -> Inside become candidate, before sending request vote",loc_id_);
      auto event = commo()->SendRequestVote(
                              0,
                              tempCurrentTerm, // sending my term
                              loc_id_,  //sending my id
                              tempLastLogIndex, //my last log index
                              tempLastLogTerm, // my last log term
                              &max_return_term,
                              &total_votes_received
                            );
      Log_info("Server %lu -> Inside become candidate, after send request vote before wait",loc_id_);                      
      event->Wait(15000);
      Log_info("Server %lu -> Inside become candidate, after send request vote after wait",loc_id_);                      
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
          Log_info("Server %lu -> Changed state to %s while waiting for votes",loc_id_,state.c_str());
          if(state == "Leader")
          {
            matchIndex = vector<int>(5,0);
            nextIndex = vector<int>(5,stateLog.size());
          }
          mtx_.unlock();
          return;
        }
        if(max_return_term != 0)
        { 
        
          /*
            Checking for the return term
            and breaking in case it is more than current
            term
          */
          Log_info("Server %lu -> Max return term is not zero",loc_id_);
          if(max_return_term > currentTerm)
          {
            Log_info("Server %lu -> Received bigger term after requestVote. Current term is %lu and got %lu",loc_id_,currentTerm,max_return_term);
            
            state = "follower";
            currentTerm = max_return_term;
            mtx_.unlock();
            return;               
          }
          if(max_return_term <= currentTerm)
          {
            /*
            Checking for majority of votes received
            and changing to a leader in case that happens
            */
            if(total_votes_received >= (commo()->rpc_par_proxies_[0].size()+1)/2)
            {
              Log_info("Server %lu -> Election supremacy. Won election",loc_id_);
              state = "leader";
              mtx_.unlock();
              //return;
            }
            else
            {
              Log_info("Server %lu -> Did not received majority votes",loc_id_);
            }
          }
        }
        mtx_.unlock();
        
      }
      {
        std::lock_guard<std::recursive_mutex> guard(mtx_);
        //ev->Set(0);
        Log_info("Server %lu -> Calling set on outer ev inside become candidate smart pointer global %p",loc_id_,ev.get());
        ev->Set(1);
      }
    });

    //Log_info("Server %lu -> Inside becomeCandidate before coroutine sleep",loc_id_);
    Log_info("Server %lu -> Calling wait on outer ev inside become candidate smart pointer global %p",loc_id_,ev.get());
    ev->Wait(30000);
    Log_info("Server %lu -> Inside becomeCandidate after coroutine sleep",loc_id_);
    
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
      //if((endTimeout-time_spent).count() > 20)
      //{
        //Log_info("Server %lu -> Will send votes after 20 miliseconds");
        //Coroutine::Sleep(50000);
      //}
      //else
      //{
      Log_info("Server %lu -> Will send votes after %f",loc_id_,(endTimeout-time_spent).count());
      Coroutine::Sleep((endTimeout-time_spent).count()*1000);
      //}
    }
    /*
      Updating time diff
    */
    
  //}
  //Log_info("Time elapsed since one election without result for %lli",loc_id_);
  //becomeCandidate();
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
  // if(prevLogIndex==0)
  // {
  //   Log_info("Server %lu -> Received entries from starting from %lu",loc_id_,candidateId);
  //   stateLog = vector<LogEntry>();
  //   stateLog.push_back(LogEntry(cmd,term));
  //   uint64_t temp_size = (int)stateLog.size();
  //   *followerAppendOK = true;
  //   if(leaderCommitIndex > commitIndex)
  //   {
  //     // Log_info("LeaderCommit index is greater,current commit index %lu",commitIndex);
  //     // if(temp_size < leaderCommitIndex)
  //     //   commitIndex = temp_size;
  //     // else
  //     //   commitIndex = leaderCommitIndex;
  //     commitIndex = min(leaderCommitIndex, stateLog.size());
  //   }
  //   Log_info("Server %lu -> Commit index value is %lld",commitIndex);
  // }
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
  pair<uint64_t,uint64_t> my_pair = {stateLog.size()-1,stateLog[stateLog.size()-1].term};
  if(lastLogTerm < my_pair.second)
    return false;
  else if(lastLogTerm > my_pair.second)
    return true;
  else
    return lastLogIndex >= my_pair.first;
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

  *vote_granted = false;
  *returnTerm = currentTerm;

  Log_info("Server %lu -> Current term while serving request vote for %lu is %lli",loc_id_,candidateId,currentTerm);
  
  if(term > currentTerm)
  {
    votedFor = 6;
    lastStartTime = std::chrono::system_clock::now();
    currentTerm = term;
    state = "follower";
    Log_info("Server %lu -> Current term is less than request vote term, becoming follower",loc_id_);
  }

  if(currentTerm <= term)
  {
    if(votedFor == 6 || votedFor == candidateId)
    {
      if(checkMoreUpdated(lastLogIndex, lastLogTerm))
      {
        Log_info("Server %lu -> Vote granted to %lli",loc_id_,candidateId);
        *vote_granted = true;
        votedFor = candidateId;
        lastStartTime = std::chrono::system_clock::now();
      }
      else
      {
        Log_info("Server %lu -> Logs of %lu not updated enough. Not granting vote",loc_id_,candidateId);
      }
    }
    else
    {
      Log_info("Server %lu -> Not granting vote to %lu as already voted for %d",loc_id_,candidateId,votedFor);
    }
  }
  else
  {
      Log_info("Server %lu -> Vote requested term is less than current term, not giving vote",loc_id_);
      *vote_granted = false;
  }
  mtx_.unlock();
  Log_info("Server %lu -> Reply to request vote of %lu completed. Sending %d as vote and %lli as return term",loc_id_,candidateId,*vote_granted,*returnTerm);
}


void RaftServer::becomeLeader()
{
  mtx_.lock();
  state = "leader";
  mtx_.unlock();

  int c = 0;
  chrono::time_point<chrono::system_clock> last_heartbeat_time = chrono::system_clock::now();
  chrono::duration<double,milli> time_since_heartbeat;
  
  //while(true)
  //{
    //uint64_t returned_max_term = 0;
    //bool check = false;
    mtx_.lock();
    uint64_t temp_term = currentTerm;
    mtx_.unlock();

    //if(c==0 || time_since_heartbeat.count() > 100)
    //{
      
      last_heartbeat_time = chrono::system_clock::now();
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      Coroutine::CreateRun([=](){

        uint64_t returned_max_term = 0;
        Log_info("Server %lu -> Inside leader, inside coroutine before send empty append entry",loc_id_);
        auto event = commo()->SendEmptyAppendEntries(
                                    0,
                                    temp_term,
                                    loc_id_,
                                    commitIndex,
                                    &returned_max_term
                                    );
        Log_info("Server %lu -> Inside leader, inside coroutine, after send empty append entry, before wait",loc_id_);
        event->Wait(15000);
        Log_info("Server %lu -> Inside leader, inside coroutine, after send empty append entry, after wait",loc_id_);
        if(event->status_ == Event::TIMEOUT)
        {
          Log_info("Server %lu -> Timeout happened for all heartbeats",loc_id_);
        }
        else
        {
          Log_info("Server %lu -> Response for all heartbeats received with max return term %lu",loc_id_,returned_max_term);
          if(returned_max_term > currentTerm)
          {
            Log_info("Server %lu -> After sending heartbeat, got a bigger term. Stepping down as leader",loc_id_);
            mtx_.lock();
            state = "follower";
            currentTerm = returned_max_term;
            mtx_.unlock();
          }
        }
        {
          std::lock_guard<std::recursive_mutex> guard(mtx_);
          Log_info("Server %lu -> Calling set on ev inside empty append entry %p",loc_id_,ev.get());
          //ev->Set(0);
          ev->Set(1);
        }
      });
      //c=1;
    //}
    //Log_info("Server %lu -> Inside leader before event sleep 1",loc_id_);
    //check=true;
    Log_info("Server %lu -> Calling wait on ev inside empty append entry %p",loc_id_,ev.get());
    ev->Wait(30000);
      // if(ev_global->status_ == Event::TIMEOUT)
      // {
      //   Log_info("Send empty append entry")
      // }
    Log_info("Server %lu -> Inside leader after coroutine sleep 1",loc_id_);
    
    mtx_.lock();
    if(state != "leader")
    {
      mtx_.unlock();
      return;
    }
    mtx_.unlock();

    mtx_.lock();
    
    bool check_is_replicated = true;
    for(int i=0;i<5;i++){
      if(nextIndex[i]<stateLog.size())
      {
        check_is_replicated = true;
      }
    }
    auto ev_concensus = Reactor::CreateSpEvent<IntEvent>();
    if(!check_is_replicated)
    {
      Log_info("Server %lu -> As leader, found commitIndex behind. Trying to replicate",loc_id_);
      mtx_.unlock();
      Coroutine::CreateRun([this](){
        Log_info("Server %lu -> Inside leader inside coroutine sleep 2, before starting concensus",loc_id_);
        startConsensus();
        Log_info("Server %lu -> Inside leader inside coroutine after concensus",loc_id_);
      });
      //check = true;
    }
    mtx_.unlock();
    ev_concensus->Wait(50000);
    // if(check == false)
    // {
    //   Log_info("Server %lu -> Calling coroutine sleep",loc_id_);
    //   Coroutine::Sleep(5000);
    // }
    Log_info("Server %lu -> Inside leader after concensus before coroutine sleep 2",loc_id_);
    time_since_heartbeat = chrono::system_clock::now() - last_heartbeat_time;
    if((100-time_since_heartbeat.count())>0)
      Coroutine::Sleep((100-time_since_heartbeat.count())*1000);
    Log_info("Server %lu -> Inside leader after consensus after coroutine sleep 2",loc_id_);
  //}
}

void RaftServer::startConsensus(){

  Log_info("Server %lu -> Starting concensus, trying to take a lock",loc_id_);

  std::lock_guard<std::recursive_mutex> guard(mtx_);
  
  Log_info("Server %lu -> Am the leader. Starting concensus with current leaderCommitIndex %lu",loc_id_,commitIndex);
  
  auto proxies = commo()->rpc_par_proxies_[0];
    
  for(int i=0;i<proxies.size();i++)
  {
    if(proxies[i].first != loc_id_)
    {
      Log_info("Server %lu -> Just before entering outer coroutine for sending entries",loc_id_);
      //mtx_.lock();
      //mtx_.unlock();
      auto ev_global = Reactor::CreateSpEvent<IntEvent>();
      Log_info("Server %lu -> Initialised smart pointer global ev as %p",loc_id_,ev_global.get());
      Coroutine::CreateRun([this,i,proxies,ev_global]
      {
        Log_info("Server %lu -> Entered coroutine block for sending append entries with current nextIndex %d and matchIndex %d",loc_id_,nextIndex[i],matchIndex[i]);
        while(nextIndex[i] <= (stateLog.size()-1))
        {
          //Coroutine::CreateRun([this,ev_global,i,proxies](){
            uint64_t returnTerm = 0;
            int totalLogSize = stateLog.size();
            uint64_t prevLogIndex = nextIndex[i]-1;
            uint64_t prevLogTerm = stateLog[prevLogIndex].term;
            int currentNextIndex= nextIndex[i];
            bool_t followerAppendOK = false;
            Log_info("Server %lu -> Inside start concensus before calling SendAppendEntry",loc_id_);
            mtx_.unlock();
            auto event = commo()->SendAppendEntries(
                                    0,
                                    proxies[i].first,
                                    loc_id_,
                                    prevLogIndex,
                                    prevLogTerm,
                                    stateLog[currentNextIndex].term,
                                    commitIndex,
                                    stateLog[currentNextIndex].cmd,
                                    &returnTerm,
                                    &followerAppendOK
                                  );
            //Coroutine::Sleep(1000);
            event->Wait(10000);
            //Log_info("Server %lu -> Inside start consensus before calling individual wait",loc_id_);                        
            
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
              mtx_.lock();
              if(returnTerm > currentTerm)
              {
                Log_info("Server %lu -> Received greater term from append entry response",loc_id_);
                state = "follower";
                currentTerm = returnTerm;
                mtx_.unlock();
                return false;
              }
              if(followerAppendOK)
              {
                Log_info("Server %lu -> Append entry for %lu accepted",loc_id_,proxies[i].first);
                matchIndex[i] = currentNextIndex;
                Log_info("Server %lu -> Able to increase match index value",loc_id_);
                nextIndex[i] = currentNextIndex + 1;
                Log_info("Sever %lu -> Increased nextIndex value to %d and match index to %d",nextIndex[i],matchIndex[i]);
              }
              else
              {
                Log_info("Server %lu -> Append entry for %lu rejected",loc_id_,proxies[i].first);
                if(currentNextIndex>0)
                {
                  Log_info("Server %lu -> First append entry failed, retrying",loc_id_);
                  nextIndex[i] = currentNextIndex-1;
                }
              }
              mtx_.unlock();
            }
          //});
          Log_info("Server %lu -> Inside consensus outside inner coroutine before calling sleep",loc_id_);                        
          mtx_.unlock();
          //Coroutine::Sleep(1000);
          Log_info("Server %lu -> Inside consensus outside inner coroutine after calling sleep",loc_id_);                        
        }
        mtx_.unlock();
        {
          std::lock_guard<std::recursive_mutex> guard(mtx_);
          Log_info("Server %lu -> Setting smart pointer global ev %p",loc_id_,ev_global.get());
          //ev_global->Set(0);
          ev_global->Set(1);
        }
      });
      Log_info("Server %lu -> Inside consensus outside outer coroutine before calling global wait",loc_id_);                        
      //Coroutine::Sleep(10000);
      Log_info("Server %lu -> Calling wait on smart pointer ev_global %p",ev_global.get());
      ev_global->Wait(100000);
      Log_info("Server %lu -> Inside consensus outside outer coroutine after calling global wait",loc_id_);                        
      if(ev_global->status_ == Event::TIMEOUT)
      {
        
        Log_info("Server %lu -> Log replication to %lu failed, will try later, moving to next one",loc_id_,proxies[i].first);
      }
      else
      {
        Log_info("Server %lu -> Log replication to %lu succeeded",loc_id_,proxies[i].first);
      }
      //Log_info("Server %lu -> Inside start consensus outside coroutine before calling global wait",loc_id_);                        
    }
  }
  while(true)
  {
    uint64_t j=commitIndex+1;
    int total_agreement = 1;
    Log_info("Server %lu -> Checking majority for commitIndex %lu",loc_id_,j);
    for(int i=0;i<5;i++)
    {
      if(j<=matchIndex[i])
      {
        if(stateLog[j].term == currentTerm)
        {
          Log_info("Server %lu -> Agreement achieved on commitIndex %lu for server %d",loc_id_,j,i);
          total_agreement++;
        }
        else{
          Log_info("Server %lu -> Found term mistmatch, hence could not get majority commitment");
        }
      }
    }

    Log_info("Server %lu -> Received agreement count %d",loc_id_,total_agreement);
    if(total_agreement >= 3)
    {
      Log_info("Server %lu -> Received majority agreement for commitIndex %lu",loc_id_,j);
      commitIndex=j;
    }
    else
    {
      Log_info("Server %lu -> Could not get majority for commitIndex %lu",loc_id_,j);
      break;
    }
  }
  while((lastApplied+1)<=commitIndex)
  {
      Log_info("Server %lu -> Found last applied less than commit index, passing to app next",loc_id_);
      app_next_(*(stateLog[lastApplied+1].cmd));
      lastApplied++;
  }
  Log_info("Server %lu -> Inside start consensus - end of start concensus",loc_id_);                        
}

void RaftServer::Setup() {
  
  
  Log_info("Server %lu -> Calling setup",loc_id_);
  Coroutine::CreateRun([this](){
    while(true)
    {
      Log_info("Server %lu -> Inside setup starting",loc_id_);
      mtx_.lock();
      if(state == "follower")
      {
        uint64_t temp_term = currentTerm;
        mtx_.unlock();
        Log_info("Server %lu ->Calling convert to follower",loc_id_);
        convertToFollower(temp_term);
        Log_info("Server %lu ->After convert to follower",loc_id_);
      }
      if(state == "candidate")
      {
        mtx_.unlock();
        Log_info("Server %lu ->Calling become candidate",loc_id_);
        becomeCandidate();
        Log_info("Server %lu ->After become candidate",loc_id_);
      }
      if(state == "leader")
      {
        mtx_.unlock();
        Log_info("Server %lu ->Calling become leader",loc_id_);
        becomeLeader();
        Log_info("Server %lu ->After leader",loc_id_);
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
    Log_info("Server %lu -> Not the leader, returning false from start",loc_id_);
    return false;
  }
  else
  {
    
    Log_info("Server %lu -> Beginning start logic",loc_id_);
    stateLog.push_back(LogEntry(cmd,currentTerm));
    *index = stateLog.size()-1;
    Log_info("Server %lu -> Appended Log. Starting consensus from leader",loc_id_);
    //if(commitIndex < stateLog.size())
    //{
    //Log_info("Server %lu -> commitIndex less than state log size",loc_id_);
    //Log_info("Server %lu -> Yielding control to leader to start consensus",loc_id_);
    //Coroutine::Sleep(100000);
    //Log_info("Server %lu -> Back from coroutine sleep in start",loc_id_);
      //startConsensus();
    //}
    //Log_info("Server %lu ->Came back from start concensus inside Coroutine",loc_id_);
    //Log_info("Server %lu -> Outside coroutine. Before sleep",loc_id_);
    //Coroutine::Sleep(10000);
    //Log_info("Server %lu -> Method Start -> Back from sleep",loc_id_);
    //Log_info("Server %lu -> Appended the cmd to stateLog. New size is %d and currentTerm is %lu",loc_id_,stateLog.size(),currentTerm);
    Log_info("Server %lu -> End of start. Returning true");
    return true;
  }
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  
  Log_info("Server %lu -> Inside get state",loc_id_);
  
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
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_info("Server %lu -> Inside connect disconnect",loc_id_);
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
  Log_info("Server %lu -> Checking is disconnedted %d",loc_id_,disconnected_);
  return disconnected_;
}

} // namespace janus
