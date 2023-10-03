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
  commitIndex = 0;
  //lastApplied = 0;
  nextIndex = vector<int>(5,1);
  matchIndex = vector<int> (5,0);
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
    return 150+(rand()%150);
}

void RaftServer::HandleEmptyAppendEntries(
                            const uint64_t& term,
                            const siteid_t& candidateId,
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
  if(currentTerm == term)
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
      
    mtx_.lock();
    time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
    Coroutine::Sleep(20000);
  }

  Log_info("Timeout completed as follower for %lli. Switching to candidate",loc_id_);
  /* 
    Election timeout finished as a follower
    Changing to candidate
  */
  mtx_.lock();
    state = "candidate";
  becomeCandidate();
}


void RaftServer::becomeCandidate()
{
    
  /*
    Getting random timeout for the sever
  */
  
  int electionTimeout = generateElectionTimeout();
  
  chrono::milliseconds endTimeout(electionTimeout);
  Log_info("Candidate timeout for server %lli is %d",loc_id_,electionTimeout);
  
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

  while(time_spent < endTimeout)
  {
    
    Coroutine::CreateRun([=](){

      uint64_t max_return_term=0;
      uint64_t total_votes_received=0;

      mtx_.lock();
      uint64_t tempCurrentTerm = currentTerm;
      uint64_t tempLastLogIndex = stateLog.size();
      uint64_t tempLastLogTerm = stateLog.size()==0?0:stateLog[tempLastLogIndex-1].term;
      mtx_.unlock();

      auto event = commo()->SendRequestVote(
                              0,
                              tempCurrentTerm, // sending my term
                              loc_id_,  //sending my id
                              tempLastLogIndex, //my last log index
                              tempLastLogTerm, // my last log term
                              &max_return_term,
                              &total_votes_received
                            );
      event->Wait(50000);
      if(event->status_ == Event::TIMEOUT)
      {
        Log_info("Timeout happened for all send request votes");
      }
      else
      {
        
        Log_info("Got reply from all servers with total vote count %d and max term %lli",total_votes_received,max_return_term);
        
        mtx_.lock();
        
        if(state != "candidate")
        {
          Log_info("Changed state to %s while waiting for votes",state);
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
          Log_info("Max return term is not zero");
          if(max_return_term > currentTerm)
          {
            Log_info("Received bigger term after requestVote. Current term is %lu and got %lu",currentTerm,max_return_term);
            
            state = "follower";
            currentTerm = max_return_term;
            mtx_.unlock();
            return;               
          }

          if(max_return_term == currentTerm)
          {
            /*
            Checking for majority of votes received
            and changing to a leader in case that happens
            */
            if(total_votes_received >= (commo()->rpc_par_proxies_[0].size()+1)/2)
            {
              Log_info("Election supremacy. Won election as %lli",loc_id_);
              state = "leader";
              mtx_.unlock();
              return;
            }
            else
            {
              Log_info("Did not received majority votes");
            }
          }
        }
        mtx_.unlock();
      }
    });

    //Coroutine::Sleep(20000);

    mtx_.lock();

    // if(state == "follower")
    // {
    //   mtx_.unlock();
    //   Log_info("Found follower as state");
    //   convertToFollower(currentTerm);
    //   return;
    // }
    // if(state == "leader")
    // {
    //   mtx_.unlock();
    //   Log_info("Found leader as state, becoming leader");
    //   becomeLeader();
    //   return;
    // }
    /*
      Updating time diff
    */
    time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
    Log_info("Time spent as candidate for server %lli is %f",loc_id_,time_spent.count());
  }
  Log_info("Time elapsed since one election without result for %lli",loc_id_);
  //becomeCandidate();
}

void RaftServer::HandleAppendEntries(
                      const siteid_t& candidateId,
                      const uint64_t& prevLogIndex,
                      const uint64_t& prevLogTerm,
                      const uint64_t& term,
                      const MarshallDeputy& md_cmd,
                      const uint64_t& leaderCommitIndex,
                      uint64_t* returnTerm,
                      bool_t* followerAppendOK
                      )
{
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *returnTerm = currentTerm;
  std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  if(term < currentTerm)
  {
    Log_info("Server %lu -> Received append entry with less term, sending false",loc_id_);
    *followerAppendOK = false;
    return;
  }
  if(prevLogIndex==0)
  {
    Log_info("Server %lu -> Received entries from starting from %lu",loc_id_,candidateId);
    stateLog = vector<LogEntry>();
    stateLog.push_back(LogEntry(cmd,term));
    uint64_t temp_size = (int)stateLog.size();
    *followerAppendOK = true;
    if(leaderCommitIndex > commitIndex)
    {
      // Log_info("LeaderCommit index is greater,current commit index %lu",commitIndex);
      // if(temp_size < leaderCommitIndex)
      //   commitIndex = temp_size;
      // else
      //   commitIndex = leaderCommitIndex;
      commitIndex = min(leaderCommitIndex, stateLog.size());
    }
    Log_info("Server %lu -> Commit index updated to %lld",commitIndex);
  }
  else
  {
    if(stateLog.size() >= prevLogIndex)
    {
      if(stateLog[prevLogIndex-1].term != prevLogTerm)
      {
        Log_info("Server %lu -> Previous log term does not match for append entry from %lu. Sending false",loc_id_,candidateId);
        *followerAppendOK = false;
        return;
      }
      vector<LogEntry>:: iterator it = stateLog.begin()+prevLogIndex;
      stateLog.erase(it,stateLog.end());
      stateLog.push_back(LogEntry(cmd,term));
      *followerAppendOK = true;
      if(leaderCommitIndex > commitIndex)
        commitIndex = min(leaderCommitIndex,stateLog.size());
      Log_info("Server %lu -> Appended log and updated commit index to %lu",loc_id_,commitIndex);
    }
    else
    {
      Log_info("Server %lu -> Log size is smaller than sent logs. Sending false",loc_id_);
      *followerAppendOK = false;
      return;
    }
  }
  Log_info("Server %lu -> Append entry from %lu processed",loc_id_,candidateId);
}

bool RaftServer::checkMoreUpdated(
                                  uint64_t lastLogIndex,
                                  uint64_t lastLogTerm)
{
  if(stateLog.size() == 0)
    return true;
  if(lastLogIndex == 0)
    return false;
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

  Log_info("Current term of %lli while serving request vote for %lli is %lli",loc_id_,candidateId,currentTerm);
  
  if(term > currentTerm)
  {
    votedFor = 6;
    lastStartTime = std::chrono::system_clock::now();
    currentTerm = term;
    state = "follower";
    Log_info("Current term is less than request vote term, becoming follower %lli",loc_id_);
  }

  if(currentTerm == term)
  {
    if(votedFor == 6 || votedFor == candidateId)
    {
      if(checkMoreUpdated(lastLogIndex, lastLogTerm))
      {
        Log_info("Vote granted to %lli",candidateId);
        *vote_granted = true;
        votedFor = candidateId;
        lastStartTime = std::chrono::system_clock::now();
      }
      else
      {
        Log_info("Logs of %lu not updated enough. Not granting vote",candidateId);
      }
    }
    else
    {
      Log_info("Not granting vote to %lli on %lli as already voted for %d",candidateId,loc_id_,votedFor);
    }
  }
  else
  {
      Log_info("Vote requested term is less than current term, not giving vote");
      *vote_granted = false;
  }
  mtx_.unlock();
  Log_info("Reply for request vote from %lli on server %lli completed. Sending %d as vote and %lli as return term",candidateId,loc_id_,*vote_granted,*returnTerm);
}


void RaftServer::becomeLeader()
{
  mtx_.lock();
  state = "leader";
  mtx_.unlock();

  int c = 0;
  chrono::time_point<chrono::system_clock> last_heartbeat_time = chrono::system_clock::now();
  chrono::duration<double,milli> time_since_heartbeat;
  
  while(true)
  {
    uint64_t returned_max_term;

    mtx_.lock();
    uint64_t temp_term = currentTerm;
    time_since_heartbeat = chrono::system_clock::now() - last_heartbeat_time;
    mtx_.unlock();

    if(c==0 || time_since_heartbeat.count() > 100)
    {
      
      last_heartbeat_time = chrono::system_clock::now();
      Coroutine::CreateRun([=](){

        uint64_t returned_max_term = 0;
        auto event = commo()->SendEmptyAppendEntries(
                                    0,
                                    temp_term,
                                    loc_id_,
                                    &returned_max_term
                                      );
        event->Wait(50000);
        
        if(event -> status_ == Event::TIMEOUT)
        {
          Log_info("Timeout happened for all heartbeats");
        }
        else
        {
          Log_info("Response for all heartbeats received for %lu with max return term %lu",loc_id_,returned_max_term);
          if(returned_max_term > currentTerm)
          {
            Log_info("%lli After sending heartbeat, got a bigger term. Stepping down as leader",loc_id_);
            mtx_.lock();
            state = "follower";
            currentTerm = returned_max_term;
            mtx_.unlock();
            //convertToFollower(returned_max_term);
          }
        }
      });
      c=1;

    }
    Coroutine::Sleep(20000);

    mtx_.lock();
    if(state != "leader")
    {
      mtx_.unlock();
      convertToFollower(currentTerm);
      return;
    }
    mtx_.unlock();
  }
}

void RaftServer::Setup() {
  std::lock_guard<std:recursive_mutex> guard(mtx_);
  while(true){
    if(state=="follower")
      convertToFollower(1);
    if(state=="candidate")
      becomeCandidate();
    if(state=="leader")
      becomeLeader();
  }
  
}

bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term)
{
  /* Your code here. This function can be called from another OS thread. */
  std::lock_guard<std::recursive_mutex> guard(mtx_);
  *term = currentTerm;
  *index = stateLog.size();
    
  if(state != "leader")
  {
    Log_info("Not the leader, returning false from start for %lu",loc_id_);
    return false;
  }
  else
  {
    Log_info("Server %lu is the leader. Starting concensus on the cmd",loc_id_);
    stateLog.push_back(LogEntry(cmd, currentTerm));
    auto proxies = commo()->rpc_par_proxies_[0];
    int totalAppendsCount = 0;
    for(int i=0;i<proxies.size();i++)
    {
      if(proxies[i].first != loc_id_)
      {
        auto ev_total = Reactor::CreateSpEvent<IntEvent>();
        Coroutine::CreateRun([=](){
          while(nextIndex[i] <= stateLog.size())
          {
            uint64_t returnTerm = 0;
            int totalLogSize = stateLog.size();
            uint64_t prevLogIndex = nextIndex[i]-1;
            uint64_t prevLogTerm = nextIndex[i]==1?0:stateLog[prevLogIndex-1].term;
            bool_t followerAppendOK = false;
            auto event = commo()->SendAppendEntries(
                                                0,
                                                proxies[i].first,
                                                loc_id_,
                                                prevLogIndex,
                                                prevLogTerm,
                                                stateLog[nextIndex[i]-1].term,
                                                stateLog[nextIndex[i]-1].cmd,
                                                commitIndex,
                                                &returnTerm,
                                                &followerAppendOK
                                                );
            Coroutine::Sleep(10000);
            event->Wait(5000);
            if(event->status_ == Event::TIMEOUT)
            {
              Log_info("AppendEntry from %lu to %lu timed out",proxies[i].first,loc_id_);
            }
            else
            {
              Log_info("Got response from %lu -> %lu as term, %d as didAppend",proxies[i].first,returnTerm,followerAppendOK);
              if(returnTerm > currentTerm)
              {
                Log_info("Received greater term from append entry response");
                state = "follower";
                currentTerm = returnTerm;
                return false;
              }
              if(followerAppendOK)
              {
                Log_info("Append entry by %lu for %lu accepted",loc_id_,proxies[i].first);
                matchIndex[i] = nextIndex[i];
                nextIndex[i]++;
              }
              else
              {
                Log_info("Append entry by %lu for %lu rejected",loc_id_,proxies[i].first);
                if(nextIndex[i]>1)
                  nextIndex[i]--;
              }
            }
          }
          ev_total->Set(1);
        });
        //Coroutine::Sleep(20000);
        ev_total->Wait(50000);
        if(ev_total->status_ == Event::TIMEOUT)
        {
          Log_info("Updating Append entries to %lu failed, skipping this replica and trying next",proxies[i].first);
        }
      }
    }
    while(true)
    {
      int j=commitIndex+1;
      bool check = true;
      for(int i=0;i<commo()->rpc_par_proxies_[0].size() && check;i++)
      {
        if(j<=matchIndex[i])
        {
          if(stateLog[j-1].term == currentTerm) 
            continue;
          else
            check = false;
        }
        else
          check = false;
      }
      if(check)
        commitIndex=j;
      else
        break;
    }
    *index = commitIndex;
    return true;
  }
  return true;
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  
  Log_info("Inside get state for %lu",loc_id_);
  
  mtx_.lock();

  if(state == "leader")
    *is_leader = true;
  else
    *is_leader = false;
  *term = currentTerm;

  mtx_.unlock();

  Log_info("replying with values as %d and term %lu for %lu",*is_leader,*term,loc_id_);
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
  Log_info("Checking is disconnedted for %lu",loc_id_);
  return disconnected_;
}

} // namespace janus
