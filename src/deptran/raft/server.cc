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
  currentTerm = 0;
  votedFor = -1;
  state = "follower";
  lastStartTime = chrono::system_clock::now();
  lastLogIndex = 0;
  lastLogTerm = 0;
  stateLog = vector<MY_LOG*>();
  mtx_.unlock();
  Log_info("Server initialization completed for %lli",loc_id_);
  /* Your code here for server initialization. Note that this function is 
     called in a different OS thread. Be careful about thread safety if 
     you want to initialize variables here. */

}

RaftServer::~RaftServer() {
  /* Your code here for server teardown */

}

void RaftServer::Setup() {
  convertToFollower(0);
  // SyncRpcExample();
}

bool RaftServer::Start(shared_ptr<Marshallable> &cmd,
                       uint64_t *index,
                       uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  mtx_.lock();
  *term = currentTerm;
  *index = lastLogIndex;
  string temp_state = state;
  mtx_.unlock();
  if(temp_state != "leader")
  {
    Log_info("Not the leader, returning false from start");
    return false;
  }
  else{
    Log_info("Server is the leader. returning true");
    return true;
  }
}

void RaftServer::GetState(bool *is_leader, uint64_t *term) {
  /* Your code here. This function can be called from another OS thread. */
  mtx_.lock();
  if(state == "leader")
    *is_leader = true;
  else{
    *is_leader = false;
  }
  *term = currentTerm;
  mtx_.unlock();
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
    if (event->status_ == Event::TIMEOUT) {
      Log_info("timeout happens");
    } else {
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

bool RaftServer::IsDisconnected() {
  return disconnected_;
}

} // namespace janus
