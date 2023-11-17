
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include <mutex>
#include "macros.h"

namespace janus {

RaftCommo::RaftCommo(PollMgr* poll) : Communicator(poll) {
}

shared_ptr<IntEvent> 
RaftCommo::SendRequestVote(
                            parid_t par_id,
                            siteid_t site_id,
                            uint64_t term,
                            //siteid_t candidateId,
                            uint64_t lastLogIndex,
                            uint64_t lastLogTerm,
                            uint64_t* return_term,
                            bool_t* vote_granted) 
{
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  Log_info("Server %lu -> Commo- Inside request vote",site_id);

  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  
  for (auto& p : proxies)
  {
    if(p.first == site_id)
    {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      Log_info("Server %lu -> Commo - Calling request vote to %lli",site_id,p.first);
      fuattr.callback = [=](Future* fu) 
      {
        std::recursive_mutex mutex_;
        std::lock_guard<std::recursive_mutex> guard(mutex_);
        fu->get_reply() >> *return_term;
        fu->get_reply() >> *vote_granted;
        if(ev->status_ != Event::TIMEOUT)
          ev->Set(1);
      };
      Call_Async(
                proxy, 
                RequestVote, 
                term, 
                site_id, 
                lastLogIndex, 
                lastLogTerm, 
                fuattr
              );
    }
  }
  return ev;
}

shared_ptr<IntEvent>
RaftCommo::SendAppendEntriesCombined(
                          const parid_t& par_id,
                          const siteid_t& site_id,
                          const uint64_t& prevLogIndex,
                          const uint64_t& prevLogTerm,
                          const uint64_t& logTerm,
                          const uint64_t& currentTerm,
                          const uint64_t& leaderCommitIndex,
                          const uint64_t& isHeartBeat,
                          shared_ptr<Marshallable> cmd,
                          uint64_t* followerLogSize,
                          uint64_t* returnTerm,
                          bool_t* followerAppendOK)
{
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  //Log_info("Server %lu -> Inside commo for Sending Combined Append entry ",candidateId);
  for (auto& p : proxies) {
    if (p.first == site_id)
    {
      //Log_info("Server %lu -> Found match, calling async",candidateId);
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [=](Future* fu) {

        fu->get_reply() >> *followerLogSize;
        fu->get_reply() >> *returnTerm;
        fu->get_reply() >> *followerAppendOK;
        if(ev->status_ != Event::TIMEOUT)
          ev->Set(1);
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      Log_info("Server %lu -> Trying to create marshall deputy",site_id);
      std::recursive_mutex mutex_1;
      std::lock_guard<std::recursive_mutex> lock_guard(mutex_1);
      MarshallDeputy md(cmd);
      Log_info("Server %lu -> Creation succeeded",site_id);
      Call_Async( 
                  proxy, 
                  AppendEntriesCombined, 
                  site_id,
                  prevLogIndex,
                  prevLogTerm,
                  logTerm,
                  currentTerm,
                  leaderCommitIndex,
                  isHeartBeat,
                  md,
                  fuattr
                );
    }
  }
  return ev;
}

shared_ptr<IntEvent> 
RaftCommo::SendString(parid_t par_id, siteid_t site_id, const string& msg, string* res) {
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [res,ev](Future* fu) {
        fu->get_reply() >> *res;
        ev->Set(1);
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      Call_Async(proxy, HelloRpc, msg, fuattr);
    }
  }
  return ev;
}


} // namespace janus
