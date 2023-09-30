
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "macros.h"

namespace janus {

RaftCommo::RaftCommo(PollMgr* poll) : Communicator(poll) {
}

void RaftCommo::SendRequestVote(
                                parid_t par_id,
                                uint64_t term,
                                siteid_t candidateId,
                                uint64_t lastLogIndex,
                                uint64_t lastLogTerm,
                                uint64_t* max_return_term,
                                uint64_t* total_votes_granted) {
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  Log_info("Inside request vote for %lli",candidateId);
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  uint64_t temp_max_return_term = -1;
  uint64_t temp_total_votes_granted = 1;
  //Log_info("Entering call loop for %lli",candidateId);
  for (auto& p : proxies) {
    if (p.first != candidateId) {
      Log_info("Calling request vote to %lli from %lli",p.first,candidateId);
      uint64_t returnTerm=-1;
      bool_t vote_granted=false;
      uint64_t *pointer_to_returnTerm = &returnTerm;
      bool_t *pointer_to_bool = &vote_granted;
      Coroutine::CreateRun([=](){
        RaftProxy *proxy = (RaftProxy*) p.second;
        FutureAttr fuattr;
        fuattr.callback = [=](Future* fu) {
        /* this is a handler that will be invoked when the RPC returns */
        /* retrieve RPC return values in order */

        fu->get_reply() >> *pointer_to_returnTerm;
        fu->get_reply() >> *pointer_to_bool;
        ev->Set(1);
        //Log_info("Processing RPC response for ")
        /* process the RPC response here */
        };
        //Log_info("Just before async call for %lli to %lli",candidateId,p.first);
        Call_Async( proxy, 
                  RequestVote, 
                  term, 
                  candidateId, 
                  lastLogIndex, 
                  lastLogTerm, 
                  fuattr
                );
        
        /* Always use Call_Async(proxy, RPC name, RPC args..., fuattr)
        * to asynchronously invoke RPCs */
      });
      ev->Wait(1000000);
      if(ev->status_ == Event::TIMEOUT){
        Log_info("Request vote to %lli timed out",p.first);
      }
      else{
        if(vote_granted)
          temp_total_votes_granted++;
        temp_max_return_term = max(temp_max_return_term,returnTerm);
      }
    }
  }
  *max_return_term = temp_max_return_term;
  *total_votes_granted = temp_total_votes_granted;
}

void RaftCommo::SendEmptyAppendEntries(
                                parid_t par_id,
                                uint64_t term,
                                siteid_t candidateId,
                                uint64_t* maxReturnTerm) {
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  uint64_t max_return_term = -1;
  for (auto& p : proxies) {
    if (p.first != candidateId) {
      uint64_t returnTerm = -1;
      uint64_t *pointer_to_returnTerm = &returnTerm;
      Coroutine::CreateRun([=](){
        RaftProxy *proxy = (RaftProxy*) p.second;
        FutureAttr fuattr;
        fuattr.callback = [=](Future* fu) {
          /* this is a handler that will be invoked when the RPC returns */
          /* retrieve RPC return values in order */
          fu->get_reply() >> *pointer_to_returnTerm;
          ev->Set(1);
          /* process the RPC response here */
        };
        /* Always use Call_Async(proxy, RPC name, RPC args..., fuattr)
        * to asynchronously invoke RPCs */
        Log_info("Making empty append entry to %lli",p.first);
        Call_Async( proxy, 
                    EmptyAppendEntries, 
                    term, 
                    candidateId, 
                    fuattr
                  );
      });
      ev->Wait(1000000);
      if(ev->status_ == Event::TIMEOUT)
      {
        Log_info("Empty append entry to %lli failed",p.first);
      }
      else
      {
        max_return_term = max(max_return_term,returnTerm);  
      }
    }
  }
  *maxReturnTerm = max_return_term;
}

void RaftCommo::SendAppendEntries(parid_t par_id,
                                  siteid_t site_id,
                                  shared_ptr<Marshallable> cmd) {
  /*
   * More example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [](Future* fu) {
        bool_t followerAppendOK;
        fu->get_reply() >> followerAppendOK;
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      MarshallDeputy md(cmd);
      Call_Async(proxy, AppendEntries, md, fuattr);
    }
  }
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
