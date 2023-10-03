
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
                                uint64_t term,
                                siteid_t candidateId,
                                uint64_t lastLogIndex,
                                uint64_t lastLogTerm,
                                uint64_t* max_return_term,
                                uint64_t* total_votes_granted) 
{
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  Log_info("Inside request vote for %lli",candidateId);

  auto proxies = rpc_par_proxies_[par_id];
  uint64_t temp_max_return_term = 0;
  uint64_t temp_total_votes_granted = 1;
  std::mutex mutex_;

  //Log_info("Entering call loop for %lli",candidateId);
  auto ev_total = Reactor::CreateSpEvent<IntEvent>();
  int count = 0;
  for (auto& p : proxies) 
  {
    auto ev_individual = Reactor::CreateSpEvent<IntEvent>();
    uint64_t returnTerm = 0;
    bool_t vote_granted = false;
    uint64_t *pointer_to_returnTerm = &returnTerm;
    bool_t *pointer_to_bool = &vote_granted;
    RaftProxy *proxy = (RaftProxy*) p.second;
    FutureAttr fuattr;
      
    if (p.first != candidateId) 
    {
      Log_info("Calling request vote to %lli from %lli",p.first,candidateId);
      
      fuattr.callback = [=](Future* fu) 
      {
      
        fu->get_reply() >> *pointer_to_returnTerm;
        fu->get_reply() >> *pointer_to_bool;
        ev_individual->Set(1);
        //Log_info("Processing RPC response for ")
      };
      
      //Log_info("Just before async call for %lli to %lli",candidateId,p.first);
      Call_Async( 
                proxy, 
                RequestVote, 
                term, 
                candidateId, 
                lastLogIndex, 
                lastLogTerm, 
                fuattr
              );
    }
    ev_individual -> Wait(10000);
    if(ev_individual -> status_ == Event::TIMEOUT)
    {
      Log_info("Request vote to %lli from %lli timed out",p.first,candidateId);
    }
    else
    {
      Log_info("Got response without timeout to %lli from %lli as %d as vote and %lli as return term",p.first,candidateId,vote_granted,returnTerm);
      mutex_.lock();
      count++;
      if(vote_granted)
      {
        //Log_info("Positive vote got, increasing");
        temp_total_votes_granted++;
      }
      temp_max_return_term = max(temp_max_return_term,returnTerm);
      mutex_.unlock();
      //Log_info("Updated values of temp total votes and temp index is %lli and %lli",temp_total_votes_granted,temp_max_return_term);
    }
  }
  *max_return_term = temp_max_return_term;
  *total_votes_granted = temp_total_votes_granted;
  if(count)
    ev_total -> Set(1);
  Log_info("Total votes granted are %lu and max return term is %lu",*total_votes_granted,*max_return_term);
  return ev_total;
}

shared_ptr<IntEvent> 
RaftCommo::SendEmptyAppendEntries(
                                parid_t par_id,
                                uint64_t term,
                                siteid_t candidateId,
                                uint64_t* maxReturnTerm) 
{
  /*
   * Example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  Log_info("Starting to send empty append entries for %lli",loc_id_);
  
  auto proxies = rpc_par_proxies_[par_id];
  uint64_t max_return_term = 0;
  std::mutex mutex_;
  auto ev_total = Reactor::CreateSpEvent<IntEvent>();
  int count =0;
  for (auto& p : proxies) 
  {

    uint64_t returnTerm = 0;
    uint64_t *pointer_to_returnTerm = &returnTerm;
    auto ev_individual = Reactor::CreateSpEvent<IntEvent>();
    RaftProxy *proxy = (RaftProxy*) p.second;
    FutureAttr fuattr;
      
    if(p.first != candidateId) 
    {
      Log_info("Sending empty append entry to %lli from %lli",p.first,loc_id_);
      fuattr.callback = [=](Future* fu) 
      {
          /* this is a handler that will be invoked when the RPC returns */
          /* retrieve RPC return values in order */
          fu->get_reply() >> *pointer_to_returnTerm;
          ev_individual->Set(1);
      };
      
      Call_Async( proxy, 
                  EmptyAppendEntries, 
                  term, 
                  candidateId, 
                  fuattr
                );

    }
    ev_individual -> Wait(10000);
    if(ev_individual -> status_ == Event::TIMEOUT)
    {
      Log_info("Empty append entry to %lli failed",p.first);
    }
    else
    {
      mutex_.lock();
      count++;
      Log_info("Got back %lli as return term from %lli",returnTerm,p.first);
      max_return_term = max(max_return_term,returnTerm);
      mutex_.unlock();
    }
  }
  *maxReturnTerm = max_return_term;
  if(count)
    ev_total -> Set(1);
  return ev_total;
}

shared_ptr<IntEvent> 
RaftCommo::SendAppendEntries(parid_t par_id,
                            siteid_t site_id,
                            siteid_t candidateId,
                            uint64_t prevLogIndex,
                            uint64_t prevLogTerm,
                            uint64_t term,
                            shared_ptr<Marshallable> cmd,
                            uint64_t commitIndex,
                            uint64_t* returnTerm,
                            bool_t* followerAppendOK) {
  /*
   * More example code for sending a single RPC to server at site_id
   * You may modify and use this function or just use it as a reference
   */
  auto proxies = rpc_par_proxies_[par_id];
  auto ev = Reactor::CreateSpEvent<IntEvent>();
  for (auto& p : proxies) {
    if (p.first == site_id) {
      RaftProxy *proxy = (RaftProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [=](Future* fu) {
        fu->get_reply() >> *returnTerm;
        fu->get_reply() >> *followerAppendOK;
        ev->Set(1);
      };
      /* wrap Marshallable in a MarshallDeputy to send over RPC */
      MarshallDeputy md(cmd);
      Call_Async( proxy, 
                  AppendEntries, 
                  candidateId,
                  prevLogIndex,
                  prevLogTerm,
                  term,
                  md,
                  commitIndex,
                  fuattr);

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
