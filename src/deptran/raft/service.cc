
#include "../marshallable.h"
#include "service.h"
#include "server.h"
#include<chrono>

namespace janus {

RaftServiceImpl::RaftServiceImpl(TxLogServer *sched)
    : svr_((RaftServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}

int RaftServiceImpl::ReturnLastLogTerm(){
    int returnVal=-1;
    svr_->mtx_.lock();
    returnVal = svr_->stateLog.size()-1;
    svr_->mtx_.unlock();
    return returnVal;
}


void RaftServiceImpl::HandleRequestVote(const uint64_t& term,
                                        const siteid_t& candidateId,
                                        const uint64_t& lastLogIndex,
                                        const uint64_t& lastLogTerm,
                                        uint64_t* returnTerm,
                                        bool_t* vote_granted,
                                        rrr::DeferredReply* defer) {
    Log_info("Received request vote for %lli from server %lli",svr_->loc_id_,candidateId);
    svr_->HandleRequestVote( term,
                            candidateId,
                            lastLogIndex,
                            lastLogTerm,
                            returnTerm,
                            vote_granted);
    Log_info("Back to request vote call for %lli from server %lli with vote as %d and returnTerm as %lli",svr_->loc_id_,candidateId,*vote_granted,*returnTerm);
    defer->reply();
}
  
void RaftServiceImpl::HandleEmptyAppendEntries(
                                            const uint64_t& term,
                                            const siteid_t& candidateId,
                                            uint64_t* returnTerm,
                                            rrr::DeferredReply* defer) {
    Log_info("Received empty append entries for %lli from %lli",svr_->loc_id_,candidateId);
    svr_->HandleEmptyAppendEntries(
                        term,
                        candidateId,
                        returnTerm
                                    );
    defer->reply();
} 

void RaftServiceImpl::HandleAppendEntries(
                                        const siteid_t& candidateId,
                                        const uint64_t& prevLogIndex,
                                        const uint64_t& prevLogTerm,
                                        const uint64_t& term,
                                        const MarshallDeputy& md_cmd,
                                        const uint64_t& leaderCommitIndex,
                                        uint64_t* returnTerm,
                                        bool_t* followerAppendOK,
                                        rrr::DeferredReply* defer) {
  Log_info("Received Append entries from %lu to %lu",candidateId,svr_->loc_id_);
  svr_->HandleAppendEntries(
                        candidateId,
                        prevLogIndex,
                        prevLogTerm,
                        term,
                        md_cmd,
                        leaderCommitIndex,
                        returnTerm,
                        followerAppendOK
                    );
  Log_info("Handled append entry from %lu to %lu successfully",candidateId,svr_->loc_id_);
  defer->reply();
}

void RaftServiceImpl::HandleHelloRpc(const string& req,
                                     string* res,
                                     rrr::DeferredReply* defer) {
  /* Your code here */
  Log_info("receive an rpc: %s", req.c_str());
  *res = "world";
  defer->reply();
}

} // namespace janus;
