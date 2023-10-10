
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
    svr_->HandleRequestVote( term,
                            candidateId,
                            lastLogIndex,
                            lastLogTerm,
                            returnTerm,
                            vote_granted);
    defer->reply();
}

void RaftServiceImpl::HandleAppendEntriesCombined(
                                        const siteid_t& candidateId,
                                        const uint64_t& prevLogIndex,
                                        const uint64_t& prevLogTerm,
                                        const uint64_t& logTerm,
                                        const uint64_t& currentTerm,
                                        const uint64_t& leaderCommitIndex,
                                        const uint64_t& isHeartbeat,
                                        const MarshallDeputy& md_cmd,
                                        uint64_t* followerLogSize,
                                        uint64_t* returnTerm,
                                        bool_t* followerAppendOK,
                                        rrr::DeferredReply* defer) {
  svr_->HandleAppendEntriesCombined(
                        candidateId,
                        prevLogIndex,
                        prevLogTerm,
                        logTerm,
                        currentTerm,
                        leaderCommitIndex,
                        isHeartbeat,
                        md_cmd,
                        followerLogSize,
                        returnTerm,
                        followerAppendOK
                    );
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
