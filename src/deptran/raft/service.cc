
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
    Log_info("Received request vote for %lli on server %lli",candidateId,svr_->loc_id_);
    svr_->mtx_.lock();
    *vote_granted = false;
    *returnTerm = svr_->currentTerm;
    if(term > svr_->currentTerm)
    {
        svr_->mtx_.unlock();
        Log_info("")
        svr_->convertToFollower(term);
    }
    if(svr_->currentTerm == term)
    {
        if(svr_->votedFor == -1 || svr_->votedFor == candidateId)
        {
            Log_info("Vote granted to %lli",candidateId);
            *vote_granted = true;
            svr_->votedFor = candidateId;
            svr_->lastStartTime = std::chrono::system_clock::now();
        }
    }
    else
    {
        *vote_granted = false;
    }
    svr_->mtx_.unlock();
    Log_info("Reply for request vote from %lli on server %lli completed",candidateId,svr_->loc_id_);
    defer->reply();
}
  
void RaftServiceImpl::HandleEmptyAppendEntries(const uint64_t& term,
                                            const siteid_t& leader_id,
                                            uint64_t* returnTerm,
                                            rrr::DeferredReply* defer) {
    using namespace std::chrono;
    svr_->mtx_.lock();
    *returnTerm = svr_->currentTerm;
    if(term > svr_->currentTerm)
    {
        svr_->mtx_.unlock();
        svr_->convertToFollower(term);
    }
    else if(svr_->currentTerm == term)
    {
        if(svr_->state != "follower")
        {
            svr_->mtx_.unlock();
            svr_->convertToFollower(term);
        }
    }
    svr_->mtx_.unlock();
    defer->reply();
} 

void RaftServiceImpl::HandleAppendEntries(const MarshallDeputy& md_cmd,
                                          bool_t *followerAppendOK,
                                          rrr::DeferredReply* defer) {
  
  std::shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  *followerAppendOK = false;
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
