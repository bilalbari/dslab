#pragma once

#include "rrr.hpp"

#include <errno.h>


namespace janus {

class RaftService: public rrr::Service {
public:
    enum {
        REQUESTVOTE = 0x3b35e4d5,
        APPENDENTRIESCOMBINED = 0x638424c7,
        HELLORPC = 0x6b235a58,
    };
    int __reg_to__(rrr::Server* svr) {
        int ret = 0;
        if ((ret = svr->reg(REQUESTVOTE, this, &RaftService::__RequestVote__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(APPENDENTRIESCOMBINED, this, &RaftService::__AppendEntriesCombined__wrapper__)) != 0) {
            goto err;
        }
        if ((ret = svr->reg(HELLORPC, this, &RaftService::__HelloRpc__wrapper__)) != 0) {
            goto err;
        }
        return 0;
    err:
        svr->unreg(REQUESTVOTE);
        svr->unreg(APPENDENTRIESCOMBINED);
        svr->unreg(HELLORPC);
        return ret;
    }
    // these RPC handler functions need to be implemented by user
    // for 'raw' handlers, remember to reply req, delete req, and sconn->release(); use sconn->run_async for heavy job
    virtual void RequestVote(const uint64_t& term, const siteid_t& candidateId, const uint64_t& lastLogIndex, const uint64_t& lastLogTerm, uint64_t* returnTerm, bool_t* vote_granted, rrr::DeferredReply* defer) = 0;
    virtual void AppendEntriesCombined(const siteid_t& candidateId, const uint64_t& prevLogIndex, const uint64_t& prevLogTerm, const uint64_t& logTerm, const uint64_t& currentTerm, const uint64_t& leaderCommitIndex, const uint64_t& isHeartbeat, const MarshallDeputy& cmd, uint64_t* followerLogSize, uint64_t* returnTerm, bool_t* followerAppendOK, rrr::DeferredReply* defer) = 0;
    virtual void HelloRpc(const std::string& req, std::string* res, rrr::DeferredReply* defer) = 0;
private:
    void __RequestVote__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        uint64_t* in_0 = new uint64_t;
        req->m >> *in_0;
        siteid_t* in_1 = new siteid_t;
        req->m >> *in_1;
        uint64_t* in_2 = new uint64_t;
        req->m >> *in_2;
        uint64_t* in_3 = new uint64_t;
        req->m >> *in_3;
        uint64_t* out_0 = new uint64_t;
        bool_t* out_1 = new bool_t;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
            *sconn << *out_1;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete in_3;
            delete out_0;
            delete out_1;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->RequestVote(*in_0, *in_1, *in_2, *in_3, out_0, out_1, __defer__);
    }
    void __AppendEntriesCombined__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        siteid_t* in_0 = new siteid_t;
        req->m >> *in_0;
        uint64_t* in_1 = new uint64_t;
        req->m >> *in_1;
        uint64_t* in_2 = new uint64_t;
        req->m >> *in_2;
        uint64_t* in_3 = new uint64_t;
        req->m >> *in_3;
        uint64_t* in_4 = new uint64_t;
        req->m >> *in_4;
        uint64_t* in_5 = new uint64_t;
        req->m >> *in_5;
        uint64_t* in_6 = new uint64_t;
        req->m >> *in_6;
        MarshallDeputy* in_7 = new MarshallDeputy;
        req->m >> *in_7;
        uint64_t* out_0 = new uint64_t;
        uint64_t* out_1 = new uint64_t;
        bool_t* out_2 = new bool_t;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
            *sconn << *out_1;
            *sconn << *out_2;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete in_1;
            delete in_2;
            delete in_3;
            delete in_4;
            delete in_5;
            delete in_6;
            delete in_7;
            delete out_0;
            delete out_1;
            delete out_2;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->AppendEntriesCombined(*in_0, *in_1, *in_2, *in_3, *in_4, *in_5, *in_6, *in_7, out_0, out_1, out_2, __defer__);
    }
    void __HelloRpc__wrapper__(rrr::Request* req, rrr::ServerConnection* sconn) {
        std::string* in_0 = new std::string;
        req->m >> *in_0;
        std::string* out_0 = new std::string;
        auto __marshal_reply__ = [=] {
            *sconn << *out_0;
        };
        auto __cleanup__ = [=] {
            delete in_0;
            delete out_0;
        };
        rrr::DeferredReply* __defer__ = new rrr::DeferredReply(req, sconn, __marshal_reply__, __cleanup__);
        this->HelloRpc(*in_0, out_0, __defer__);
    }
};

class RaftProxy {
protected:
    rrr::Client* __cl__;
public:
    RaftProxy(rrr::Client* cl): __cl__(cl) { }
    rrr::Future* async_RequestVote(const uint64_t& term, const siteid_t& candidateId, const uint64_t& lastLogIndex, const uint64_t& lastLogTerm, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(RaftService::REQUESTVOTE, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << term;
            *__cl__ << candidateId;
            *__cl__ << lastLogIndex;
            *__cl__ << lastLogTerm;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 RequestVote(const uint64_t& term, const siteid_t& candidateId, const uint64_t& lastLogIndex, const uint64_t& lastLogTerm, uint64_t* returnTerm, bool_t* vote_granted) {
        rrr::Future* __fu__ = this->async_RequestVote(term, candidateId, lastLogIndex, lastLogTerm);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *returnTerm;
            __fu__->get_reply() >> *vote_granted;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_AppendEntriesCombined(const siteid_t& candidateId, const uint64_t& prevLogIndex, const uint64_t& prevLogTerm, const uint64_t& logTerm, const uint64_t& currentTerm, const uint64_t& leaderCommitIndex, const uint64_t& isHeartbeat, const MarshallDeputy& cmd, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(RaftService::APPENDENTRIESCOMBINED, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << candidateId;
            *__cl__ << prevLogIndex;
            *__cl__ << prevLogTerm;
            *__cl__ << logTerm;
            *__cl__ << currentTerm;
            *__cl__ << leaderCommitIndex;
            *__cl__ << isHeartbeat;
            *__cl__ << cmd;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 AppendEntriesCombined(const siteid_t& candidateId, const uint64_t& prevLogIndex, const uint64_t& prevLogTerm, const uint64_t& logTerm, const uint64_t& currentTerm, const uint64_t& leaderCommitIndex, const uint64_t& isHeartbeat, const MarshallDeputy& cmd, uint64_t* followerLogSize, uint64_t* returnTerm, bool_t* followerAppendOK) {
        rrr::Future* __fu__ = this->async_AppendEntriesCombined(candidateId, prevLogIndex, prevLogTerm, logTerm, currentTerm, leaderCommitIndex, isHeartbeat, cmd);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *followerLogSize;
            __fu__->get_reply() >> *returnTerm;
            __fu__->get_reply() >> *followerAppendOK;
        }
        __fu__->release();
        return __ret__;
    }
    rrr::Future* async_HelloRpc(const std::string& req, const rrr::FutureAttr& __fu_attr__ = rrr::FutureAttr()) {
        rrr::Future* __fu__ = __cl__->begin_request(RaftService::HELLORPC, __fu_attr__);
        if (__fu__ != nullptr) {
            *__cl__ << req;
        }
        __cl__->end_request();
        return __fu__;
    }
    rrr::i32 HelloRpc(const std::string& req, std::string* res) {
        rrr::Future* __fu__ = this->async_HelloRpc(req);
        if (__fu__ == nullptr) {
            return ENOTCONN;
        }
        rrr::i32 __ret__ = __fu__->get_error_code();
        if (__ret__ == 0) {
            __fu__->get_reply() >> *res;
        }
        __fu__->release();
        return __ret__;
    }
};

} // namespace janus



