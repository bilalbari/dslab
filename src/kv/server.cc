#include "../deptran/__dep__.h"
#include "server.h"
#include "../deptran/raft/server.h"
#include "client.h"

namespace janus {

static int volatile x1 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_MULTI_STRING,
                                     [] () -> Marshallable* {
                                       return new MultiStringMarshallable;
                                     });

int64_t KvServer::GetNextOpId() {
  verify(sp_log_svr_);
  int64_t ret = sp_log_svr_->site_id_;
  ret = ret << 32;
  ret = ret + op_id_cnt_++; 
  return ret;
}

int KvServer::Put(const uint64_t& oid, 
                  const string& k,
                  const string& v) {
  /* 
  Your are recommended to use MultiStringMarshallable as the format 
  for the log entries. Here is an example how you can use it.
  auto s = make_shared<MultiStringMarshallable>();
  s->data_.push_back(to_string(op_id));
  s->data_.push_back("put");
  s->data_.push_back(k);
  s->data_.push_back(v);
  */
  /* your code here */
  Coroutine::CreateRun([=](){
    std::recursive_mutex my_mutex;
    auto s = make_shared<MultiStringMarshallable>();
    s->data_.push_back(to_string(oid));
    s->data_.push_back("put");
    s->data_.push_back(k);
    s->data_.push_back(v);
    Marshallable m = (Marshallable)(*s);
    std::shared_ptr<Marshallable> my_shared(&m);
    RaftServer& raft_server = GetRaftServer();
    uint64_t *index,*term;
    if(raft_server.Start(my_shared,index,term))
    {
      my_mutex.lock();
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      my_waiting_requests[to_string(oid)] = ev;
      my_mutex.unlock();
      ev->Wait(500000);
      if(ev->status_ == Event::TIMEOUT)
        return KV_TIMEOUT;
      else
        return KV_SUCCESS;
    }
    else
      return KV_NOTLEADER;
  });
}

int KvServer::Append(const uint64_t& oid, 
                     const string& k,
                     const string& v) {
  Coroutine::CreateRun([=](){
    std::recursive_mutex my_mutex;
    auto s = make_shared<MultiStringMarshallable>();
    s->data_.push_back(to_string(oid));
    s->data_.push_back("append");
    s->data_.push_back(k);
    s->data_.push_back(v);
    Marshallable m = (Marshallable)(*s);
    std::shared_ptr<Marshallable> my_shared(&m);
    RaftServer& raft_server = GetRaftServer();
    uint64_t *index,*term;
    if(raft_server.Start(my_shared,index,term))
    {
      my_mutex.lock();
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      my_waiting_requests[to_string(oid)] = ev;
      my_mutex.unlock();
      ev->Wait(500000);
      if(ev->status_ == Event::TIMEOUT)
        return KV_TIMEOUT;
      else
        return KV_SUCCESS;
    }
    else
      return KV_NOTLEADER;
  });
}

int KvServer::Get(const uint64_t& oid, 
                  const string& k,
                  string* v) {
  Coroutine::CreateRun([=](){
    std::recursive_mutex my_mutex;
    auto s = make_shared<MultiStringMarshallable>();
    s->data_.push_back(to_string(oid));
    s->data_.push_back("get");
    s->data_.push_back(k);
    s->data_.push_back("get");
    Marshallable m = (Marshallable)(*s);
    std::shared_ptr<Marshallable> my_shared(&m);
    RaftServer& raft_server = GetRaftServer();
    uint64_t *index,*term;
    if(raft_server.Start(my_shared,index,term))
    {
      my_mutex.lock();
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      my_waiting_requests[to_string(oid)] = ev;
      my_mutex.unlock();
      ev->Wait(500000);
      if(ev->status_ == Event::TIMEOUT)
        return KV_TIMEOUT;
      else
      {
        my_mutex.lock();
        *v = my_key_value_map[k];
        my_mutex.unlock();
        return KV_SUCCESS;
      }
    }
    else
      return KV_NOTLEADER;
  });
}

void KvServer::OnNextCommand(Marshallable& m) {
  Coroutine::CreateRun([=](){
    std::recursive_mutex my_mutex;
    my_mutex.lock();
    auto v = (MultiStringMarshallable*)(&m);
    string oid = v->data_[0];
    string command = v->data_[1];
    string key = v->data_[2];
    string value = v->data_[3];
    if(my_waiting_requests[oid]->status_ != Event::TIMEOUT)
    {
      if(command == "put")
        my_key_value_map[key] = value;
      else if(command == "append")
        my_key_value_map[key] = my_key_value_map[key]+value;
      my_waiting_requests[oid]->Set(1);
    }
    my_waiting_requests.erase(oid);
    my_mutex.unlock();
  });  
}

shared_ptr<KvClient> KvServer::CreateClient() {
  /* don't change this function */
  auto cli = make_shared<KvClient>();
  verify(commo_ != nullptr);
  cli->commo_ = commo_;
  uint32_t id = sp_log_svr_->site_id_;
  id = id << 16;
  cli->cli_id_ = id+cli_cnt_++; 
  return cli;
}

} // namespace janus;