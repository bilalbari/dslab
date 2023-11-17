#include "../deptran/__dep__.h"
#include "server.h"
#include "../deptran/raft/server.h"
#include "client.h"
#define Log_info Log_debug

namespace janus {

static int volatile x1 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_MULTI_STRING,
                                     [] () -> Marshallable* {
                                       return new MultiStringMarshallable;
                                     });

KvServer::KvServer(uint64_t maxraftstate=0) {
    maxraftstate_ = maxraftstate;
}

KvServer::~KvServer() {
  /* Your code here for server teardown */

}

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
    std::recursive_mutex my_mutex;
    my_mutex.lock();
    RaftServer& raft_server = GetRaftServer();
    Log_info("KV Server %lu -> Starting, put for %lu",raft_server.site_id_,oid);
    auto s = make_shared<MultiStringMarshallable>();
    s->data_.push_back(to_string(oid));
    s->data_.push_back("put");
    s->data_.push_back(k);
    s->data_.push_back(v);
    std::shared_ptr<Marshallable> my_command(s);
    uint64_t index = 0,term = 0;
    uint64_t *pointer_to_index = &index, *pointer_to_term = &term;
    Log_info("KV Server %lu  -> Before checking start, put for %lu",raft_server.site_id_,oid);
    my_mutex.unlock();
    if(raft_server.Start(my_command, pointer_to_index, pointer_to_term))
    {
      Log_info("KV Server %lu  -> Inside start block, put for %lu",raft_server.site_id_,oid);
      my_mutex.lock();
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      my_waiting_requests[to_string(oid)] = ev;
      Log_info("KV Server %lu  -> Before wait, put for %lu",raft_server.site_id_,oid);
      my_mutex.unlock();
      ev->Wait(25000000);
      my_mutex.lock();
      Log_info("KV Server %lu -> After wait, put for %lu",raft_server.site_id_,oid);
      if(ev->status_ == Event::TIMEOUT)
      {
        Log_info("KV Server %lu  -> Sending timeout put for %lu",raft_server.site_id_,oid);
        my_mutex.unlock();
        return KV_TIMEOUT;
      }
      else
      {
        Log_info("KV Server %lu -> Sending success put for %lu",raft_server.site_id_,oid);
        my_mutex.unlock();
        return KV_SUCCESS;
      }
    }
    else
    {
      my_mutex.lock();
      Log_info("KV Server %lu -> Sending back no leader, put for %lu",raft_server.site_id_,oid);
      my_mutex.unlock();
      return KV_NOTLEADER;
    }
}

int KvServer::Append(const uint64_t& oid, 
                     const string& k,
                     const string& v) {
    std::recursive_mutex my_mutex;
    my_mutex.lock();
    RaftServer& raft_server = GetRaftServer();
    Log_info("KV Server %lu -> Starting, append for %lu",raft_server.site_id_,oid);
    auto s = make_shared<MultiStringMarshallable>();
    s->data_.push_back(to_string(oid));
    s->data_.push_back("append");
    s->data_.push_back(k);
    s->data_.push_back(v);
    std::shared_ptr<Marshallable> my_shared(s);
    uint64_t index = 0,term = 0;
    uint64_t *pointer_to_index = &index, *pointer_to_term = &term;
    Log_info("KV Server %lu  -> Before calling start, append for %lu",raft_server.site_id_,oid);
    my_mutex.unlock();
    if(raft_server.Start(my_shared,pointer_to_index,pointer_to_term))
    {
      my_mutex.lock();
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      my_waiting_requests[to_string(oid)] = ev;
      my_mutex.unlock();
      Log_info("KV Server %lu  -> Before wait, append for %lu",raft_server.site_id_,oid);
      ev->Wait(25000000);
      my_mutex.lock();
      Log_info("KV Server %lu  -> After wait, append for %lu",raft_server.site_id_,oid);
      if(ev->status_ == Event::TIMEOUT)
      {
        Log_info("KV Server %lu  -> Returning timed out,append for %lu",raft_server.site_id_,oid);
        my_mutex.unlock();
        return KV_TIMEOUT;
      }
      else
      {
        Log_info("KV Server %lu  -> Returning success, append for %lu",raft_server.site_id_,oid);
        my_mutex.unlock();
        return KV_SUCCESS;
      }
    }
    else
    {
      my_mutex.lock();
      Log_info("KV Server %lu  -> Returning not leader, append for %lu",raft_server.site_id_,oid);
      my_mutex.unlock();
      return KV_NOTLEADER;
    }
}

int KvServer::Get(const uint64_t& oid, 
                  const string& k,
                  string* v) {
    std::recursive_mutex my_mutex;
    my_mutex.lock();
    RaftServer& raft_server = GetRaftServer();
    Log_info("KV Server %lu  -> Starting get for %lu",raft_server.loc_id_,oid);
    auto s = make_shared<MultiStringMarshallable>();
    s->data_.push_back(to_string(oid));
    s->data_.push_back("get");
    s->data_.push_back(k);
    s->data_.push_back("get");
    Log_info("KV Server %lu  -> Marshallable created, get for %lu",raft_server.site_id_,oid);
    std::shared_ptr<Marshallable> my_shared(s);
    uint64_t index = 0,term = 0;
    uint64_t *pointer_to_index = &index, *pointer_to_term = &term;
    my_mutex.unlock();
    if(raft_server.Start(my_shared,pointer_to_index,pointer_to_term))
    {
      my_mutex.lock();
      Log_info("KV Server %lu  -> Inside start, get for %lu",raft_server.site_id_,oid);
      auto ev = Reactor::CreateSpEvent<IntEvent>();
      my_waiting_requests[to_string(oid)] = ev;
      my_mutex.unlock();
      Log_info("KV Server %lu  -> Before wait, get for %lu",raft_server.site_id_,oid);
      ev->Wait(25000000);
      my_mutex.lock();
      Log_info("KV Server %lu  -> After wait, get for %lu",raft_server.site_id_,oid);
      if(ev->status_ == Event::TIMEOUT)
      {
        Log_info("KV Server %lu  -> Sending time out, get for %lu",raft_server.site_id_,oid);
        my_mutex.unlock();
        return KV_TIMEOUT;
      }
      else
      {
        Log_info("KV Server %lu  -> Sending success response, get for %lu",raft_server.site_id_,oid);
        if(my_key_value_map.find(k)!=my_key_value_map.end())
          (*v) = my_key_value_map[k];
        my_mutex.unlock();
        return KV_SUCCESS;
      }
    }
    else
    {
      Log_info("KV Server %lu  -> Sending not leader, get for %lu",raft_server.site_id_,oid);
      return KV_NOTLEADER;
    }
}

void KvServer::OnNextCommand(Marshallable& m) {
    std::recursive_mutex my_mutex;
    my_mutex.lock();
    RaftServer& raft_server = GetRaftServer();
    Log_info("KV Server %lu  -> Inside OnNextCommand",raft_server.site_id_);
    auto v = (MultiStringMarshallable*)(&m);
    string oid = v->data_[0];
    uint64_t oid_int = stoull(oid);
    string command = v->data_[1];
    string key = v->data_[2];
    string value = v->data_[3];
    Log_info("KV Server %lu  -> OnNext, processing request %lu",raft_server.site_id_,oid_int);
    if(command == "put")
    {
      my_key_value_map[key] = value;
      Log_info("KV Server %lu  -> OnNext, Got put command for %lu",raft_server.site_id_,oid_int);
    }
    else if(command == "append")
    {
      Log_info("KV Server %lu  -> OnNext, Got append command for %lu",raft_server.site_id_,oid_int);
      if(my_key_value_map.find(key)!=my_key_value_map.end())
      {
        my_key_value_map[key] = my_key_value_map[key]+value;
      }
      else
        my_key_value_map[key] = value;
    }
    else
      Log_info("KV Server %lu  -> OnNext, Got get command for %lu",raft_server.site_id_,oid_int);
    if(raft_server.state=="leader" && my_waiting_requests.find(oid)!=my_waiting_requests.end())
    {
      if(my_waiting_requests[oid]->status_ != Event::TIMEOUT)
        my_waiting_requests[oid]->Set(1);
      my_waiting_requests.erase(oid);
      Log_info("KV Server %lu -> OnNext, Processed and deleted pending request %lu",raft_server.site_id_,oid_int);
    }
    my_mutex.unlock();
}

shared_ptr<KvClient> KvServer::CreateClient() {
  /* don't change this function */
  auto cli = make_shared<KvClient>();
  cli->commo_ = sp_log_svr_->commo_;
  verify(cli->commo_ != nullptr);
  uint32_t id = sp_log_svr_->site_id_;
  id = id << 16;
  cli->cli_id_ = id+cli_cnt_++; 
  return cli;
}

} // namespace janus;