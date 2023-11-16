#include "../deptran/__dep__.h"
#include "server.h"
#include "../deptran/raft/server.h"
#include "client.h"

namespace janus {

int64_t ShardKvServer::GetNextOpId() {
  verify(sp_log_svr_);
  int64_t ret = sp_log_svr_->site_id_;
  ret = ret << 32;
  ret = ret + op_id_cnt_++; 
  return ret;
}

int ShardKvServer::Put(const uint64_t& oid, 
                  const string& k,
                  const string& v) {
    // lab_shard: fill in your code
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
      outstanding_requests_[to_string(oid)] = ev;
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

int ShardKvServer::Append(const uint64_t& oid, 
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
      outstanding_requests_[to_string(oid)] = ev;
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

int ShardKvServer::Get(const uint64_t& oid, 
                  const string& k,
                  string* v) {
    std::recursive_mutex my_mutex;
    my_mutex.lock();
    RaftServer& raft_server = GetRaftServer();
    Log_info("KV Server %lu  -> Starting get for %lu",raft_server.site_id_,oid);
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
      outstanding_requests_[to_string(oid)] = ev;
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
        if(kv_store_.find(k)!=kv_store_.end())
          (*v) = kv_store_[k];
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

void ShardKvServer::OnNextCommand(Marshallable& m) {
    // lab_shard: fill in your code
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
      kv_store_[key] = value;
      Log_info("KV Server %lu  -> OnNext, Got put command for %lu",raft_server.site_id_,oid_int);
    }
    else if(command == "append")
    {
      Log_info("KV Server %lu  -> OnNext, Got append command for %lu",raft_server.site_id_,oid_int);
      if(kv_store_.find(key)!=kv_store_.end())
      {
        kv_store_[key] = kv_store_[key]+value;
      }
      else
        kv_store_[key] = value;
    }
    else
      Log_info("KV Server %lu  -> OnNext, Got get command for %lu",raft_server.site_id_,oid_int);
    if(raft_server.state=="leader" && outstanding_requests_.find(oid)!=outstanding_requests_.end())
    {
      if(outstanding_requests_[oid]->status_ != Event::TIMEOUT)
        outstanding_requests_[oid]->Set(1);
      outstanding_requests_.erase(oid);
      Log_info("KV Server %lu -> OnNext, Processed and deleted pending request %lu",raft_server.site_id_,oid_int);
    }
    my_mutex.unlock();
}

shared_ptr<ShardKvClient> ShardKvServer::CreateClient(Communicator* comm) {
  auto cli = make_shared<ShardKvClient>();
  cli->commo_ = comm;
  verify(cli->commo_ != nullptr);
  static uint32_t id = 0;
  id++;
  cli->cli_id_ = id; 
  return cli;
}

} // namespace janus;