
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/map.hpp>
#include <boost/serialization/vector.hpp>
#include "service.h"
#include "client.h"
#include "../kv/server.h"

namespace janus {

void ShardMasterServiceImpl::Join(const map<uint32_t, std::vector<uint32_t>>& gid_server_map, uint32_t* ret, rrr::DeferredReply* defer) {
  std::recursive_mutex my_mutex;
  my_mutex.lock();
  RaftServer& raft_server = GetRaftServer();
  Log_info("Shard Master %lu -> Got join request",raft_server.loc_id_);
  stringstream ss;
  boost::archive::text_oarchive oarch(ss);
  oarch << gid_server_map;
  MultiStringMarshallable m;
  auto s = make_shared<MultiStringMarshallable>();
  uint64_t tempRequestNumber = currentRequestNumber;
  Log_info("Shard Master %lu -> Assigned request %lu, join",raft_server.loc_id_,tempRequestNumber);
  currentRequestNumber++;
  Log_info("Shard Master %lu -> Updated currentRequestNumber to %lu",raft_server.loc_id_,currentRequestNumber);
  s->data_.push_back(to_string(tempRequestNumber));
  s->data_.push_back("join");
  s->data_.push_back(ss.str());
  std::shared_ptr<Marshallable> my_command(s);
  uint64_t index =0 ,term = 0;
  uint64_t *pointerToIndex = &index, *pointerToTerm = &term;
  my_mutex.unlock();
  Log_info("Shard Master %lu -> Calling start for request %lu",raft_server.loc_id_,tempRequestNumber);
  if(raft_server.Start(my_command, pointerToIndex, pointerToTerm))
  {
    my_mutex.lock();
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    my_waiting_requests[tempRequestNumber] = ev;
    my_mutex.unlock();
    Log_info("Shard Master %lu -> Before waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    ev->Wait(1000000);
    Log_info("Shard Master %lu -> After waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    my_mutex.lock();
    if(ev->status_ == Event::TIMEOUT)
    {
      Log_info("Shard Master %lu -> request %lu timed out",raft_server.loc_id_,tempRequestNumber);
      *ret = KV_TIMEOUT;
    }
    else
    {
      Log_info("Shard Master %lu -> request %lu successfully handled",raft_server.loc_id_, tempRequestNumber);
      *ret = KV_SUCCESS;
    }
    my_mutex.unlock();
  }
  else
  {
    *ret = KV_NOTLEADER;
    Log_info("Shard Master %lu -> Returning not leader for request %lu",raft_server.loc_id_,tempRequestNumber);
  }
  defer->reply();
}

void ShardMasterServiceImpl::Leave(const std::vector<uint32_t>& gids, uint32_t* ret, rrr::DeferredReply* defer) {
  std::recursive_mutex my_mutex;
  my_mutex.lock();
  RaftServer& raft_server = GetRaftServer();
  Log_info("Shard Master %lu -> Got Leave request",raft_server.loc_id_);
  stringstream ss;
  boost::archive::text_oarchive oarch(ss);
  oarch << gids;
  MultiStringMarshallable m;
  auto s = make_shared<MultiStringMarshallable>();
  uint64_t tempRequestNumber = currentRequestNumber;
  Log_info("Shard Master %lu -> Assigned request %lu, Leave",raft_server.loc_id_,tempRequestNumber);
  currentRequestNumber++;
  Log_info("Shard Master %lu -> Updated currentRequestNumber to %lu",raft_server.loc_id_,currentRequestNumber);
  s->data_.push_back(to_string(tempRequestNumber));
  s->data_.push_back("leave");
  s->data_.push_back(ss.str());
  std::shared_ptr<Marshallable> my_command(s);
  uint64_t index =0 ,term = 0;
  uint64_t *pointerToIndex = &index, *pointerToTerm = &term;
  my_mutex.unlock();
  Log_info("Shard Master %lu -> Calling start for request %lu",raft_server.loc_id_,tempRequestNumber);
  if(raft_server.Start(my_command, pointerToIndex, pointerToTerm))
  {
    my_mutex.lock();
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    my_waiting_requests[tempRequestNumber] = ev;
    my_mutex.unlock();
    Log_info("Shard Master %lu -> Before waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    ev->Wait(1000000);
    Log_info("Shard Master %lu -> After waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    my_mutex.lock();
    if(ev->status_ == Event::TIMEOUT)
    {
      Log_info("Shard Master %lu -> request %lu timed out",raft_server.loc_id_,tempRequestNumber);
      *ret = KV_TIMEOUT;
    }
    else
    {
      Log_info("Shard Master %lu -> request %lu successfully handled",raft_server.loc_id_);
      *ret = KV_SUCCESS;
    }
    my_mutex.unlock();
  }
  else
  {
    *ret = KV_NOTLEADER;
    Log_info("Shard Master %lu -> Returning not leader for request %lu",raft_server.loc_id_,tempRequestNumber);
  }
  defer->reply();
}
void ShardMasterServiceImpl::Move(const int32_t& shard, const uint32_t& gid, uint32_t* ret, rrr::DeferredReply* defer) {
  std::recursive_mutex my_mutex;
  my_mutex.lock();
  RaftServer& raft_server = GetRaftServer();
  Log_info("Shard Master %lu -> Got move request",raft_server.loc_id_);
  MultiStringMarshallable m;
  auto s = make_shared<MultiStringMarshallable>();
  uint64_t tempRequestNumber = currentRequestNumber;
  Log_info("Shard Master %lu -> Assigned request number %lu, move",raft_server.loc_id_,tempRequestNumber);
  currentRequestNumber++;
  Log_info("Shard Master %lu -> Updated currentRequestNumber to %lu",raft_server.loc_id_,currentRequestNumber);
  s->data_.push_back(to_string(tempRequestNumber));
  s->data_.push_back("move");
  s->data_.push_back(to_string(shard));
  s->data_.push_back(to_string(gid));
  std::shared_ptr<Marshallable> my_command(s);
  uint64_t index =0 ,term = 0;
  uint64_t *pointerToIndex = &index, *pointerToTerm = &term;
  my_mutex.unlock();
  Log_info("Shard Master %lu -> calling start for request %lu",raft_server.loc_id_,tempRequestNumber);
  if(raft_server.Start(my_command, pointerToIndex, pointerToTerm))
  {
    my_mutex.lock();
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    my_waiting_requests[tempRequestNumber] = ev;
    my_mutex.unlock();
    Log_info("Shard Master %lu -> Before waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    ev->Wait(1000000);
    Log_info("Shard Master %lu -> After waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    my_mutex.lock();
    if(ev->status_ == Event::TIMEOUT)
    {
      Log_info("Shard Master %lu -> Request %lu timed out",raft_server.loc_id_,tempRequestNumber);
      *ret = KV_TIMEOUT;
    }
    else
    {
      Log_info("Shard Master %lu -> Request %lu successfully handled",raft_server.loc_id_,tempRequestNumber);
      *ret = KV_SUCCESS;
    }
    my_mutex.unlock();
  }
  else
  {
    *ret = KV_NOTLEADER;
    Log_info("Shard Master %lu -> Returning not leader for request %lu",raft_server.loc_id_,tempRequestNumber);
  }
  defer->reply();
}

void ShardMasterServiceImpl::Query(const int32_t& config_no, uint32_t* ret, ShardConfig* config, rrr::DeferredReply* defer) {
  std::recursive_mutex my_mutex;
  my_mutex.lock();
  RaftServer& raft_server = GetRaftServer();
  Log_info("Shard Master %lu -> Got query request",raft_server.loc_id_);
  MultiStringMarshallable m;
  auto s = make_shared<MultiStringMarshallable>();
  uint64_t tempRequestNumber = currentRequestNumber;
  Log_info("Shard Master %lu -> Assigned request number %lu, query",raft_server.loc_id_,tempRequestNumber);
  currentRequestNumber++;
  s->data_.push_back(to_string(tempRequestNumber));
  s->data_.push_back("query");
  s->data_.push_back(to_string(config_no));
  std::shared_ptr<Marshallable> my_command(s);
  uint64_t index =0 ,term = 0;
  uint64_t *pointerToIndex = &index, *pointerToTerm = &term;
  my_mutex.unlock();
  Log_info("Shard Master %lu -> Calling start for request %lu",raft_server.loc_id_,tempRequestNumber);
  if(raft_server.Start(my_command, pointerToIndex, pointerToTerm))
  {
    my_mutex.lock();
    auto ev = Reactor::CreateSpEvent<IntEvent>();
    my_waiting_requests[tempRequestNumber] = ev;
    my_mutex.unlock();
    Log_info("Shard Master %lu -> Before waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    ev->Wait(1000000);
    Log_info("Shard Master %lu -> After waiting for request %lu",raft_server.loc_id_,tempRequestNumber);
    my_mutex.lock();
    if(ev->status_ == Event::TIMEOUT)
    {
      Log_info("Shard Master %lu -> Request %lu timed out",raft_server.loc_id_,tempRequestNumber);
      *ret = KV_TIMEOUT;
    }
    else
    {
      Log_info("Shard Master %lu -> Request %lu successfully handled",raft_server.loc_id_,tempRequestNumber);
      if(config_no==-1 || config_no>currentConfiguration)
        *config = configs_[currentConfiguration];
      else
        *config = configs_[config_no];
      *ret = KV_SUCCESS;
    }
    my_mutex.unlock();
  }
  else
  {
    *ret = KV_NOTLEADER;
    Log_info("Shard Master %lu -> Returning not leader for request %lu",raft_server.loc_id_,tempRequestNumber);
  }
  defer->reply();
}

map<uint32_t,vector<uint32_t>> ShardMasterServiceImpl::GetServerToShardMapping()
{
  map<uint32_t,vector<uint32_t>> my_mapping;
  for(auto x:currentConfig.shard_group_map_)
  {
    my_mapping[x.second].push_back(x.first);
  }
  return my_mapping;
}

vector<uint32_t> ShardMasterServiceImpl::GetAllGroups()
{
  vector<uint32_t> allGroups;
  for(auto x:currentConfig.group_servers_map_)
  {
    allGroups.push_back(x.first);
  }
  return allGroups;
}

void ShardMasterServiceImpl::HandleJoin(map<uint32_t, vector<uint32_t>> gid_server_map)
{
  vector<uint32_t> incomingGroups;
  for(auto x:gid_server_map)
  {
    currentConfig.group_servers_map_[x.first] = x.second;
    incomingGroups.push_back(x.first);
  }
  if(currentConfiguration==0)
  {
    int currentTotalGroups = currentConfig.group_servers_map_.size();
    int startingGroup = 0;
    vector<uint32_t> allGroups = GetAllGroups();
    for(auto x:currentConfig.shard_group_map_)
    {
      currentConfig.shard_group_map_[x.first] = allGroups[startingGroup++];
      startingGroup%=currentTotalGroups;
    }
    currentConfiguration++;
    currentConfig.number = currentConfiguration;
    configs_[currentConfiguration] = currentConfig;
  }
  else
  {
    int currentTotalShards = currentConfig.shard_group_map_.size();
    int totalServers = currentConfig.group_servers_map_.size();
    int maxShardLimit = currentTotalShards/totalServers;
    map<uint32_t,vector<uint32_t>> serverToShardMapping = GetServerToShardMapping();
    vector<uint32_t> extraShards;
    for(auto x:serverToShardMapping)
    {
      if(x.second.size()>maxShardLimit)
      {
        for(int i=maxShardLimit;i<x.second.size();i++)
          extraShards.push_back(x.second[i]);
      }
    }
    int startingGroup = 0;
    int incomingGroupTotal = incomingGroups.size();
    for(auto x:extraShards)
    {
      currentConfig.shard_group_map_[x] = incomingGroups[startingGroup++];
      startingGroup%=incomingGroupTotal;
    }
    currentConfiguration++;
    currentConfig.number = currentConfiguration;
    configs_[currentConfiguration] = currentConfig;
  }
}

void ShardMasterServiceImpl::HandleLeave(vector<uint32_t> gids)
{
  vector<uint32_t> remainingServers;
  vector<uint32_t> redistributedShards;
  unordered_map<uint32_t,uint32_t> leavingServers;
  map<uint32_t,vector<uint32_t>> serverToShardMapping = GetServerToShardMapping();
  for(auto x: gids)
  {  
    leavingServers[x] = 1;
    currentConfig.group_servers_map_.erase(x);
  }
  for(auto x:serverToShardMapping)
  {
    if(leavingServers.find(x.first)!=leavingServers.end())
    {
      for(auto y:x.second)
        redistributedShards.push_back(y);
    }
    else
      remainingServers.push_back(x.first);
  }
  int startingGroup = 0;
  int totalRemainingGroups = remainingServers.size();
  for(auto x:redistributedShards)
  {
    currentConfig.shard_group_map_[x] = remainingServers[startingGroup++];
    startingGroup%=totalRemainingGroups;
  }
  currentConfiguration++;
  currentConfig.number = currentConfiguration;
  configs_[currentConfiguration] = currentConfig;
}

void ShardMasterServiceImpl::HandleQuery(uint32_t config_no)
{
  Log_info("Shard Master -> Successfully processed query response for request");   
}

void ShardMasterServiceImpl::HandleMove(int32_t shard,uint32_t gid)
{
  Log_info("Shard Master -> Successfully processed move response");
}

void ShardMasterServiceImpl::OnNextCommand(Marshallable& m) {
  std:recursive_mutex my_mutex;
  my_mutex.lock();
  RaftServer& raftServer = GetRaftServer();
  Log_info("Shard Master %lu -> Inside OnNextCommand",raftServer.loc_id_);
  auto v = (MultiStringMarshallable*)(&m);
  string id = v->data_[0];
  uint64_t id_int = stoull(id);
  Log_info("Shard Master %lu -> Inside OnNextCommand. Starting processing for request %lu",raftServer.loc_id_,id_int);
  string command = v->data_[1];
  if(command == "join")
  {
    Log_info("Shard Master %lu -> Inside OnNextCommand. Got Join, for request %lu",raftServer.loc_id_,id_int);
    stringstream ss2;
    ss2 << v->data_[2];
    boost::archive::text_iarchive iarch(ss2);
    map<uint32_t, std::vector<uint32_t>> new_map;
    iarch >> new_map;
    HandleJoin(new_map);
  }
  else if(command == "leave")
  {
    Log_info("Shard Master %lu -> Inside OnNextCommand. Got Leave, for request %lu",raftServer.loc_id_,id_int);
    stringstream ss2;
    ss2 << v->data_[2];
    boost::archive::text_iarchive iarch(ss2);
    vector<uint32_t> new_vector;
    iarch >> new_vector;
    HandleLeave(new_vector);
  }
  else if(command == "move")
  {
    Log_info("Shard Master %lu -> Inside OnNextCommand. Got Move, for request %lu",raftServer.loc_id_,id_int);
    uint32_t shard = stoull(v->data_[2]);
    uint32_t gid = stoull(v->data_[3]);
    HandleMove(shard,gid);
  }
  else
  {
    Log_info("Shard Master %lu -> Inside OnNextCommand. Got Query, for request %lu",raftServer.loc_id_,id_int);
    uint32_t configNo = stoull(v->data_[2]);
    HandleQuery(configNo);
  }
  if(raftServer.state=="leader" && my_waiting_requests.find(id_int)!=my_waiting_requests.end())
  {
    if(my_waiting_requests[id_int]->status_ != Event::TIMEOUT)
      my_waiting_requests[id_int]->Set(1);
    my_waiting_requests.erase(id_int);
    Log_info("Shard Master %lu -> Inside OnNext, Processed and deleted request %lu",raftServer.loc_id_,id_int);
  }
  my_mutex.unlock();
}

// do not change anything below
shared_ptr<ShardMasterClient> ShardMasterServiceImpl::CreateClient() {
  auto cli = make_shared<ShardMasterClient>();
  cli->commo_ = sp_log_svr_->commo_;
  uint32_t id = sp_log_svr_->site_id_;
  return cli;
}

} // namespace janus