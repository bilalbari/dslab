

#include "client.h"
#include "server.h"


namespace janus {

int ShardKvClient::Op(function<int(uint32_t*)> func,string k) {
  Log_info("Inside OP");
  uint64_t t1 = Time::now();
  ShardConfig *currentConfig = new ShardConfig();
  auto x = make_shared<ShardMasterClient>();
  Log_info("Successfully created ShardMaster Client");
  ShardMasterClient myClient = *x;
  uint32_t returnValue = 0;
  myClient.Query(-1, currentConfig);
  Log_info("Got shardConfig query result");
  shardid_t shard_id = Key2Shard(k);
  uint32_t myShardGID = currentConfig->shard_group_map_[shard_id];
  Log_info("Got GID as %ld",myShardGID);
  vector<uint32_t> myGIDServers = currentConfig->group_servers_map_[myShardGID];
  for(int i=0;i<myGIDServers.size();i++)
    Log_info("Servers %ld",myGIDServers[i]);
  Log_info("Got my maps from current config");
  while(true)
  {
    for(int i=0;i<myGIDServers.size();i++)
    {
      leader_idx_ = myGIDServers[i];
      Log_info("Checking for leader %d",leader_idx_);
      uint64_t t2 = Time::now();
      if (t2 - t1 > 100000000) {
        return KV_TIMEOUT;
      }
      uint32_t ret = 0;
      int r1; 
      r1 = func(&ret);
      if(r1 == ETIMEDOUT || ret == KV_TIMEOUT) {
        Log_info("Got KV Timeout for leader %d",leader_idx_);
        return KV_TIMEOUT;
      }
      if (ret == KV_SUCCESS) {
        Log_info("Got KV Success for leader %d",leader_idx_);
        return KV_SUCCESS;
      }
      if (ret == KV_NOTLEADER) {
        Log_info("Got KV Not Leader for leader %d",leader_idx_);
        continue;
      }
    }
    Log_info("Found no leader, sleeping for some time and trying again");
    usleep(1000000);
  }
  
  
}

int ShardKvClient::Put(const string& k, const string& v) {
  Log_info("Received put in ShardKV");
  return Op([&](uint32_t* r)->int{
    return Proxy(leader_idx_).Put(GetNextOpId(), k, v, r);
  },k);
}

ShardKvProxy& ShardKvClient::Proxy(siteid_t site_id) {
  verify(commo_);
  auto p = (ShardKvProxy*)commo_->rpc_proxies_.at(site_id);
  return *p; 
}

int ShardKvClient::Append(const string& k, const string& v) {
  return Op([&](uint32_t* r)->int{
    return Proxy(leader_idx_).Append(GetNextOpId(), k, v, r);
  },k);
}

int ShardKvClient::Get(const string& k, string* v) {
  return Op([&](uint32_t* r)->int{
    return Proxy(leader_idx_).Get(GetNextOpId(), k, r, v);
  },k);
}

ShardMasterClient ShardKvClient::CreateShardMasterClient(){
  auto p = make_shared<ShardMasterClient>();
  return *p;
}

} // namesapce janus;