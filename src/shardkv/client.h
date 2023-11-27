
#pragma once

#include "../deptran/__dep__.h"
#include "shardkv_rpc.h"
#include "../shardmaster/client.h"
#define ff first
#define ss second
namespace janus {

class Communicator;
class ShardKvClient {
 public: 
  Communicator* commo_ = nullptr;
  shared_ptr<ShardMasterService> sms_log_svr_; 
  int leader_idx_ = 0;
  unordered_map<uint64_t, unordered_map<string,pair<string,string> > > myClientMap;
  recursive_mutex myMutex;
  uint32_t cli_id_{UINT32_MAX};
  uint64_t counter_{0};
  ShardKvClient()
  {
    commo_ = new Communicator();
  }

  ShardKvProxy& Proxy(siteid_t site_id);
  int Op(function<int(uint32_t*)>,string k);
  int Put(const string& k, const string& v);
  int Get(const string& k, string* v);
  int Append(const string& k, const string& v);

  uint64_t TxBegin();
  int TxPut(const uint64_t tx_id, const string& k, const string& v);
  int TxGet(const uint64_t tx_id, const string& k, string* v);
  int TxCommit(const uint64_t tx_id);
  int TxAbort(const uint64_t tx_id);
  
  uint64_t GetNextOpId() {
    verify(cli_id_ != UINT32_MAX);
    uint64_t ret = cli_id_;
    ret = ret << 32;
    counter_++;
    ret = ret + counter_; 
    return ret;
  }

  // lab_shard: do not change this function
  shardid_t Key2Shard(string key) {
    shardid_t shard = 0;
    verify(key.length()>0);
    char x = key[0];
    shard = x - '0';
	  shard %= 10; // N shards is 10
	  return shard;
  }

};

} // namespace janus