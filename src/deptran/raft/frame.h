#pragma once

#include <mutex>
#include "../communicator.h"
#include "../frame.h"
#include "../constants.h"
#include "commo.h"
#include "server.h"
#include "service.h"

namespace janus {

class RaftFrame : public Frame {
 private:
  slotid_t slot_hint_ = 1;
  std::function<void()> restart_;
  RaftServiceImpl *service;
#ifdef RAFT_TEST_CORO
  static std::recursive_mutex raft_test_mutex_;
  static std::shared_ptr<Coroutine> raft_test_coro_;
  static uint16_t n_replicas_;
  static map<siteid_t, RaftFrame*> frames_;
  static bool all_sites_created_s;
  // static uint16_t n_commo_;
  static bool tests_done_;
#endif
 public:
  RaftFrame(int mode);
  RaftCommo *commo_ = nullptr;
  shared_ptr<Persister> persister = nullptr;
  /* TODO: have another class for common data */
  RaftServer *svr_ = nullptr;
  void SetRestart(function<void()> restart) override;
  void Restart() override;
  Executor *CreateExecutor(cmdid_t cmd_id, TxLogServer *sched) override;
  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
  TxLogServer *CreateScheduler() override;
  TxLogServer *RecreateScheduler() override;
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi) override;
};

} // namespace janus
