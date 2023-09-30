#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"
#include <chrono>
#include <ctime>
#include <thread>
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "raft_rpc.h"
#include "macros.h"
#include "../communicator.h"

using namespace std;

namespace janus {

#define HEARTBEAT_INTERVAL 100000

struct MY_LOG{
  string message;
  int term;
  MY_LOG(string my_message,int term_value){
    message = my_message;
    term = term_value;
  }
};

class RaftServer : public TxLogServer {
 public:
  

  uint64_t currentTerm;
  uint64_t votedFor;
  vector<MY_LOG*> stateLog;
  string state;
  chrono::time_point<chrono::system_clock> lastStartTime;
  uint64_t lastLogIndex;
  uint64_t lastLogTerm;
  
  public:

  int generateElectionTimeout(){
    // Selecting timeout bw 500ms to 1s as recommended by
    // professor
    srand(loc_id_);
    return 5+(rand()%5);
  }

  void sendHeartBeat(){
    mtx_.lock();
    uint64_t temp_term = currentTerm;
    mtx_.unlock();
    uint64_t return_term;
    commo()->SendEmptyAppendEntries(
                                    0,
                                    temp_term,
                                    loc_id_,
                                    &return_term
                                      );
    Log_info("Max Term From Heartbeat is %d",return_term);
    if(return_term != -1)
    {
      if(return_term > temp_term)
      {
        Log_info("Received a bigger term response from heartbeat, switching to follower");
        convertToFollower(return_term);
        return;
      }
    }              
  }

  void becomeLeader(){
    mtx_.lock();
    state = "leader";
    chrono::time_point<chrono::system_clock> last_heartbeat_time = chrono::system_clock::now();
    mtx_.unlock();
    while(true){
      chrono::duration<double,std::milli> time_since_heartbeat = chrono::system_clock::now()-last_heartbeat_time;
      /*
        Setting heartbeat interval to 100ms
      */
      
      if(time_since_heartbeat.count() >= 100){
        sendHeartBeat();
        last_heartbeat_time = chrono::system_clock::now();  
      }
      
      mtx_.lock();
      if(state != "leader")
      {
        mtx_.unlock();
        //convertToFollower();
        return;
      }
      else{
        mtx_.unlock();
        this_thread::sleep_for(chrono::milliseconds(20));
      }
    }
  }

  void becomeCandidate(){
    
    /*
      Getting random timeout for the sever
    */
    
    int electionTimeout = generateElectionTimeout();
    
    // chrono::milliseconds endTimeout(electionTimeout);
    Log_info("Candidate timeout for server %lli is %d",loc_id_,electionTimeout);
    chrono::seconds endTimeout(electionTimeout);

    /*
      Locking the mutex to get state variables
      before sending off RPC for request vote
    */
    uint64_t initial_term;
    int temp_last_log_index,temp_last_log_term;
    mtx_.lock();
    state = "candidate";
    currentTerm = currentTerm+1;
    //Log_info("Server %lli  started as candidate with term %lli",loc_id_,currentTerm);
    initial_term = currentTerm;
    lastStartTime = chrono::system_clock::now();;
    votedFor = loc_id_;
    temp_last_log_index = lastLogIndex;
    temp_last_log_term = lastLogTerm;
    mtx_.unlock();


    // chrono::duration<double,milli> time_spent = chrono::system_clock::now() - lastStartTime;
    //Log_info("Initialising time_spent for server %lli",loc_id_);
    chrono::duration<double> time_spent = chrono::system_clock::now() - lastStartTime;
    /*
    
    Make rpc call to all servers available
    and processing their replies till
    timeout

    */
    //Log_info("Entering while loop for sending RPC for server %lli",loc_id_);
    while(time_spent < endTimeout)
    {
      //part_id is 0 for lab 1
      uint64_t max_return_term=-1;
      uint64_t total_votes_received=0;
      commo()->SendRequestVote(
                                0,
                                initial_term, // sending my term
                                loc_id_,  //sending my id
                                temp_last_log_index, //my last log index
                                temp_last_log_term, // my last log term
                                &max_return_term,
                                &total_votes_received
                              );
      Log_info("Got reply from all servers with total vote count %d and max term %lli",total_votes_received,max_return_term);
      mtx_.lock();
      if(state!="candidate")
      {
        Log_info("Changed state to %s while waiting for votes",state);
        return;
      }
      mtx_.unlock();
      if(max_return_term!=-1) //skipping failed RPC's
      { 
      
        /*
          Checking for the return term
          and breaking in case it is more than current
          term
        */
        if(max_return_term > initial_term)
        {
          Log_info("Received bigger term after requestVote");
          convertToFollower(max_return_term); 
          return;               
        }
        else if(max_return_term == initial_term)
        {
          /*
          Checking for majority of votes received
          and changing to a leader in case that happens
          */
          if(total_votes_received > (commo()->rpc_par_proxies_[0].size()+1)/2)
          {
            Log_info("Election supremacy\n");
            becomeLeader();
            return;
          }
        }
      }
      this_thread::sleep_for(chrono::milliseconds(1000));
      mtx_.lock();
      /*
        Updating time diff
      */
      time_spent = chrono::system_clock::now() - lastStartTime;
      mtx_.unlock();
      Log_info("Time spent as candidate for server %lli is %f",loc_id_,time_spent.count());
    }  
    Log_info("Time elapsed since one election without result for %lli",loc_id_);
    becomeCandidate();
  }

  void runFollowerTimeout(){
    int electionTimeout = generateElectionTimeout();
    Log_info("Timeout for server %lli is %i",loc_id_,electionTimeout);
    using namespace std::chrono;
    chrono::seconds endTimeout(electionTimeout);
    mtx_.lock();
    uint64_t initial_term = currentTerm;
    //chrono::duration<double,milli> time_spent = chrono::system_clock::now() - lastStartTime;
    chrono::duration<double> time_spent = chrono::system_clock::now() - lastStartTime;
    mtx_.unlock();
    while(time_spent < endTimeout){
      mtx_.lock();
      if(initial_term != currentTerm){
        Log_info("Term updated. Exiting follower loop for server %lli",loc_id_);
        mtx_.unlock();
        return;
      }
      if(state!="follower"){
        Log_info("State changed to %s. Ending election timeout as a follower",state);
        mtx_.unlock();
        return;
      }
      //sleeping for 1 second
      // this_thread::sleep_for(chrono::milliseconds(20));
      mtx_.unlock();
      this_thread::sleep_for(chrono::milliseconds(1000));
      mtx_.lock();
      time_spent = chrono::system_clock::now() - lastStartTime;
      mtx_.unlock();
      Log_info("Time spent as follower %f for server %lli",time_spent.count(),loc_id_);
    }
    Log_info("Timeout completed as follower for %lli. Switching to candidate",loc_id_);
    //Election timeout finished as a follower
    //Changing to candidate
    becomeCandidate();
  }

  void convertToFollower(const uint64_t& term){
    Log_info("Started as follower for server %lli",loc_id_);
    votedFor = -1;
    mtx_.lock();
    lastStartTime = std::chrono::system_clock::now();
    currentTerm = term;
    state = "follower";
    mtx_.unlock();
    runFollowerTimeout();
  }
  /* do not modify this class below here */

 public:
  RaftServer(Frame *frame) ;
  ~RaftServer() ;

  bool Start(shared_ptr<Marshallable> &cmd, uint64_t *index, uint64_t *term);
  void GetState(bool *is_leader, uint64_t *term);

 private:
  bool disconnected_ = false;
	void Setup();

 public:
  void SyncRpcExample();
  void Disconnect(const bool disconnect = true);
  void Reconnect() {
    Disconnect(false);
  }
  bool IsDisconnected();

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };
  RaftCommo* commo() {
    return (RaftCommo*)commo_;
  }
};
} // namespace janus
