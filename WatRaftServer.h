#ifndef _WAT_RAFT_SERVER_H_
#define _WAT_RAFT_SERVER_H_

#include "WatRaft.h"
#include "WatRaftState.h"
#include <pthread.h>
#include <string>
#include <thrift/server/TThreadedServer.h>
#include <map>
#include <fstream>

namespace WatRaft {

class WatRaftConfig; // Forward declaration
typedef std::map<std::string, std::string> smMap;

struct key_val{
    std::string key;
    std::string val;
};

class WatRaftServer {
  public:
    WatRaftServer(int node_id, const WatRaftConfig* config) throw (int);
    WatRaftServer(int node_id, const WatRaftConfig* config, int client) throw (int);
    ~WatRaftServer();
  
    // Block and wait until the server shutdowns.
    int wait();
    // Set the RPC server once it is created in a child thread.
    void set_rpc_server(apache::thrift::server::TThreadedServer* server);
    int get_id() { return node_id; } 
    void timedout();
    void heartbeats();
    void candidateCall();
    void writeToFile(std::string input);
    void recoverState(int node_id, const WatRaftConfig* config);
    void addToState(std::string input);

    int node_id;     
    int32_t term;
    int32_t leader_id;
    int32_t commit_index;
    int32_t last_log_index;
    int32_t last_log_term;
    int32_t prev_log_index;
    int32_t prev_log_term;
    int32_t numServers;
    bool votedForCurTerm;
    bool heartbeat;
    int32_t votesCollected;
    int32_t majority;
    std::vector<Entry> entries;
    WatRaftState wat_state;   // Tracks the current state of the node.
    smMap sm;
    std::vector<key_val> puts;

  private:
    
    apache::thrift::server::TThreadedServer* rpc_server;
    const WatRaftConfig* config;
    pthread_t rpc_thread;
    pthread_t time_out;
    pthread_t leader;
    static const int num_rpc_threads = 64;
    static void* start_rpc_server(void* param);
    static void* start_time_out(void* param);
    static void* send_append(void* param);



};
} // namespace WatRaft

#endif
