#include "WatRaftServer.h"
#include "WatRaft.h"
#include "WatRaftState.h"
#include "WatRaftHandler.h"
#include "WatRaftConfig.h"
#include <pthread.h>
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <transport/TSocket.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/server/TThreadedServer.h>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <sstream>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

using boost::shared_ptr;


namespace WatRaft {


void* WatRaftServer::send_append(void* param) 
{
    WatRaftServer* raft = static_cast<WatRaftServer*>(param);
    raft->wat_state.wait_e(WatRaftState::LEADER);
    printf("initiate leader protocol\n");

    while(true) 
    {
        raft->wat_state.change_state(WatRaftState::LEADER);
        // printf("send heartbeats\n");
        raft->leader_id = raft->node_id;
        // std::cout << raft->puts.size()<<std::endl;
        ServerMap::const_iterator it = raft->config->get_servers()->begin();
        int vote_granted = 1;
        
        for (; it != raft->config->get_servers()->end(); it++) {
            if (it->first == raft->node_id) {
                continue; // Skip itself
            } 

            boost::shared_ptr<TSocket> socket(
            new TSocket((it->second).ip, (it->second).port));
            boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                
            WatRaftClient client(protocol);
            
            try {           
                transport->open();
                AEResult _return;
                
                if (raft->puts.size() == 0) {
                    client.append_entries(_return, raft->term, raft->node_id, raft->prev_log_index, raft->prev_log_term, raft->entries, raft->commit_index);
                    // printf("puts were zero\n");

                    if (_return.term > raft->term) {
                        raft->wat_state.change_state(WatRaftState::FOLLOWER);
                    } else if (_return.success == false) {

                        // return failed , this isnt working fix it
                        // printf("lagging client\n");
                        AEResult _return2;
                        _return2.success = false;
                        int32_t prev_log_index_c = raft->prev_log_index; 
                        int32_t prev_log_term_c = raft->entries[prev_log_term_c].term;
                        while(!_return2.success) {
                            prev_log_index_c--; 
                            prev_log_term_c = raft->entries[prev_log_term_c].term;
                            client.append_entries(_return2, raft->term, raft->node_id, prev_log_index_c, prev_log_term_c, raft->entries, raft->commit_index);                            
                        }
                    }

                } else {
                    // printf("puts was nt zero\n");
                    std::vector<key_val>::iterator it = raft->puts.begin();
                    for (; it != raft->puts.end(); ++it) {
                        Entry entry;// = new Entry();
                        entry.term = raft->term;
                        entry.key = it->key;
                        entry.val = it->val;
                        raft->entries.push_back(entry);

                        std::ostringstream ss;
                        ss << entry.term;
                        std::string str = ss.str();

                        raft->writeToFile(" " +str+ " " + entry.key.c_str()+ " " + entry.val.c_str() + "\n" );
                    }
                    raft->puts.clear();

                    raft->last_log_index = raft->entries.size()-1;
                    raft->last_log_term = raft->entries[raft->entries.size()-1].term;

                    client.append_entries(_return, raft->term, raft->node_id, raft->prev_log_index, raft->prev_log_term, raft->entries, raft->commit_index);

                    // std::cout << _return.success << " reply" << std::endl;

                    if (_return.success) {
                        vote_granted++;
                        if (vote_granted >= raft->majority) {
                            // printf("achieved quorum, commit and update statemachine\n");
                            // apply to state machine and or increment commit index
                            int sz = raft->entries.size();
                            for (int i = raft->commit_index+1; i<sz; i++) {
                                // add work here later
                                Entry e = raft->entries[i];
                                std::map<std::string, std::string>::iterator search = raft->sm.find(e.key);
                                if (search != raft->sm.end()) {
                                    // found existing key
                                    raft->sm.erase(e.key);
                                    raft->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
                                } else {
                                    raft->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
                                }
                                // printf("committing\n");
                                raft->writeToFile("c\n" );
                            }
                            // update committed values
                            raft->commit_index = raft->entries.size()-1;
                        }
                    } else {
                        // return failed , this isnt working fix it
                        // printf("lagging client\n");
                        AEResult _return2;
                        _return2.success = false;
                        int32_t prev_log_index_c = raft->prev_log_index; 
                        int32_t prev_log_term_c = raft->entries[prev_log_term_c].term;
                        while(!_return2.success) {
                            prev_log_index_c--; 
                            prev_log_term_c = raft->entries[prev_log_term_c].term;
                            client.append_entries(_return2, raft->term, raft->node_id, prev_log_index_c, prev_log_term_c, raft->entries, raft->commit_index);                            
                        }
                    }
                }

                transport->close();
            } catch (TTransportException e) {
                // printf("Caught exception: %s\n", e.what());
            }
        }
        // printf("jump to updates prev %d\n", );
        raft->prev_log_index = raft->entries.size()-1;
        // printf("jump to updates prev %lu\n", raft->entries.size()-1);
        raft->prev_log_term = raft->entries[raft->entries.size()-1].term;
        // printf("end for, updating prev log entry to %d\n", raft->prev_log_index);
        // printf("\nprinting the log\n");
        std::vector<Entry>::iterator it2 = raft->entries.begin();
        for (; it2 != raft->entries.end(); ++it2) {
            Entry e;
            e.term = it2->term;
            e.key = it2->key;
            e.val = it2->val;
            printf("%s ", e.key.c_str());
            printf("%s\n", e.val.c_str());
        }

        // printf("wait to send next heartbeat\n");
        int rand = std::rand() % 1000000;
        int time = 1500000 + rand;
        usleep(time);

    }

    return NULL;
}

void WatRaftServer::candidateCall() {
    // start elections
    
    if (!votedForCurTerm) {
        // printf("candidate starting elections for term %d %d\n", this->term, this->votedForCurTerm);
        votedForCurTerm = true;
        this->term++;
        this->votesCollected = 1;
        this->leader_id = -1;
        
            ServerMap::const_iterator it = config->get_servers()->begin();
            for (; it != config->get_servers()->end(); it++) {
                if (it->first == node_id) {
                continue; // Skip itself
                } 

                boost::shared_ptr<TSocket> socket(
                new TSocket((it->second).ip, (it->second).port));
                boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
                boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                
                WatRaftClient client(protocol);
            
                try {
                    int32_t candidate_id = this->node_id;

                    if (this->entries.size() == 0) {
                        this->last_log_index = 0;
                        this->last_log_term = 0;
                    } else {
                        this->last_log_index = entries.size() - 1;
                        this->last_log_term = entries.at(entries.size()-1).term;
                    }

                    transport->open();
                    RVResult _return;
                    // client.debug_echo(remote_str, "Hello");
                    client.request_vote(_return, term, candidate_id, last_log_index, last_log_term);
                    // vote for myself
                    if (_return.vote_granted) {
                        // printf("got vote\n");
                        this->votesCollected++;
                        if (this->votesCollected >= this->majority) {
                            this->wat_state.change_state(WatRaftState::LEADER);
                            this->leader_id = this->node_id;
                        }
                    } else {
                        this->wat_state.change_state(WatRaftState::FOLLOWER);
                    }

                    transport->close();

                } catch (TTransportException e) {
                    // printf("Caught exception: %s\n", e.what());
                }
            }
    } else {
        this->wat_state.change_state(WatRaftState::FOLLOWER);
    }
}



void* WatRaftServer::start_time_out(void* param) {
    WatRaftServer* raft = static_cast<WatRaftServer*>(param);
    while (true) {

        raft->wat_state.wait_e(WatRaftState::FOLLOWER);
        int rand = std::rand() % 50000000;
        int time = 5000000 + rand;
        usleep(time);
        if (!raft->heartbeat) {
            // printf("Timed Out, change state to candidate\n");
            raft->wat_state.change_state(WatRaftState::CANDIDATE);
        }
            
        raft->heartbeat = false;
        // printf("\nprinting the log\n");
        // std::vector<Entry>::iterator it = raft->entries.begin();
        // for (; it != raft->entries.end(); ++it) {
        //     Entry e;
        //     e.term = it->term;
        //     e.key = it->key;
        //     e.val = it->val;
        //     printf("%s ", e.key.c_str());
        //     printf("%s\n", e.val.c_str());
        // }
    }
    return NULL;
}

int WatRaftServer::wait() {
    wat_state.wait_ge(WatRaftState::SERVER_CREATED);
    // Perhaps perform your periodic tasks in this thread.
    wat_state.change_state(WatRaftState::FOLLOWER);
    while(true) {
        wat_state.wait_e(WatRaftState::CANDIDATE);
        this->candidateCall();
        if (leader_id == -1) {
            wat_state.change_state(WatRaftState::FOLLOWER);    
        }
        
        votedForCurTerm = false;
    }

    pthread_join(rpc_thread, NULL);
    pthread_join(time_out, NULL);
    return 0;
}

void WatRaftServer::set_rpc_server(TThreadedServer* server) {
    rpc_server = server;
    wat_state.change_state(WatRaftState::SERVER_CREATED);
}

void* WatRaftServer::start_rpc_server(void* param) {
    WatRaftServer* raft = static_cast<WatRaftServer*>(param);
    shared_ptr<WatRaftHandler> handler(new WatRaftHandler(raft));
    shared_ptr<TProcessor> processor(new WatRaftProcessor(handler)); 
    // Get IP/port for this node
    IPPortPair this_node = 
        raft->config->get_servers()->find(raft->node_id)->second;
    shared_ptr<TServerTransport> serverTransport(
        new TServerSocket(this_node.ip, this_node.port));
    shared_ptr<TTransportFactory> transportFactory(
        new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    shared_ptr<ThreadManager> threadManager = 
        ThreadManager::newSimpleThreadManager(num_rpc_threads, 0);
    shared_ptr<PosixThreadFactory> threadFactory = 
        shared_ptr<PosixThreadFactory>(new PosixThreadFactory());
    threadManager->threadFactory(threadFactory);
    threadManager->start();
    TThreadedServer* server = new TThreadedServer(
        processor, serverTransport, transportFactory, protocolFactory);
    raft->set_rpc_server(server);
    server->serve();
    return NULL;
}

void WatRaftServer::writeToFile(std::string input) {

    std::ostringstream ss;
    ss << this->node_id;
    std::string str = ss.str();

    std::string name = "state" + str;
    // printf("writing to disk %s\n", name.c_str());
    std::ofstream file(name.c_str(), std::fstream::out | std::fstream::app);
    file << input;
    file.close();
}

void WatRaftServer::addToState(std::string input) {
    int32_t term; 
    bool commit = false;
    std::string val;
    std::string key; 

    if(input.at(input.length()-1) == 'c') {
        commit = true;
    }

    if (commit) {
        int sz2 = this->entries.size();
        for (int i = this->commit_index; i<sz2; i++) {
            Entry e = entries[i];
            std::map<std::string, std::string>::iterator search = this->sm.find(e.key);
            if (search != this->sm.end()) {
                // found existing key
                this->sm.erase(e.key);
                this->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
            } else {
                this->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
            }
        }
        this->commit_index++;
    } else {
        std::stringstream ss(input);

        ss >> term >> key >> val;

        Entry e;
        e.term = term;
        e.key = key;
        e.val = val;

        this->entries.push_back(e);
        this->term = term;
        this->prev_log_index = this->entries.size()-1; 
        this->prev_log_term = this->entries[this->entries.size()-1].term;
        this->last_log_index =  this->entries.size()-1; 
        this->last_log_term = this->entries[this->entries.size()-1].term;
    }
}

void WatRaftServer::recoverState(int node_id, const WatRaftConfig* config) {

    this->node_id = node_id;
    this->term = 0;
    this->numServers = config->get_servers()->size();
    this->majority = (this->numServers/2)+1;
    this->commit_index = 0;
    this->heartbeat = false;
    this->last_log_index = 0;
    this->last_log_term = 0;
    this->prev_log_index = 0;
    this->prev_log_term = 0;
    Entry e;
    e.term = 0;
    e.key = "";
    e.val = "";
    this->entries.push_back(e);

    std::ostringstream ss;
    ss << this->node_id;
    std::string str = ss.str();

    std::string name = "state" + str;
    if (std::ifstream(name.c_str())) {
        // printf("reading from file\n");
        std::string line;
        std::ifstream file(name.c_str());
        // printf("recovering from PERSISTENT STATE\n");
        while (std::getline(file, line)) {
            // printf("%s\n", line.c_str());
            addToState(line);
        }
        file.close();
    }
    return;
}

WatRaftServer::WatRaftServer(int node_id, const WatRaftConfig* config)
        throw (int) : node_id(node_id), rpc_server(NULL), config(config) {
    this->recoverState(node_id, config);
    int rc = pthread_create(&rpc_thread, NULL, start_rpc_server, this);
    int timeout = pthread_create(&time_out, NULL, start_time_out, this);
    int ldr = pthread_create(&leader, NULL, send_append, this);
    if (rc != 0) {
        throw rc; // Just throw the error code
    }
    if (timeout != 0) {
        throw timeout; // Just throw the error code
    }

    if (ldr != 0) {
        throw ldr; // Just throw the error code
    }

}

WatRaftServer::~WatRaftServer() {
    // printf("In destructor of WatRaftServer\n");
    delete rpc_server;
}


} // namespace WatRaft

using namespace WatRaft;

int main(int argc, char **argv) {
    

    if (argc < 3) {
        printf("Usage: %s server_id config_file\n", argv[0]);
        return -1;
    }

    WatRaftConfig config;
    config.parse(argv[2]);

    if (argc == 4) {
        std::string check = argv[3];
        if (check.compare("client") == 0) {
            while (true) {
                printf("\n-----------------------enter command and value, nodeid put key val or nodeid get key-----------------------\n");
                std::string input;

                if (!std::getline(std::cin, input)) { continue; }

                int node_id; 
                std::string command;
                std::string val;
                std::string key; 

                std::stringstream ss(input);
                ss >> node_id >> command;

                if (command.compare("get") == 0) {
                    ss >> key;
                } else {
                    ss >> key >> val;
                }

                // std::cout << node_id << " " << command << " " << key << " " << val << std::endl;

                //iterator over servers
                ServerMap::const_iterator it = config.get_servers()->begin();
                for (; it != config.get_servers()->end(); it++) {
                    if (it->first == node_id) {
                        // only issue command here
                        boost::shared_ptr<TSocket> socket(
                        new TSocket((it->second).ip, (it->second).port));
                        boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
                        boost::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
                        WatRaftClient client(protocol);

                        try {
                            transport->open();
                            
                            try {
                                if (command.compare("get") == 0) {
                                    client.get(val, key);
                                    printf("val returned %s\n", val.c_str());
                                } else if (command.compare("put") == 0) {
                                    client.put(key,val);
                                }
                            } catch (WatRaft::WatRaftException e) {
                                    printf("Caught exception: %s\n", e.what());
                            }

                            transport->close();
                            // Create a WatID object from the return value.
                            
                        } catch (TTransportException e) {
                            printf("Caught exception: %s\n", e.what());
                        }
                    }
                }
            }
        } else {
            printf("Usage: %s server_id config_file client\n", argv[0]);
            return -1;       
        }
    }

    try {
        WatRaftServer server(atoi(argv[1]), &config);
        server.wait(); // Wait until server shutdown.
    } catch (int rc) {
        printf("Caught exception %d, exiting\n", rc);
        return -1;
    }

    return 0;
}
