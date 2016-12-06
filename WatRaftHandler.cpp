#include "WatRaftHandler.h"
#include <string>
#include <vector>
#include <sstream>

#include "WatRaftServer.h"

namespace WatRaft {

WatRaftHandler::WatRaftHandler(WatRaftServer* raft_server) : server(raft_server) {
  // Your initialization goes here
}

WatRaftHandler::~WatRaftHandler() {}

void WatRaftHandler::get(std::string& _return, const std::string& key) {
    // Your implementation goes here
    printf("get\n");
    if (this->server->node_id == this->server->leader_id) {
        
        std::map<std::string, std::string>::iterator search = this->server->sm.find(key);
        if (search != this->server->sm.end()) {
            // found existing key
            _return = search->second;
        } else {
            // not found
          printf("Key not found\n");
          WatRaftException err;
          err.error_code = WatRaftErrorType::KEY_NOT_FOUND;
          err.error_message = "key not available";
          throw err;
        }
    } else {
      printf("put called on NOT LEADER, throw appropiate error\n");
      WatRaftException err;
      if (this->server->leader_id == -1) {
        err.error_code = WatRaftErrorType::LEADER_NOT_AVAILABLE;  
        err.error_message = "leader not available, try again\n";
      } else if (this->server->leader_id != this->server->node_id) {
        err.error_code = WatRaftErrorType::NOT_LEADER;  
        err.error_message = "not leader, try again with correct leader\n";
        err.__isset.node_id = true;
        err.node_id = this->server->leader_id;
      }
      throw err;
    }

}

void WatRaftHandler::put(const std::string& key, const std::string& val) {
    // Your implementation goes here
    printf("put\n");
    if (this->server->node_id == this->server->leader_id) {
      printf("put called on leader add to list of sending message\n");
      key_val kv;
      kv.key = key;
      kv.val = val;
      this->server->puts.push_back(kv);
    } else {
      printf("put called on NOT LEADER, throw appropiate error\n");
      WatRaftException err;
      if (this->server->leader_id == -1) {
        err.error_code = WatRaftErrorType::LEADER_NOT_AVAILABLE;  
        err.error_message = "leader not available, try again\n";
      } else if (this->server->leader_id != this->server->node_id) {
        err.error_code = WatRaftErrorType::NOT_LEADER;  
        err.error_message = "not leader, try again with correct leader\n";
        err.__isset.node_id = true;
        err.node_id = this->server->leader_id;
      }
      throw err;
    }
}    

void WatRaftHandler::append_entries(AEResult& _return,
                                    const int32_t term,
                                    const int32_t leader_id,
                                    const int32_t prev_log_index,
                                    const int32_t prev_log_term,
                                    const std::vector<Entry> & entries,
                                    const int32_t leader_commit_index) 
{
    // Your implementation goes here
    printf("apppend entries or heartbeat called\n");
    if (term < this->server->term) {
      _return.term = this->server->term;
      _return.success = false;
      // return _return;
    } else {

      server->heartbeat = true;
      server->votedForCurTerm = false;
      server->term = term;
      server->leader_id = leader_id;
      
      if (entries.size() == 0) {
        // no values in yet
      _return.term = this->server->term;
      _return.success = true;
      //do we agree on last entries
      } else if (prev_log_index == this->server->last_log_index && prev_log_term == this->server->last_log_term) {
        // simply update my entries to match his from this point onwards
        int32_t sz = entries.size();

        for (int i=prev_log_index+1; i<sz; i++) {
          // printf("in loop, i is %d\n", i);
          this->server->entries.push_back(entries[i]);

          std::ostringstream ss;
          ss << entries[i].term;
          std::string str = ss.str();

          this->server->writeToFile(" " +str+ " " + entries[i].key+ " " + entries[i].val + "\n" );
        }
        
        this->server->last_log_index = entries.size()-1;
        this->server->last_log_term = entries[entries.size()-1].term;
        this->server->prev_log_index = entries.size()-1;
        this->server->prev_log_term = entries[entries.size()-1].term;
        
        if (this->server->commit_index < leader_commit_index) {
          int sz2 = entries.size();
          for (int i = this->server->commit_index+1; i<sz2; i++) {
              // add work here later
              Entry e = entries[i];
              std::map<std::string, std::string>::iterator search = this->server->sm.find(e.key);
              if (search != this->server->sm.end()) {
                  // found existing key
                  this->server->sm.erase(e.key);
                  this->server->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
              } else {
                  this->server->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
              }
              this->server->writeToFile("c\n" );
          }
        }

        this->server->commit_index = leader_commit_index;

        _return.term = this->server->term;
        _return.success = true;

        // return _return;

      } else {
        // i do not agree with him
        if (prev_log_index <= this->server->last_log_index) {
            int start_index;
          
          // if (this->server->entries[this->server->last_log_index].term == prev_log_term) {
          //     start_index = prev_log_index+1;
          // } else {
              start_index = prev_log_index+1;    
          // }

          // i have fonud the correct index
                  int32_t sz = entries.size();
                  for (int i=start_index; i<sz; i++) {
                    this->server->entries.push_back(entries[i]);
                    std::ostringstream ss;
                    ss << entries[i].term;
                    std::string str = ss.str();

                    this->server->writeToFile(" " +str+ " " + entries[i].key+ " " + entries[i].val + "\n" );
                  }

                  this->server->last_log_index = entries.size()-1;
                  this->server->last_log_term = entries[prev_log_index].term;

                  this->server->prev_log_index = entries.size()-1;
                  this->server->prev_log_term = entries[entries.size()-1].term;

                  if (this->server->commit_index < leader_commit_index) {
                  int sz2 = entries.size();
                  for (int i = this->server->commit_index+1; i<sz2; i++) {
                      // add work here later
                      Entry e = entries[i];
                      std::map<std::string, std::string>::iterator search = this->server->sm.find(e.key);
                      if (search != this->server->sm.end()) {
                          // found existing key
                          this->server->sm.erase(e.key);
                          this->server->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
                      } else {
                          this->server->sm.insert(std::pair<std::string, std::string>(e.key, e.val));
                      }
                      this->server->writeToFile("c\n" );
                  }
                }                   

                  this->server->commit_index = leader_commit_index;

                  _return.term = this->server->term;
                  _return.success = true;

                  // return _return;

        } else {
          printf("recovery called, trace to index\n");
          _return.term = this->server->term;
          _return.success = false;
        }
      }
    }
}


void WatRaftHandler::request_vote(RVResult& _return,
                                  const int32_t term,
                                  const int32_t candidate_id,
                                  const int32_t last_log_index,
                                  const int32_t last_log_term) {
    // Your implementation goes here
    printf("request_vote for term %d %d\n", term, this->server->term);

    bool check;

    if (this->server->term > term || this->server->last_log_index > last_log_index) {
      check =  false;
    }
    check =  true;

    if ((term > this->server->term) && check && !this->server->votedForCurTerm) {
      this->server->heartbeat = true;
      printf("voting +vely\n");
      // this->server->term = term;
      this->server->votedForCurTerm = true;
      _return.term = this->server->term;
      _return.vote_granted = true;
    } else {
      printf("voting -vely\n");
      _return.term = this->server->term;
      _return.vote_granted = false;
    }

}

void WatRaftHandler::debug_echo(std::string& _return, const std::string& msg) {
    _return = msg;
    printf("debug_echo\n");
}
} // namespace WatRaft

