# raft

raft consensus with thrift, complete with file persistence

thrift code (.thrift files) were provided by Dr Bernard Wong (uwaterloo, course cs 854)

precursor to our rdma based consesus protocol 

Config Files are used to determine the network setup
Handler is used to define behaviour of the protocol
Server is used to handle network calls and interactions
State is used for maintaing state for handler logic

All commented out printfs were used for live demostration and the code was tested for correctness with separate test code