#!/bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/

rm state*

gnome-terminal -e './server 1 servers.txt'
gnome-terminal -e './server 2 servers.txt'
gnome-terminal -e './server 3 servers.txt'
gnome-terminal -e './server 3 servers.txt client'