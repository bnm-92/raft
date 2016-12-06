#!/bin/bash

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib/

rm state*

gnome-terminal -e './server 1 servers2.txt'
gnome-terminal -e './server 2 servers2.txt'
gnome-terminal -e './server 3 servers2.txt'
gnome-terminal -e './server 4 servers2.txt'
gnome-terminal -e './server 5 servers2.txt'
