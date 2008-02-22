#!/bin/bash
echo "setting env"
export PYTHONPATH=~/lib/python2.5/site-packages/:~/svn/tools/python/:~/svn/tools/python/pythongrid:$PYTHONPATH
export LD_LIBRARY_PATH=/usr/local/sge/lib/lx26-amd64/

~/svn/tools/python/pythongrid/pythongrid.py $1 $2
