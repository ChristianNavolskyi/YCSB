#!/bin/bash

./multi-db-runner.sh -w $1 -o ~/Documents/BachelorThesis/results -d neo4j -f neo4j.path -d orientdb -f orientdb.url -d apachejena -f outputdirectory -d sparksee -f sparksee.path -l ~/Documents/HdrHistogram/HistogramLogProcessor -t 3

