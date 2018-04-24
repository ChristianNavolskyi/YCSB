#!/bin/bash

./multi-db-runner.sh -w $1 \
-o ~/Documents/BachelorThesis/results \
-d orientdb -f orientdb.url \
-d sparksee -f sparksee.path \
-l ~/Documents/HdrHistogram/HistogramLogProcessor \
-t 3

