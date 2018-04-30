#!/bin/bash

./multi-db-runner.sh -w $1 \
-o ~/Documents/BachelorThesis/results \
-d orientdb -f orientdb.url \
-l ~/Documents/HdrHistogram/HistogramLogProcessor \
-t 3
