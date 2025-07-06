#!/bin/bash

export PYTHONPATH=./../../

# Database and Hive connection parameters
HOST="localhost"
PORT=10000
USER=hive
DATABASE="pol_route"
CONTAINER="hive4"
# Execution parameters
RUNS=5
CORES="1,2,4,6"
# Output directory for results
QUERIES_DIR="./../../queries"
OUTPUT_CSV="./../../results/execution_results.csv"

python3 run_queries.py \
  --host $HOST \
  --port $PORT \
  --user $USER \
  --queries_dir $QUERIES_DIR \
  --database $DATABASE \
  --output_csv $OUTPUT_CSV \
  --runs $RUNS \
  --cores $CORES \
  --container $CONTAINER
