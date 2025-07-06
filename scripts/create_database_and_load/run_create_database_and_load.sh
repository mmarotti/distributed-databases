#!/bin/bash

export PYTHONPATH=./../../

HOST="localhost"
PORT=10000
USER=hive

# Run the Python script with default parameters
python3 create_database_and_load.py --host $HOST --port $PORT --user $USER