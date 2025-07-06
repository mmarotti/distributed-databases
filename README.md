# Distributed Databases Project

## Setup Instructions

### 1. Install Python Requirements

```bash
pip install -r requirements.txt
```

### 2. Start Hive with Docker Compose

```bash
docker-compose up -d
```

This will start the Hive server and mount the `./data` directory into the container.

### 3. Load Data into Hive

Run the script to create the database and load the CSV data:

```bash
cd scripts/create_database_and_load
bash run_create_database_and_load.sh
```

This will connect to Hive and load the data from the `/data` directory into the appropriate tables.