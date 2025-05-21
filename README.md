# Parallelism on Databases Project

This setup demonstrates how to:

1. Run HiveServer2 in Docker
2. Connect using Python with PyHive

## Prerequisites
- Docker
- Python 3.7+
- **System Dependencies**:
  - Ubuntu/Debian: `sudo apt install libsasl2-dev python3-dev`
  - MacOS: `brew install cyrus-sasl`

## Quick Start

### 1. Start Hive Container
#### Start container in background
docker compose up -d

#### Wait 1-2 minutes for Hive to initialize (check logs if needed)
docker compose logs hive4

### 2. Run Python Code

#### Install required packages
pip install -r requirements.txt

#### Run
python main.py