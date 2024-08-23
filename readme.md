# Algolia Data Engineering Challenge: Shopify Data Pipeline

## Overview

This project implements a data pipeline designed for the Algolia Integration team to process Shopify configuration data. It extracts CSV files from an S3 bucket, transforms the data by filtering and enhancing it, and loads the transformed data into a PostgreSQL database. The pipeline is designed with scalability in mind to handle higher volumes of data.

## Prerequisites

- Docker (recommended to configure with 4+ GB of memory, ideally 8 GB)
- Docker Compose v2.14.0+

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/lylehpoisson/algolia-interview.git
   cd algolia-interview
   ```

2. **Build the Docker images (make sure Docker Desktop is running first)**:
    ```bash
    docker-compose build
    ```

3. **Start the services**:
    ```
    docker-compose up -d
    ```

## Pipeline Description

The pipeline operates on a daily schedule and processes files from the specified S3 bucket `alg-data-public`. It processes data from April 1, 2019, to April 7, 2019, inclusive, processing files named `[YYYY-MM-DD].csv`.

### Steps

- **Extract**: Download the day's CSV file from the S3 bucket.
- **Transform**:
  - Filter out rows with an empty `application_id`.
  - Add a `has_specific_prefix` column set to true if `index_prefix` is not `shopify_`, otherwise false.
- **Load**: Insert the data into the PostgreSQL database, updating existing records as necessary.

## Directory Structure

```plaintext
.
├── Dockerfile
├── docker-compose.yml
├── dags
│   └── algolia_etl.py
│   └── sql
│       ├── create_table.sql
│       └── load_csv.sql
├── tests
│   ├── __init__.py
│   └── test_algolia_etl.py
├── airflow-logs
│   └── (directory for airflow's log files)
└── (additional supporting directories and files)
```

## Testing

Unit tests are provided to ensure the integrity and reliability of the pipeline. Run tests using the following command, after the Installation steps above:

```bash
docker exec -it algolia-interview-airflow-worker-1 bash -c "pytest"
```

## Usage

After starting the services, navigate to `http://localhost:8080` to access the Airflow UI. You can log in using the default username and password, which are both `airflow`. Please feel free to update these as necessary by setting values for the `_AIRFLOW_WWW_USER_USERNAME` and `_AIRFLOW_WWW_USER_PASSWORD` environment variables in your local copy of the `.env` file, but do not push these changes to this repo. 

Upon logging in, the DAG `algolia_etl` will be visible. Since the date range for automatic runs (April 1 through April 7, 2019) has passed , you should manually trigger the DAG to process past data. Here’s how:

1. Click on the DAG name `algolia_etl`.
2. Unpause the DAG by toggling the slider if it's not active.
3. Click the "Play" button to trigger the DAG manually.

This process is important for ensuring that the DAG processes the intended historical data. If you encounter any issues while triggering the DAG, verify that your environment variables are set correctly and consult the Airflow logs for detailed error messages.

## Maintenance and Logging

Logs are stored in the `airflow-logs` directory, organized by DAG ID and run ID, providing detailed information for debugging and verification purposes.

## Troubleshooting

If any issues are encountered with the DAG or database connection not showing up on the Airflow UI, please run 
```bash
docker-compose up -d
```
again, which will take care of this issue. Please feel free to send any questions or comments to lyle.h.poisson@gmail.com.