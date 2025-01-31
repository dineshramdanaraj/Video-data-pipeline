# Video Data Pipeline

This project implements a **video data processing pipeline** using a combination of modern tools and frameworks. The pipeline is designed to handle incoming video files, process them through multiple stages (bronze, silver, and gold layers), and finally store the processed data in a database while notifying users of updates. The architecture follows the **Medallion Architecture** for data transformation and leverages tools like **Apache Airflow**, **Kafka**, **Hamilton**, and **Graphviz** for orchestration, modularity, and visualization.

---

## Table of Contents
- [Video Data Pipeline](#video-data-pipeline)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
  - [Architecture](#architecture)
  - [Tools and Technologies](#tools-and-technologies)
  - [Setup and Installation](#setup-and-installation)
    - [Prerequisites](#prerequisites)
    - [Docker Images](#docker-images)
    - [Install Dependencies](#install-dependencies)
  - [Running the Pipeline](#running-the-pipeline)
  - [Directory Structure](#directory-structure)
  - [Environment Variables](#environment-variables)
  - [|------------------|------------------------------------------|-----------------------------------|](#-----------------------------------------------------------------------------------------------)
  - [Client.properties](#clientproperties)
  - [DAG Workflows](#dag-workflows)
    - [1. **Kafka Consumer DAG**](#1-kafka-consumer-dag)
    - [2. **Video Process DAG**](#2-video-process-dag)
  - [Note:](#note)

---

## Overview

The **Video Data Pipeline** is a robust system for processing video files through a series of stages:
1. **Bronze Layer**: Ingests raw video data from a Kafka producer.
2. **Silver Layer**: Processes the video files (e.g., quality checks, metadata extraction, derivative generation).
3. **Gold Layer**: Stores the processed data in a database and sends notifications to users.

The pipeline is orchestrated using **Apache Airflow**, with modular components built using **Hamilton** for better testing and maintainability. Kafka is used as a message broker to trigger the pipeline when new video files arrive.

---

## Architecture

The pipeline follows the **Medallion Architecture**, which consists of three layers:

1. **Bronze Layer**:
   - Ingests raw video data from Kafka.
   - Validates and extracts basic metadata.
   - Stores the raw data in a staging area.

2. **Silver Layer**:
   - Processes the video files (e.g., quality checks, resolution adjustments, metadata enrichment).
   - Generates derivative videos if required.
   - Updates the video metadata.

3. **Gold Layer**:
   - Stores the processed data in a PostgreSQL database.
   - Sends email notifications to users using **SMTPlib**.

---

## Tools and Technologies

- **Apache Airflow**: For orchestrating the macro-level DAG workflows.
- **Kafka**: For message brokering and triggering the pipeline.
- **Hamilton**: For building modular and testable micro-level DAGs.
- **Graphviz**: For visualizing DAG workflows.
- **Poetry**: For dependency management.
- **Ruff**: For linting and organizing the code.
- **Docker**: For containerizing Airflow and Kafka.
- **SMTPlib**: For sending email notifications.
- **PostgreSQL**: For storing processed data.

---

## Setup and Installation

### Prerequisites
1. Install **Graphviz**:
   ```bash
   sudo apt-get install graphviz
   ```
   In windows ( add environment variable and path to the graphviz setup file)
2. Install **Python** (>= 3.12).
3. Install **Kafka**:
   a. Apache Kafka
      - Download Kafka.
      - Follow the setup instructions in the official documentation.
      - Docker Image For Windows 
   b. Confluent Kafka
      - Set up Confluent Account
      - Create a KAFKA cluster on Confluent Cloud
      - Integrate it with the project with the help of client.properties file 
4. Install **Poetry**:
   ```bash
   pip install poetry
   ```

### Docker Images
- **Airflow**: Use the official Airflow Docker image.


### Install Dependencies
1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/video-data-pipeline.git
   cd video-data-pipeline
   ```
2. Install dependencies using Poetry:
   ```bash
   poetry install
   ```
3. Install Docker Images
   ```bash
   docker build
   ```
---

## Running the Pipeline

1. **Set Up Environment Variables**:
   - Create a `.env` file in the root directory and customize it according to your setup:
     ```plaintext
     PG_CONN_URI=postgresql://user:password@localhost:5432/video_db
     EMAIL_RECEIVER=user@example.com
     SMTP_SERVER=smtp.example.com
     SMTP_PORT=587
     SMTP_USER=your-email@example.com
     SMTP_PASSWORD=your-password
     ```

2. **Run the Sync Script**:
   - Generate temporary `.ipynb` files for testing:
     ```bash
     poetry run python sync-datalake-files.py
     ```

3. **Start Docker Containers**:
   - Start Airflow and Kafka using Docker Compose:
     ```bash
     docker-compose up
     ```

4. **Trigger the Pipeline**:
   - Send a Kafka message to trigger the pipeline:
     ```bash
     poetry run python producer.py
     ```

---

## Directory Structure

```
video-data-pipeline/
├── datalake/
│   ├── bronze_layer/         # Bronze layer modules
│   ├── silver_layer/         # Silver layer modules
│   ├── gold_layer/           # Gold layer modules
│   ├── common_func.py        # Common functions and utilities
├── dags/                     # Airflow DAG definitions
├── config/
│   ├── airflow.cfg           # Stores Airflow Configurations
├── tests/                    # Unit and integration tests
├── .env                      # Environment variables
├── sync-datalake-files.py    # Script to generate test files
├── producer.py               # Kafka producer script
├── poetry.lock               # Poetry lock file
├── pyproject.toml            # Poetry project configuration
├── README.md                 # Project documentation
```

---

## Environment Variables

| Variable         | Description                              | Example Value                     |
|------------------|------------------------------------------|-----------------------------------|
| `AIRFLOW_UID`    | Airflow Unique ID                        | `76888`                           |
| `PG_CONN_URI`    | PostgreSQL connection URI                | `postgresql://user:password@localhost:5432/video_db` |
| `EMAIL_RECEIVER` | Email address for notifications          | `user@example.com`               |
| `SMTP_SERVER`    | SMTP server for sending emails           | `smtp.example.com`               |
| `SMTP_PORT`      | SMTP server port                         | `587`                            |
| `SMTP_USER`      | SMTP username                            | `your-email@example.com`         |
| `SMTP_PASSWORD`  | SMTP password                            | `your-password`                  |
|------------------|------------------------------------------|-----------------------------------|
---
## Client.properties

 - For Confluent Kafka Setup
| Variable | Description | Example Value |
|----------|-------------|---------------|
| `bootstrap.servers` | Kafka broker addresses to connect to | `kafka:9092` |
| `security.protocol` | Communicate with Kafka brokers | `SASL_SSL` |
| `sasl.mechanisms` | SASL mechanism used for authentication | `PLAIN` |
| `sasl.username` | Username for SASL authentication | `your-username` |
| `sasl.password` | Password for SASL authentication | `your-password` |
| `client.id` | Unique identifier for the Kafka client | `AVD BVV` |


---
## DAG Workflows

### 1. **Kafka Consumer DAG**
   - Listens to Kafka for new video messages.
   - Triggers the **Video Process DAG** when a new video is detected.

### 2. **Video Process DAG**
   - Processes videos through the three layers:
     1. **Bronze Task**: Ingests and validates raw video data.
     2. **Silver Task**: Processes the video (quality checks, metadata extraction, etc.).
     3. **Gold Task**: Stores the data in PostgreSQL and sends notifications.

---


## Note: 
- The DAG images are generated in 01_dag.ipynb in each directory(Bronze, Silver, Gold) via Graphviz (for better visualization).
- The helper functions are created as a private functions, so that they aren't considered a DAG Task/Function.
- Update the dataclasses created for Pipeline in Airflow.cfg, to make sure Airflow DAG recognizes it.



