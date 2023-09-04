# Batch Processing : Data Pipeline for Recruitment Start Up

## Table of Contents
- [Batch Processing : Data Pipeline for Recruitment Start Up](#batch-processing--data-pipeline-for-recruitment-start-up)
  - [Table of Contents](#table-of-contents)
  - [1. Introduction](#1-introduction)
    - [Technologies used](#technologies-used)
  - [2. Implementation overview](#2-implementation-overview)
  - [3. Design](#3-design)
  - [4. Project Structure](#4-project-structure)
  - [5. Settings](#5-settings)
    - [Prerequisites](#prerequisites)
    - [Important note](#important-note)
    - [Running](#running)
  - [6. Implementation](#6-implementation)
    - [Import data from Input Data](#import-data-from-input-data)
    - [6.1 Generate recruitment data into Data Lake (Cassandra)](#61-generate-recruitment-data-into-data-lake-cassandra)
    - [6.2 Load data from PostgreSQL to Amazon Redshift](#62-load-data-from-postgresql-to-amazon-redshift)
  - [7. Visualize result](#7-visualize-result)
    - [Results](#results)


## 1. Introduction 
Data is collected from an start up company about their recruitment. We will build ETL pipelines which will transform raw data into actionable insights, store them in data lake (Cassandra), data warehouse (Mysql, Amazon Redshift) for enhanced data analytics capabilities.

### Technologies used
- Python
- Pyspark
- Cassandra
- Mysql
- Airflow
- AWS services : S3, Redshift (data warehouse)
- Docker
- Grafana

## 2. Implementation overview

Design data models for OLTP database (Cassandra) and data warehouse (MySQL, Amazon Redshift). Generate data from MySQL to Cassandra. Build an ETL pipeline to transform raw data from Cassandra and store them in S3 for staging. Then implement another ETL pipeline which process data from S3 and load them to Amazon Redshift for enhanced data analytics. Using Airflow to orchestrate pipeline workflow and Docker to containerize the project - allow for fast build, test, and deploy project.

<img src = assets/project_architect_overview.png alt = "Airflow conceptual view">

## 3. Design 
<div style="display: grid; grid-template-columns: auto">
    <img src=assets/data_warehouse.png alt="Data model" width="600" height="500">
    <p> <b> <i> Data model for Data Warehouse </i> </b> </p>
</div>
<br> <br>
<div style="display: grid; grid-template-columns: auto">
  <img src=assets/data_redshift.png alt="Data model for Redshift" width="600" height="500">
  <p> <b> <i> Data model for Redshift </i> </b> </p>
</div>
<br> <br>
<div style="display: grid; grid-template-columns: auto">
  <img src=assets/airflow_workflow.png alt="Airflow Workflow" width="900px" height="300px">
  <p"> <b> <i> Airflow workflow </i> </b> </p>
</div>


## 4. Project Structure

```bash
Recruitment_Project
├── airflow
│   ├── config
│   ├── dags
│   │   ├── create_redshift.py
│   │   ├── dags_setup.py
│   │   ├── ETL_Cassandra_S3.py
│   │   └── fake_data.py
│   ├── logs
│   │   ├── dag_processor_manager
│   │   │   └── dag_processor_manager.log
│   │   └── scheduler
│   │       ├── 2023-09-03
│   │       │   ├── dag (copy).py.log
│   │       │   ├── dag.py.log
│   │       │   └── dags_setup.py.log
│   │       └── latest -> /opt/airflow/logs/scheduler/2023-09-03
│   └── plugins
├── assets
│   ├── airflow_workflow.png
│   ├── data_redshift.png
│   ├── data_visualization.png
│   ├── data_warehouse.png
│   ├── ETL_Redshift.png
│   ├── Generate_Data.png
│   ├── grafana_config.png
│   └── project_architect_overview.png
├── docker-compose.yml
├── Input_Data
│   ├── data_cassandra
│   │   ├── search.csv
│   │   └── tracking.csv
│   └── data_mysql
│       ├── application.csv
│       ├── campaign.csv
│       ├── company.csv
│       ├── conversation.csv
│       ├── dev_user.csv
│       ├── events.csv
│       ├── group.csv
│       ├── job.csv
│       ├── job_location.csv
│       ├── master_city.csv
│       ├── master_major_category.csv
│       ├── master_minor_category.csv
│       ├── master_publisher.csv
│       ├── master_role.csv
│       ├── user_client_assignee.csv
│       └── user_role.csv
├── README.md
├── requirements.txt
└── Transformed_Data
    └── Transformed_Data.csv
```
<br>

## 5. Settings

### Prerequisites
- AWS account
- AWS Services(S3, Redshift)
- Docker
- Airflow

### Important note

<b> You must specify AWS credentials for each of the following </b>

- S3 access : Create an IAM user "S3-admin" with <b> S3FullAccess </b>
- Redshift access : Create an IAM user "Redshift-admin" with <b> RedshiftFulAccess </b> policy

### Running 
```
# Start docker containers on your local computer
docker compose up -d

# Add pip packages
pip install --no-cache-dir -r /requirements.txt
```

## 6. Implementation

### Import data from [Input Data](Input_Data/)

### 6.1 Generate recruitment data into Data Lake (Cassandra)

<img src=assets/Generate_Data.png alt="ETL psql" height="400">

<b> Airflow tasks </b>

```python
fake_data = PythonOperator(
    task_id = 'fake_data',
    python_callable = generating_dummy_data,
    op_args=[random.randint(1,20)]
)
```
<b> [Generate data file](airflow/dags/fake_data.py) </b>
<br>

### 6.2 Load data from PostgreSQL to Amazon Redshift
<img src=assets/ETL_Redshift.png alt="ETL redshift" weight="500px" height="250px">

<b> Airflow tasks </b>
  
```python
Extract = PythonOperator(
    task_id = 'extract_data_from_cassandra',
    python_callable = extract_data_from_cassandra,
)

Transform = PythonOperator(
    task_id = 'Transform_data',
    python_callable = process_result,
    op_args=[Extract.output]
)

Load = PythonOperator(
    task_id = 'Load_data_to_s3',
    python_callable = write_data_to_s3,
    op_args=[Transform.output]
)

create_redshift = PythonOperator (
    task_id = 'create_redshift_schema_and_copy_data_to_redshift',
    python_callable = create_redshift_schema
)
```
<br>

<b> 1. Extract : </b> Extract data from Cassandra database

<b> 2. Transform : </b> Transform craw data to result

<b> 3. Load : </b> Load data from Cassandra to S3 bucket

<b> 4. create_redshift : </b> Load data from S3 bucket to Redshift

<br>
## 7. Visualize result

Connect redshift to grafana and visualize results

<div style="display: flex; flex-direction: column;">
  <img src=assets/grafana_config.png alt="connect_grafana_redshift">
  <p> <b> <i> Connect to grafana </i> </b> </p>
</div>

### Results

<img src=assets/data_visualization.png>

<p><i>The self-generated data is a bit misleading :)) </i></p>