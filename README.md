## AIHW Hospital Admissions Data Pipeline
#### 📌 Project Overview

This project implements an automated, cost-efficient data ingestion pipeline that extracts Australian Institute of Health and Welfare (AIHW) hospital admissions data, ingests it via Fivetran Webhooks, and stores it in Amazon S3 as analytics-ready Parquet files.

The pipeline is fully automated using GitHub Actions and includes version-based ingestion control to prevent duplicate data loads.


#### 🏗️ Architecture Overview

Source: AIHW MyHospitals API

Orchestration: GitHub Actions

Ingestion: Fivetran Webhook Connector

Storage: Amazon S3 (Data Lake)

Format: Parquet

Update Strategy: Version-controlled ingestion using version_information.data_version


<img width="3506" height="440" alt="image" src="https://github.com/user-attachments/assets/6c40524b-f2aa-4507-a812-5340e7bb8223" />



#### 🔁 Data Flow

GitHub Actions triggers the pipeline (manual or scheduled).

Python script calls the AIHW API endpoint.

The script reads version_information.data_version.

If the version is new, records are sent in batches to the Fivetran webhook.

Fivetran processes events and loads them into S3 as Parquet files.

The latest data_version is saved back to the repository to prevent duplicate ingestion.


#### 🧠 Duplicate Prevention Logic

The pipeline uses the AIHW metadata field:

version_information.data_version

Example:

2025121002


This value uniquely identifies a dataset version published by AIHW.

If the version has not changed since the last run, the pipeline exits safely without loading data.


#### 📂 Repository Structure
aihw-pipeline/

├── .github/

│   └── workflows/

│       └── aihw_to_webhook.yml

├── aihw_to_webhook.py

├── last_data_version.txt

└── README.md

#### 📦 Output Data (S3)

Data is written to Amazon S3 in Parquet format:

s3://fivetran-aihw-raw/

└── aihw/

    └── webhooks/
    
        └── test_1/
        
            └── data/
            
                ├── part-00000.parquet
                
                └── _metadata files

#### 🚀 Key Features

Fully automated ingestion

Serverless orchestration (no always-on compute)

Secure webhook-based ingestion

Idempotent data loads (no duplicates)

Analytics-ready Parquet output

Free-tier friendly architecture




#### 🔮 Next Steps

Bronze → Silver → Gold transformations

Data quality checks

Power BI dashboards

Incremental partitioning by reporting period


##### Author

Ola Akinbola

Data Analytics & Engineering Portfolio Project
