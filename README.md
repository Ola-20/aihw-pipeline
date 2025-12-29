AIHW Hospital Admissions Data Pipeline
📌 Project Overview

This project implements an automated, cost-efficient data ingestion pipeline that extracts Australian Institute of Health and Welfare (AIHW) hospital admissions data, ingests it via Fivetran Webhooks, and stores it in Amazon S3 as analytics-ready Parquet files.

The pipeline is fully automated using GitHub Actions and includes version-based ingestion control to prevent duplicate data loads.


🏗️ Architecture Overview

Source: AIHW MyHospitals API

Orchestration: GitHub Actions

Ingestion: Fivetran Webhook Connector

Storage: Amazon S3 (Data Lake)

Format: Parquet

Update Strategy: Version-controlled ingestion using version_information.data_version


flowchart LR
    A[AIHW MyHospitals API] --> B[GitHub Actions]
    B --> C[Python Script<br/>aihw_to_webhook.py]
    C --> D[Fivetran Webhook]
    D --> E[Fivetran Processing]
    E --> F[Amazon S3 Data Lake]
    F --> G[Parquet Files]

    C --> H[data_version check]
    H -->|New Version| D
    H -->|No Change| I[Exit Pipeline]