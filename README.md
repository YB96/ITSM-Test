# ITSM ETL

## Overview

This project demonstrates an end-to-end **IT Service Management (ITSM) data pipeline** using **Apache Airflow**, **DBT**, **Postgres**, and **Apache Superset**. The goal is to process, transform, and visualize ServiceNow ticket data to gain actionable insights into service delivery performance.

The workflow includes:

1. **Data Ingestion:** Loading raw ticket CSV data into Postgres.
2. **Data Transformation:** Cleaning, aggregating, and calculating metrics using DBT.
3. **Workflow Orchestration:** Using Airflow to schedule and manage the data pipeline.
4. **Visualization:** Creating interactive dashboards in Superset to monitor ticket trends, resolution times, closure rates, and backlog.

---

## Project Structure

ITSM-Test/
│
├─ dags/ # Airflow DAG files
│ └─ itsm_pipeline_dag.py
│
├─ dbt/ # DBT project
│ ├─ models/
│ │ ├─ clean_tickets.sql
│ │ ├─ avg_resolution_time.sql
│ │ ├─ closure_rate.sql
│ │ ├─ monthly_ticket_summary.sql
│ │ └─ schema.yml
│ └─ dbt_project.yml
│
├─ data/ # Input CSV files
│ └─ itsm_tickets.csv
│
├─ logs/ # Airflow logs
├─ docker-compose.yml 
├─ Dockerfile.airflow 
├─ requirements.txt



---

## Prerequisites

- Docker Compose containing everything 
- CSV file containing ITSM tickets



* Thank you. *