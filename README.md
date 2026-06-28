# M-A-Data-Migration-Regulatory-Quality-Assurance
An enterprise-grade ELT data migration architecture featuring built-in statistical credit risk validation and data quality controls tailored to BCBS 239 and regulatory compliance standards.



This project simulates a production-grade environment where 5 million credit records are extracted, cleansed, and transformed while maintaining strict data lineage and auditability under the BCBS 239 risk data aggregation framework.

Key Features
The pipeline addresses real-world financial data engineering challenges through:

Automated Data Quality Framework: Utilizes dbt (data build tool) to verify data integrity at every stage of the ELT process (e.g., validating currency formats, checking for negative loan balances, detecting age anomalies).

Dead Letter Queue (Quarantine System): Malformed records (e.g., corrupted loan amounts or missing reference base rates) do not break the pipeline. They are automatically isolated into an audit table (rejected_loans) for operational review, allowing valid data to proceed downstream uninterrupted.

Statistical Risk Validation (Data Drift Detection): A Python post-migration script leverages the Kolmogorov-Smirnov test to prove that data transformation and cleansing did not distort the original Probability of Default (PD) distribution. The system raises an alert if the post-migration risk profile shifts beyond an acceptable threshold.

Data Lineage & Auditability: Automatically generates a visual dependency graph and documentation proving the origin and transformation steps of every financial metric.

 Tech-Stack & Architecture

Database: PostgreSQL (Simulating two isolated instances: Legacy Source & Target DWH)

Data Transformation: dbt (Data Build Tool)

Orchestration: Apache Airflow

Data Generation & Analytics: Python (pandas, scipy.stats, Faker)

Infrastructure: Docker & Docker-Compose

Quick Start
The system is designed with an Infrastructure-as-Code approach. Follow these steps to spin up the environment and generate the mock portfolio data:

Clone the repository:

Bash
git clone https://github.com/YourUsername/bank-ma-data-migration.git
cd bank-ma-data-migration
Spin up the containers (PostgreSQL, Airflow, dbt):

Bash
docker-compose up -d
Open http://localhost:8080 in your browser to access the Apache Airflow UI (login: admin, password: admin) and trigger the DAG named ma_migration_pipeline.

📂 Repository Structure
/data_generator/ - Python scripts (Faker) generating the synthetic 5M loan portfolio. Zero real-world PII/GDPR data included.

/dbt_project/ - SQL models, macros, and schema test configurations (Data Quality rules).

/airflow/dags/ - Airflow DAG definitions establishing workflow orchestration.

/validation/ - Statistical testing modules validating risk parameter distributions post-load.
