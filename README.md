Airflow Hospital ETL Pipeline

Overview

This project is an ETL (Extract, Transform, Load) pipeline built using Apache Airflow to process hospital admissions data. The pipeline extracts data from CSV files, transforms it, and loads it into a PostgreSQL database.

Tech Stack
•	Apache Airflow: Workflow orchestration
•	PostgreSQL: Relational database
•	Docker: Containerization
•	Google Cloud Platform (GCP): Cloud deployment (optional)
•	Python & Pandas: Data processing


Folder Structure

📁 airflow_hospital_etl
│-- 📁 dags
│   └── hospital_etl.py    # Airflow DAG definition
│-- 📁 data
│   ├── hospital_admissions.csv    # Raw data file
│   ├── processed_admissions.csv   # Transformed data
│   └── final_admissions.csv       # Final clean data
│-- 📁 scripts
│   └── db_setup.sql        # SQL script to set up the database
│-- 📁 logs
│-- README.md               # Project documentation


Installation & Setup

1️⃣ Clone the Repository
git clone https://github.com/your_username/airflow_hospital_etl.git
cd airflow_hospital_etl

2️⃣ Set Up PostgreSQL Database
Modify and run scripts/db_setup.sql to create the required database and table.
CREATE DATABASE healthcare_db;


CREATE TABLE hospital_admissions (
    patient_id INT,
    admission_date TIMESTAMP,
    discharge_date TIMESTAMP,
    diagnosis TEXT,
    department TEXT
);


3️⃣ Run Apache Airflow


Start the scheduler and web server:
airflow scheduler & airflow webserver --port 8080
Open Airflow UI at http://localhost:8080 and search for hospital_etl


4️⃣ Execute the DAG

Trigger the hospital_etl DAG in Airflow UI or run:

airflow dags trigger hospital_etl
Contributions & Future Work
•	Add Docker Compose for easy setup
•	Deploy pipeline on GCP (Cloud Composer)
•	Implement real-time data streaming with Apache Kafka
Contact

🔹 Author: Augustine Narokwe
🔹 GitHub: @Narokwe
🔹 LinkedIn: https://www.linkedin.com/in/augustine-narokwe-354505293
________________________________________
📌 If you find this project useful, give it a ⭐ on GitHub!

