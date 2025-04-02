CREATE TABLE IF NOT EXISTS hospital_admissions (
    id SERIAL PRIMARY KEY,
    patient_id INT,
    admission_date TIMESTAMP,
    discharge_date TIMESTAMP,
    diagnosis TEXT,
    department TEXT
);