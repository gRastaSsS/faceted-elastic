USE example;

CREATE TABLE IF NOT EXISTS study (
    id INT,
    country VARCHAR(16),
    type VARCHAR(16),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS patient (
    id INT,
    study_id INT,
    age INT,
    sex VARCHAR(8),
    PRIMARY KEY (id),
    FOREIGN KEY (study_id) REFERENCES study(id)
);

CREATE TABLE IF NOT EXISTS sample (
    id INT,
    patient_id INT,
    type VARCHAR(16),
    organ VARCHAR(16),
    PRIMARY KEY (id),
    FOREIGN KEY (patient_id) REFERENCES patient(id)
);
