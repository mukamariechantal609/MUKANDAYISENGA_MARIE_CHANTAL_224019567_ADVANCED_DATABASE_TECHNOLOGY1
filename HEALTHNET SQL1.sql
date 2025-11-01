---\Declarative Constraints – Safe Prescriptions 

-- Create schema
CREATE SCHEMA healthnet;
SET search_path TO healthnet;

-- Patient table
CREATE TABLE patient (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);


ALTER USER postgres WITH PASSWORD 'SYSTEM';

-- Prescription table with constraints
CREATE TABLE patient_med (
    patient_med_id SERIAL PRIMARY KEY,
    patient_id INT NOT NULL REFERENCES patient(id),
    med_name VARCHAR(80) NOT NULL,
    dose_mg NUMERIC(6,2) CHECK (dose_mg >= 0),
    start_dt DATE,
    end_dt DATE,
    CHECK (start_dt IS NULL OR end_dt IS NULL OR start_dt <= end_dt)
);

-- Passing inserts
INSERT INTO patient (name) VALUES ('John Doe');
INSERT INTO patient_med (patient_id, med_name, dose_mg, start_dt, end_dt)
VALUES (1, 'Amoxicillin', 250.00, '2025-10-01', '2025-10-10');

-- Failing inserts
-- Negative dose
INSERT INTO patient_med (patient_id, med_name, dose_mg)
VALUES (1, 'Ibuprofen', -100.00);

-- Inverted dates
INSERT INTO patient_med (patient_id, med_name, dose_mg, start_dt, end_dt)
VALUES (1, 'Ciprofloxacin', 200.00, '2025-11-10', '2025-10-01');

---\ Active Databases – Statement-Level Trigger for Bill Totals
-- Tables
CREATE TABLE bill (
    id SERIAL PRIMARY KEY,
    total NUMERIC(12,2) DEFAULT 0
);

CREATE TABLE bill_item (
    bill_id INT REFERENCES bill(id),
    amount NUMERIC(12,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bill_audit (
    bill_id INT,
    old_total NUMERIC(12,2),
    new_total NUMERIC(12,2),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Temporary table to collect affected BILL_IDs
CREATE TABLE bill_ids_temp (
    bill_id INT
);

---\ Row-level trigger function:
CREATE OR REPLACE FUNCTION collect_bill_ids()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO bill_ids_temp (bill_id)
    VALUES (COALESCE(NEW.bill_id, OLD.bill_id));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

---\ Row-level trigger definition:

CREATE TRIGGER trg_collect_bill_ids
AFTER INSERT OR UPDATE OR DELETE ON bill_item
FOR EACH ROW
EXECUTE FUNCTION collect_bill_ids();

---\ Statement-level trigger to recompute totals
---\ Statement-level function:

CREATE OR REPLACE FUNCTION recompute_bill_totals_stmt()
RETURNS TRIGGER AS $$
DECLARE
    bill_row RECORD;
    old_total NUMERIC(12,2);
    new_total NUMERIC(12,2);
BEGIN
    FOR bill_row IN SELECT DISTINCT bill_id FROM bill_ids_temp LOOP
        SELECT total INTO old_total FROM bill WHERE id = bill_row.bill_id;

        SELECT COALESCE(SUM(amount), 0)
        INTO new_total
        FROM bill_item
        WHERE bill_id = bill_row.bill_id;

        UPDATE bill SET total = new_total WHERE id = bill_row.bill_id;

        INSERT INTO bill_audit (bill_id, old_total, new_total)
        VALUES (bill_row.bill_id, old_total, new_total);
    END LOOP;

    DELETE FROM bill_ids_temp; -- Clear after processing
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

---\ Statement-level trigger definition:

CREATE TRIGGER trg_recompute_totals_stmt
AFTER INSERT OR UPDATE OR DELETE ON bill_item
FOR EACH STATEMENT
EXECUTE FUNCTION recompute_bill_totals_stmt();


 -- Statement-level trigger (not FOR EACH ROW)
DROP TRIGGER IF EXISTS trg_bill_total_stmt ON bill_item;

CREATE TRIGGER trg_bill_total_stmt
AFTER INSERT OR UPDATE OR DELETE ON bill_item
FOR EACH STATEMENT
EXECUTE FUNCTION recompute_bill_totals_stmt();


---\ Deductive Databases – Supervision Chain
-- Supervision table
CREATE TABLE staff_supervisor (
    employee VARCHAR(50),
    supervisor VARCHAR(50)
);

WITH RECURSIVE supers(emp, sup, hops, path) AS (
    -- Anchor: start with direct supervision links, cast path to TEXT
    SELECT employee, supervisor, 1, employee::TEXT
    FROM staff_supervisor

    UNION ALL

    -- Recursive: climb up the supervision chain
    SELECT s.employee, t.sup, t.hops + 1, t.path || '>' || t.sup
    FROM staff_supervisor s
    JOIN supers t ON s.supervisor = t.emp
    WHERE POSITION(t.sup IN t.path) = 0
)
-- Final selection: deepest supervisor per employee
SELECT emp, sup AS top_supervisor, hops
FROM supers
WHERE (emp, hops) IN (
    SELECT emp, MAX(hops) FROM supers GROUP BY emp
);

---\ Knowledge Bases – Infectious Disease Roll-Up
-- Triple table
CREATE TABLE triple (
    s VARCHAR(100),
    p VARCHAR(50),
    o VARCHAR(100)
);

-- Recursive isA closure
WITH RECURSIVE isa(entity, category) AS (
    SELECT s, o FROM triple WHERE p = 'isA'
    UNION ALL
    SELECT t.s, i.category
    FROM triple t
    JOIN isa i ON t.o = i.entity
    WHERE t.p = 'isA'
),
infectious_patients AS (
    SELECT DISTINCT t.s
    FROM triple t
    JOIN isa ON t.o = isa.entity
    WHERE t.p = 'hasDiagnosis' AND isa.category = 'InfectiousDisease'
)
SELECT s AS patient_id FROM infectious_patients;

---\ Spatial Databases – Clinics Within Radius and Nearest 3
---\ Enable PostGIS Extension
CREATE EXTENSION postgis;
SELECT PostGIS_Version();

-- Clinic table
CREATE TABLE clinic (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    geom GEOMETRY(Point, 4326)
);

-- Spatial index
CREATE INDEX clinic_spx ON clinic USING GIST (geom);

-- Ambulance location
SELECT id, name
FROM clinic
WHERE ST_DWithin(
    geom,
    ST_SetSRID(ST_MakePoint(30.0600, -1.9570), 4326),
    0.001  -- ~1km in degrees
);

-- Nearest 3 clinics
SELECT id, name,
       ST_Distance(
           geom,
           ST_SetSRID(ST_MakePoint(30.0600, -1.9570), 4326)
       ) AS km
FROM clinic
ORDER BY km
LIMIT 3;



