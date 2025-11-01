---\ UNIVERSITY OF RWANDA
---\ MUKANDAYISENGA MARUE  HANTAL
---\ REGNO: 224019567

---\ BranchDB_B – Data for Branch B

-- Create Member table for Branch B

CREATE TABLE Member_B (
    MemberID SERIAL PRIMARY KEY,
    Name VARCHAR(100),
    Contact VARCHAR(50),
    MembershipType VARCHAR(50),
    BranchID CHAR(1) CHECK (BranchID = 'B')
);

-- Create Attendance table for Branch B

CREATE TABLE Attendance_B (
    AttendanceID SERIAL PRIMARY KEY,
    MemberID INT REFERENCES Member_B(MemberID),
    CheckInTime TIMESTAMP,
    CheckOutTime TIMESTAMP,
    Date DATE
);

-- Create Trainer table for Branch B

CREATE TABLE Trainer_B (
    TrainerID SERIAL PRIMARY KEY,
    Name VARCHAR(100),
    Specialty VARCHAR(50),
    BranchID CHAR(1) CHECK (BranchID = 'B')
);

-- Create Session table for Branch B

CREATE TABLE Session_B (
    SessionID SERIAL PRIMARY KEY,
    TrainerID INT REFERENCES Trainer_B(TrainerID),
    MemberID INT REFERENCES Member_B(MemberID),
    Date DATE,
    Time TIME
);


-- Payment Table
CREATE TABLE Payment_B (
    PaymentID SERIAL PRIMARY KEY,
    MemberID INT REFERENCES Member_(MemberID),
    Amount DECIMAL(10,2),
    Date DATE,
    Method VARCHAR(50)
);

-- Subscription Table

CREATE TABLE Subscription_B(
    SubscriptionID SERIAL PRIMARY KEY,
    MemberID INT REFERENCES Member_B(MemberID),
    StartDate DATE,
    EndDate DATE,
    PlanType VARCHAR(50)
);

---\ Insert 50 Records into BranchDB_B Tables
---\ Insert 100 Members into Member_B
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO Member_B (Name, Contact, MembershipType, BranchID)
    VALUES (
      'Member_B' || i,
      '0788000' || LPAD(i::TEXT, 3, '0'),
      CASE WHEN i % 3 = 0 THEN 'Gold'
           WHEN i % 3 = 1 THEN 'Silver'
           ELSE 'Bronze' END,
      'B'
    );
  END LOOP;
END $$;

---\ Insert 100 Trainers into Trainer_B
-- Clear tables first if needed
TRUNCATE TABLE Trainer_B RESTART IDENTITY CASCADE;

TRUNCATE TABLE Member_B RESTART IDENTITY CASCADE;

SELECT * FROM Member_B;

-- Insert 20 Trainers
DO $$
BEGIN
  FOR i IN 1..20 LOOP
    INSERT INTO Trainer_B (Name, Specialty, BranchID)
    VALUES (
      'TrainerB_' || i,
      CASE WHEN i % 2 = 0 THEN 'Yoga' ELSE 'HIIT' END,
      'B'
    );
  END LOOP;
END $$;


---\ Insert 100 Attendance Records into Attendance_B



  ---\ Insert 100 Sessions into Session_A
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO Session_B (TrainerID, MemberID, Date, Time)
    VALUES (
      i,
      i,
      CURRENT_DATE - (i % 30),
      TIME '08:00' + (i % 10) * INTERVAL '1 hour'
    );
  END LOOP;
END $$;

  ---\ Insert 100 Sessions into Payment_B
DO $$
BEGIN
  FOR i IN 1..50 LOOP
    INSERT INTO Payment_B (MemberID, Amount, Date, Method)
    VALUES (
      i,
      1000 + (i * 10),
      CURRENT_DATE - (i % 10),
      CASE WHEN i % 2 = 0 THEN 'Cash' ELSE 'Card' END
    );
  END LOOP;
END $$;

  ---\ Insert 100 Sessions into Subscripion_B
DO $$
BEGIN
  FOR i IN 1..50 LOOP
    INSERT INTO Subscription_B (MemberID, StartDate, EndDate, PlanType)
    VALUES (
      i,
      CURRENT_DATE - (i * 2),
      CURRENT_DATE + (30 - i),
      CASE WHEN i % 2 = 0 THEN 'Monthly' ELSE 'Annual' END
    );
  END LOOP;
END $$;

-- Enable the extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Create schemas for Branch A and Branch B
CREATE SCHEMA IF NOT EXISTS branch_a;
CREATE SCHEMA IF NOT EXISTS branch_b;

CREATE DATABASE gym_branch_A;


-- Example: Move Member_B into branch_b schema
ALTER TABLE Member_B SET SCHEMA branch_b; 



-- 2. Create a foreign server definition for Branch A
CREATE SERVER gym_branch_a_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'localhost',         -- Replace with actual host if remote
    dbname 'gym_branch_a',      -- Replace with actual Branch B database name
    port '5432'               -- Default PostgreSQL port
  );


-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER gym_branch_a_server
OPTIONS (
  user 'postgres',
  password 'postgres'
);

-- Import schema or table
IMPORT FOREIGN SCHEMA public
  LIMIT TO (member_a)
  FROM SERVER gym_branch_a_server
  INTO branch_a;
  -- 4. Import ALL tables from Branch B's public schema into Branch A
IMPORT FOREIGN SCHEMA public
  FROM SERVER gym_branch_a_server
  INTO branch_b;
  ALTER USER postgres WITH PASSWORD 'postgres';
  
---\Simulate a "Database Link" via Cross-Schema Access

  SELECT * FROM branch_b.member_b;

 ---\  Demonstrate a Remote SELECT

-- Select all Gold members from Branch B
SELECT name, membershiptype
FROM branch_b.member_b
WHERE membershiptype = 'Gold';

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name IN ('member_a', 'member_b');

SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables;

CREATE TABLE branch_a.member_a (
  MemberID SERIAL PRIMARY KEY,
  Name VARCHAR(100),
  Contact VARCHAR(50),
  MembershipType VARCHAR(50),
  BranchID CHAR(1) CHECK (BranchID = 'A')
);

SELECT * FROM branch_a.member_a;

INSERT INTO branch_a.member_a (Name, Contact, MembershipType, BranchID)
VALUES
  ('Alice', '0788123456', 'Gold', 'A'),
  ('Bob', '0788234567', 'Silver', 'A'),
  ('Claire', '0788345678', 'Bronze', 'A'),
  ('David', '0788456789', 'Gold', 'A'),
  ('Esther', '0788567890', 'Silver', 'A'),
  ('Frank', '0788678901', 'Bronze', 'A'),
  ('Grace', '0788789012', 'Gold', 'A'),
  ('Henry', '0788890123', 'Silver', 'A'),
  ('Irene', '0788901234', 'Bronze', 'A'),
  ('Jack', '0788012345', 'Gold', 'A'),
  ('Karen', '0788123450', 'Silver', 'A'),
  ('Leo', '0788234561', 'Bronze', 'A'),
  ('Mia', '0788345672', 'Gold', 'A'),
  ('Nathan', '0788456783', 'Silver', 'A'),
  ('Olivia', '0788567894', 'Bronze', 'A'),
  ('Paul', '0788678905', 'Gold', 'A'),
  ('Queen', '0788789016', 'Silver', 'A'),
  ('Ryan', '0788890127', 'Bronze', 'A'),
  ('Sarah', '0788901238', 'Gold', 'A'),
  ('Tom', '0788012349', 'Silver', 'A'),
  ('Uma', '0788123451', 'Bronze', 'A'),
  ('Victor', '0788234562', 'Gold', 'A'),
  ('Wendy', '0788345673', 'Silver', 'A'),
  ('Xavier', '0788456784', 'Bronze', 'A'),
  ('Yvonne', '0788567895', 'Gold', 'A'),
  ('Zack', '0788678906', 'Silver', 'A'),
  ('Bella', '0788789017', 'Bronze', 'A'),
  ('Chris', '0788890128', 'Gold', 'A'),
  ('Diana', '0788901239', 'Silver', 'A'),
  ('Ethan', '0788012340', 'Bronze', 'A'),
  ('Fiona', '0788123452', 'Gold', 'A'),
  ('George', '0788234563', 'Silver', 'A'),
  ('Hannah', '0788345674', 'Bronze', 'A'),
  ('Isaac', '0788456785', 'Gold', 'A'),
  ('Judy', '0788567896', 'Silver', 'A'),
  ('Kyle', '0788678907', 'Bronze', 'A'),
  ('Lena', '0788789018', 'Gold', 'A'),
  ('Mark', '0788890129', 'Silver', 'A'),
  ('Nina', '0788901240', 'Bronze', 'A'),
  ('Omar', '0788012341', 'Gold', 'A'),
  ('Pam', '0788123453', 'Silver', 'A'),
  ('Quinn', '0788234564', 'Bronze', 'A'),
  ('Rita', '0788345675', 'Gold', 'A'),
  ('Sam', '0788456786', 'Silver', 'A'),
  ('Tina', '0788567897', 'Bronze', 'A'),
  ('Umar', '0788678908', 'Gold', 'A'),
  ('Vera', '0788789019', 'Silver', 'A'),
  ('Will', '0788890130', 'Bronze', 'A'),
  ('Xena', '0788901241', 'Gold', 'A'),
  ('Yuri', '0788012342', 'Silver', 'A');
  
SELECT COUNT(*) FROM branch_a.member_a;

SELECT * FROM branch_a.member_a;

---\ Perform a Distributed Join (Cross-Schema Join)
-- Join members from Branch A and Branch B who have the same contact number

SELECT
  a.name AS branch_a_member,
  b.name AS branch_b_member,
  a.contact
FROM
  branch_a.member_a a
JOIN
  branch_b.member_b b
ON
  a.contact = b.contact;

---\Prepare a Large Table

SELECT * FROM Attendance_B;
SELECT * FROM Member_B;


  INSERT INTO Member_B (Name, Contact, MembershipType, BranchID)
SELECT
  'MemberB_' || i,
  '0799' || LPAD(i::text, 6, '0'),
  CASE
    WHEN i % 3 = 0 THEN 'Gold'
    WHEN i % 3 = 1 THEN 'Silver'
    ELSE 'Bronze'
  END,
  'B'
FROM generate_series(1, 500) AS i;

SELECT MemberID FROM Member_B ORDER BY MemberID;


-- Corrected insert for 1000 attendance records
INSERT INTO Attendance_B (MemberID, CheckInTime, CheckOutTime, Date)
SELECT
  FLOOR(RANDOM() * 50 + 1)::INT,  -- always between 1 and 50
  NOW() - (RANDOM() * INTERVAL '30 days'),
  NOW() - (RANDOM() * INTERVAL '30 days') + INTERVAL '1 hour',
  (NOW() - (RANDOM() * INTERVAL '30 days'))::DATE
FROM generate_series(1, 1000);


---\ Enable Parallel Query Execution
---\ PostgreSQL automatically decides when to use parallelism. To encourage it:

-- Set parallel workers
 max_parallel_workers_per_gather = 4;
---\ You can also check parallel settings:

SELECT name, setting FROM pg_settings WHERE name LIKE '%parallel%';

---\ Compare Serial vs Parallel Execution
---\ Serial Query

ALTER TABLE Attendance_B ADD COLUMN activity_type VARCHAR(50);

-- Example: update existing records randomly

UPDATE Attendance_B
SET activity_type = CASE
  WHEN RANDOM() < 0.5 THEN 'Cardio'
  ELSE 'Weights'
END;

EXPLAIN ANALYZE
SELECT COUNT(*) FROM Attendance_B WHERE activity_type = 'Cardio';

---\ Parallel Query (encouraged by settings)

EXPLAIN ANALYZE
SELECT COUNT(*) FROM Attendance_B WHERE activity_type = 'Cardio';



---\ Enable Two-Phase Commit
 ---\ In postgresql.conf
SHOW config_file;
SHOW max_prepared_transactions;

---\ Create the missing table

-- Create branch_a_log table
CREATE TABLE branch_a_log (
  id SERIAL PRIMARY KEY,
  message TEXT
);

-- Create branch_b_log table
CREATE TABLE branch_b_log (
  id SERIAL PRIMARY KEY,
  message TEXT
);

---\ Begin a Transaction and Prepare It
---\ Start a transaction
BEGIN;
---\ Perform operations
INSERT INTO branch_a_log (message) VALUES ('Hello from 2PC A');
INSERT INTO branch_b_log (message) VALUES ('Hello from 2PC B');

---\ Prepare the transaction
PREPARE TRANSACTION 'gym_tx_001';

-- Check pending transactions
SELECT * FROM pg_prepared_xacts;

-- Commit it
COMMIT PREPARED 'gym_tx_001';

-- Prepare the transaction
PREPARE TRANSACTION 'tx_network_failure';

---\ Distributed Concurrency Control with Lock
---\ Create a Sample Table

CREATE TABLE gym_member (
  member_id SERIAL PRIMARY KEY,
  name TEXT,
  membership_type TEXT
);

-- Insert one record to target
INSERT INTO gym_member (name, membership_type) VALUES ('Alice', 'Gold');

---\ Open Two Sessions (e.g., two psql terminals or pgAdmin tabs)
---| Session 1
BEGIN;

-- Lock the row by updating it
UPDATE gym_member SET membership_type = 'Platinum' WHERE member_id = 1;

-- Keep the transaction open (simulate delay)
-- Do NOT commit yet

---\ Session 2 (run while Session 1 is still open)
BEGIN;

-- Try to update the same row
UPDATE gym_member SET membership_type = 'Silver' WHERE member_id = 1;

---\ Inspect Locks Using pg_locks
SELECT pid, locktype, relation::regclass, mode, granted
FROM pg_locks
WHERE relation = 'gym_member'::regclass;

---\ Resolve the Conflict
COMMIT;

---\ Parallel Data Loading in PostgreSQL
---\Parallel ETL in Gym Membership System
---\ Create Source Table
CREATE TABLE raw_attendance (
  attendance_id SERIAL PRIMARY KEY,
  member_id INT,
  branch_id TEXT,
  checkin_time TIMESTAMP,
  checkout_time TIMESTAMP,
  activity_type TEXT
);

---\ Populate with 1 million rows

INSERT INTO raw_attendance (member_id, branch_id, checkin_time, checkout_time, activity_type)
SELECT
  (RANDOM() * 1000)::INT,
  CASE WHEN i % 3 = 0 THEN 'A'
       WHEN i % 3 = 1 THEN 'B'
       ELSE 'C' END,
  NOW() - INTERVAL '1 day' * (i % 30),
  NOW() - INTERVAL '1 day' * (i % 30) + INTERVAL '1 hour',
  CASE WHEN i % 2 = 0 THEN 'Cardio' ELSE 'Weights' END
FROM generate_series(1, 1000000) AS s(i);

DROP TABLE IF EXISTS attendance_partitioned CASCADE;


---\ Create Partitioned Target Table
CREATE TABLE attendance_partitioned (
  attendance_id SERIAL,
  member_id INT,
  branch_id TEXT,
  checkin_time TIMESTAMP,
  checkout_time TIMESTAMP,
  activity_type TEXT
) PARTITION BY LIST (branch_id);


CREATE TABLE attendance_a PARTITION OF attendance_partitioned FOR VALUES IN ('A');
CREATE TABLE attendance_b PARTITION OF attendance_partitioned FOR VALUES IN ('B');
CREATE TABLE attendance_c PARTITION OF attendance_partitioned FOR VALUES IN ('C');

ALTER TABLE attendance_b ADD COLUMN trainer_name TEXT;
ALTER TABLE attendance_b ALTER COLUMN activity_type TYPE VARCHAR(100);


SELECT DISTINCT branch_id FROM raw_attendance;

CREATE TABLE attendance_b_backup AS
SELECT * FROM attendance_b;
CREATE TABLE attendance_a_backup AS
SELECT * FROM attendance_a;
CREATE TABLE attendance_c_backup AS
SELECT * FROM attendance_c;

DROP TABLE attendance_b;
DROP TABLE attendance_a;
DROP TABLE attendance_c;



---\ Serial Insert (Baseline)

EXPLAIN ANALYZE
INSERT INTO attendance_partitioned
SELECT * FROM raw_attendance;




-- Session 1
INSERT INTO attendance_partitioned
SELECT * FROM raw_attendance WHERE branch_id = 'A';

-- Session 2
INSERT INTO attendance_partitioned
SELECT * FROM raw_attendance WHERE branch_id = 'B';

-- Session 3
INSERT INTO attendance_partitioned
SELECT * FROM raw_attendance WHERE branch_id = 'C';

---\ Compare Performance
EXPLAIN ANALYZE
INSERT INTO attendance_partitioned
SELECT * FROM raw_attendance WHERE branch_id = 'A';

---\ Optional: Aggregate in Parallel
-- Parallel aggregation by activity type
SELECT activity_type, COUNT(*) 
FROM attendance_partitioned
GROUP BY activity_type;

---\ Distributed Query Optimization in PostgreSQL

---\ Analyze a Distributed Join
CREATE FOREIGN TABLE attendance_remote (
  attendance_id INT,
  member_id INT,
  checkin_time TIMESTAMP
) SERVER gym_fdw_server OPTIONS (schema_name 'public', table_name 'attendance');

CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER gym_fdw_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host '127.0.0.1', port '5432', dbname 'gym_remote_db');

CREATE USER MAPPING FOR CURRENT_USER
SERVER gym_fdw_server
OPTIONS (user 'postegres', password 'postegres');

CREATE FOREIGN TABLE attendance_remote (
  attendance_id INT,
  member_id INT,
  checkin_time TIMESTAMP
)
SERVER gym_fdw_server
OPTIONS (schema_name 'public', table_name 'attendance');

---\ Run EXPLAIN on a distributed join
EXPLAIN VERBOSE
SELECT m.name, a.checkin_time
FROM gym_member_local m
JOIN attendance_remote a ON m.member_id = a.member_id
WHERE a.checkin_time > CURRENT_DATE - INTERVAL '7 days';

CREATE TABLE gym_member_local (
  member_id INT PRIMARY KEY,
  name TEXT,







  
  membership_type TEXT
);

INSERT INTO gym_member_local (member_id, name, membership_type)
VALUES
  (1, 'Alice', 'Gold'),
  (2, 'Bob', 'Silver'),
  (3, 'Charlie', 'Platinum');

  EXPLAIN VERBOSE
SELECT m.name, a.checkin_time
FROM gym_member_local m
JOIN attendance_remote a ON m.member_id = a.member_id
WHERE a.checkin_time > CURRENT_DATE - INTERVAL '7 days';

---\ Benchmark Setup: Gym Membership System 
SELECT branch_id, COUNT(*) AS total_checkins
FROM attendance_log
WHERE checkin_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY branch_id;

---\ Create the missing table

CREATE TABLE attendance_log (
  attendance_id SERIAL PRIMARY KEY,
  member_id INT,
  branch_id TEXT,
  checkin_time TIMESTAMP,
  checkout_time TIMESTAMP,
  activity_type TEXT
);

---\ Populate with sample data (optional for testing)
INSERT INTO attendance_log (member_id, branch_id, checkin_time, checkout_time, activity_type)
SELECT
  (RANDOM() * 1000)::INT,
  CASE WHEN i % 3 = 0 THEN 'A'
       WHEN i % 3 = 1 THEN 'B'
       ELSE 'C' END,
  NOW() - INTERVAL '1 day' * (i % 30),
  NOW() - INTERVAL '1 day' * (i % 30) + INTERVAL '1 hour',
  CASE WHEN i % 2 = 0 THEN 'Cardio' ELSE 'Weights' END
FROM generate_series(1, 100000) AS s(i);

---\ Retry your benchmark query

EXPLAIN ANALYZE
SELECT branch_id, COUNT(*) AS total_checkins
FROM attendance_log
WHERE checkin_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY branch_id;

---\ Complex Query: Attendance Summary by Branch
SELECT branch_id, COUNT(*) AS total_checkins
FROM attendance_log
WHERE checkin_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY branch_id;

---\ Centralized Execution
SET max_parallel_workers_per_gather = 4;

EXPLAIN ANALYZE
SELECT branch_id, COUNT(*) FROM attendance_partitioned
WHERE checkin_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY branch_id;

---\ Distributed Execution
EXPLAIN VERBOSE
SELECT branch_id, COUNT(*) FROM attendance_remote
WHERE checkin_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY branch_id;

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'attendance_remote';

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'attendance';

DROP FOREIGN TABLE IF EXISTS attendance_remote;

CREATE FOREIGN TABLE attendance_remote (
  attendance_id INT,
  member_id INT,
  branch_id TEXT,
  checkin_time TIMESTAMP,
  checkout_time TIMESTAMP,
  activity_type TEXT
)
SERVER gym_fdw_server
OPTIONS (schema_name 'public', table_name 'attendance');


EXPLAIN VERBOSE
SELECT branch_id, COUNT(*) 
FROM attendance_remote
WHERE checkin_time > CURRENT_DATE - INTERVAL '30 days'
GROUP BY branch_id;








