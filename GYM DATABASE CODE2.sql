---\ SQL Scripts for Fragmented Schemas
---\ BranchDB_A – Data for Branch A

-- Create Member table for Branch A

CREATE TABLE Member_A (
    MemberID SERIAL PRIMARY KEY,
    Name VARCHAR(100),
    Contact VARCHAR(50),
    MembershipType VARCHAR(50),
    BranchID CHAR(1) CHECK (BranchID = 'A')
);


-- Create Attendance table for Branch A

CREATE TABLE Attendance_A (
    AttendanceID SERIAL PRIMARY KEY,
    MemberID INT REFERENCES Member_A(MemberID),
    CheckInTime TIMESTAMP,
    CheckOutTime TIMESTAMP,
    Date DATE
);

-- Create Trainer table for Branch A

CREATE TABLE Trainer_A (
    TrainerID SERIAL PRIMARY KEY,
    Name VARCHAR(100),
    Specialty VARCHAR(50),
    BranchID CHAR(1) CHECK (BranchID = 'A')
);

-- Create Session table for Branch A

CREATE TABLE Session_A (
    SessionID SERIAL PRIMARY KEY,
    TrainerID INT REFERENCES Trainer_A(TrainerID),
    MemberID INT REFERENCES Member_A(MemberID),
    Date DATE,
    Time TIME
);

-- Payment Table
CREATE TABLE Payment_A (
    PaymentID SERIAL PRIMARY KEY,
    MemberID INT REFERENCES Member_A(MemberID),
    Amount DECIMAL(10,2),
    Date DATE,
    Method VARCHAR(50)
);

-- Subscription Table
CREATE TABLE Subscription_A (
    SubscriptionID SERIAL PRIMARY KEY,
    MemberID INT REFERENCES Member_A(MemberID),
    StartDate DATE,
    EndDate DATE,
    PlanType VARCHAR(50)
);
Re
---\ Insert 100 Members into Member_A
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO Member_A (Name, Contact, MembershipType, BranchID)
    VALUES (
      'Member_' || i,
      '0788000' || LPAD(i::TEXT, 3, '0'),
      CASE WHEN i % 3 = 0 THEN 'Gold'
           WHEN i % 3 = 1 THEN 'Silver'
           ELSE 'Bronze' END,
      'A'
    );
  END LOOP;
END $$;

---\ Insert 100 Trainers into Trainer_A
sql
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO Trainer_A (Name, Specialty, BranchID)
    VALUES (
      'Trainer_' || i,
      CASE WHEN i % 2 = 0 THEN 'Cardio' ELSE 'Strength' END,
      'A'
    );
  END LOOP;
END $$;

---\ Insert 100 Attendance Records into Attendance_A
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO Attendance_A (MemberID, CheckInTime, CheckOutTime, Date)
    VALUES (
      i,
      CURRENT_TIMESTAMP - (i || ' hours')::INTERVAL,
      CURRENT_TIMESTAMP - ((i - 1) || ' hours')::INTERVAL,
      CURRENT_DATE - (i % 30)
    );
  END LOOP;

  ---\ Insert 100 Sessions into Session_A
DO $$
BEGIN
  FOR i IN 1..100 LOOP
    INSERT INTO Session_A (TrainerID, MemberID, Date, Time)
    VALUES (
      i,
      i,
      CURRENT_DATE - (i % 30),
      TIME '08:00' + (i % 10) * INTERVAL '1 hour'
    );
  END LOOP;
END $$;

-- Create schemas for Branch A and Branch B
CREATE SCHEMA IF NOT EXISTS branch_a;
CREATE SCHEMA IF NOT EXISTS branch_b;

CREATE DATABASE gym_branch_b;


-- Example: Move Member_A into branch_a schema
ALTER TABLE Member_A SET SCHEMA branch_a; 

-- Enable the extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- 2. Create a foreign server definition for Branch B
CREATE SERVER gym_branch_b_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (
    host 'localhost',         -- Replace with actual host if remote
    dbname 'gym_branch_b',      -- Replace with actual Branch B database name
    port '5432'               -- Default PostgreSQL port
  );


-- Create user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER gym_branch_b_server
OPTIONS (
  user 'postgres',
  password 'postgres'
);

-- Import schema or table
IMPORT FOREIGN SCHEMA public
  LIMIT TO (member_b)
  FROM SERVER gym_branch_b_server
  INTO branch_a;
  -- 4. Import ALL tables from Branch B's public schema into Branch A
IMPORT FOREIGN SCHEMA public
  FROM SERVER gym_branch_b_server
  INTO branch_a;
  ALTER USER postgres WITH PASSWORD 'postgres';

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name = 'member_b';

-- Now you can query it like a local table
SELECT * FROM public.member_a;

SELECT foreign_table_schema, foreign_table_name
FROM information_schema.foreign_tables
WHERE foreign_table_name = 'member_b';

SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'branch_a';

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name IN ('member_a', 'member_b');










