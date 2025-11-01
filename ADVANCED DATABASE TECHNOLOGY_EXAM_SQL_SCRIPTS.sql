---\ AFRICAN CENTRE OF EXCELLENCE IN DATA SCIENCE (ACE-DS)

---\ ADVANCED DATABASE PROJECT-BASED EXAM 
---\ Module Code: DSM6235    
---\ School/Centre: African Centre of Excellence in Data Science 

---\ MUKANDAYISENGA MARIE CHANTAL
---\ REGNO: 224019567
---\ SECTION A
---\ 1.reate horizontally fragmented tables Attendance_A on Node_A and Attendance_B on Node_B 
---\ using a deterministic rule (HASH or RANGE on a natural key).


CREATE TABLE Attendance_A (
    AttendanceID SERIAL PRIMARY KEY,
    MemberID INT,
    CheckInTime TIMESTAMP,
    CheckOutTime TIMESTAMP,
    Date DATE
);

CREATE TABLE Attendance_B (
    AttendanceID SERIAL PRIMARY KEY,
    MemberID INT,
    CheckInTime TIMESTAMP,
    CheckOutTime TIMESTAMP,
    Date DATE
);
---\ on Node_B). Reuse these rows for all remaining tasks.	
);
INSERT INTO Attendance_A (MemberID, CheckInTime, CheckOutTime, Date)
VALUES
(1, CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP - INTERVAL '1 hour', CURRENT_DATE),
(2, CURRENT_TIMESTAMP - INTERVAL '3 hours', CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_DATE),
(3, CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_TIMESTAMP - INTERVAL '3 hours', CURRENT_DATE),
(4, CURRENT_TIMESTAMP - INTERVAL '5 hours', CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_DATE),
(5, CURRENT_TIMESTAMP - INTERVAL '6 hours', CURRENT_TIMESTAMP - INTERVAL '5 hours', CURRENT_DATE);

INSERT INTO Attendance_B (MemberID, CheckInTime, CheckOutTime, Date)
VALUES
(6, CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_TIMESTAMP - INTERVAL '1 hour', CURRENT_DATE),
(7, CURRENT_TIMESTAMP - INTERVAL '3 hours', CURRENT_TIMESTAMP - INTERVAL '2 hours', CURRENT_DATE),
(8, CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_TIMESTAMP - INTERVAL '3 hours', CURRENT_DATE),
(9, CURRENT_TIMESTAMP - INTERVAL '5 hours', CURRENT_TIMESTAMP - INTERVAL '4 hours', CURRENT_DATE),
(10, CURRENT_TIMESTAMP - INTERVAL '6 hours', CURRENT_TIMESTAMP - INTERVAL '5 hours', CURRENT_DATE);

---\ create a forein server
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER node_b_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'localhost', dbname 'node_b_db', port '5432');

---\ create user mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER node_b_server
OPTIONS (user 'gym_user1', password 'gym_pass');


---\ 
SELECT usename FROM pg_user WHERE usename = 'gym_user';
CREATE USER gym_user WITH PASSWORD 'gym_pass';
GRANT CONNECT ON DATABASE node_b_db TO gym_user;
CREATE DATABASE node_b_db;


---\ import defined tables
IMPORT FOREIGN SCHEMA public
FROM SERVER node_b_server
INTO public;
CREATE FOREIGN TABLE Attendance_B (
    AttendanceID INT,
    MemberID INT,
    CheckInTime TIMESTAMP,
    CheckOutTime TIMESTAMP,
    Date DATE
)
SERVER node_b_server
OPTIONS (table_name 'Attendance_B');

---\ create the view

CREATE VIEW Attendance_ALL AS
SELECT * FROM Attendance_A
UNION ALL
SELECT * FROM Attendance_B;

---\ validate the view

SELECT COUNT(*) FROM Attendance_ALL;
---\ check sum;
SELECT SUM(MOD(MemberID, 97)) FROM Attendance_ALL;
---\ compare this with;
SELECT SUM(MOD(MemberID, 97)) FROM Attendance_A;
SELECT SUM(MOD(MemberID, 97)) FROM Attendance_B;
---\ Validation queries
-- Local fragment
SELECT COUNT(*) AS count_a FROM Attendance_A;

-- Remote fragment (via FDW)
SELECT COUNT(*) AS count_b FROM Attendance_B;

-- Combined view
SELECT COUNT(*) AS count_all FROM Attendance_ALL;

---\ Enable FDW Extension on Node_A
CREATE EXTENSION IF NOT EXISTS postgres_fdw;
CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host 'IP_or_hostname_of_Node_B',
    dbname 'node_b_db',
    port '5432'
);
---\ check the user
SELECT * FROM pg_user_mappings;
---\ create uaser mapping
CREATE USER MAPPING FOR CURRENT_USER
SERVER proj_link
OPTIONS (
    user 'gym_user',
    password 'gym_pass'
);

CREATE SERVER proj_link
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host '192.168.43.182',
    dbname 'node_b_db',
    port '5432'
);

SELECT * FROM Attendance_B;
---\ Remote SELECT on Session via FDW (proj_link)
SELECT * FROM Session
LIMIT 5;

---\ Run the Distributed Join
SELECT
    A.AttendanceID AS LocalID,
    B.AttendanceID AS RemoteID,
    A.MemberID,
    A.CheckInTime AS LocalCheckIn,
    B.CheckInTime AS RemoteCheckIn
FROM Attendance_A A
JOIN Attendance_B B ON A.MemberID = B.MemberID
LIMIT 5;

SELECT * FROM Member LIMIT 5;

SELECT
    A.AttendanceID,
    A.Memberid,
    M.fullname,
    A.CheckInTime,
    A.Date
FROM Attendance_A A
JOIN Member M ON A.Memberid = M.Memberid
WHERE A.Date >= '2025-01-10'
LIMIT 10;

---\ Group by Date — Count Attendance Records
SELECT
    Date,
    COUNT(*) AS TotalSessions
FROM Attendance_ALL
GROUP BY Date
ORDER BY Date
LIMIT 10;

---\ Group by MemberID — Count Sessions per Member
SELECT
    MemberID,
    COUNT(*) AS SessionCount
FROM Attendance_ALL
GROUP BY MemberID
ORDER BY SessionCount DESC
LIMIT 10;

/*+ PARALLEL(Attendance_A,8) PARALLEL(Attendance_B,8) */
SET max_parallel_workers_per_gather = 8;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;

---\ Run the Aggregation Query
SELECT
    Date,
    COUNT(*) AS TotalSessions
FROM Attendance_ALL
GROUP BY Date
ORDER BY Date
LIMIT 10;

EXPLAIN ANALYZE
SELECT
    Date,
    COUNT(*) AS TotalSessions
FROM Attendance_ALL
GROUP BY Date
ORDER BY Date
LIMIT 10;

EXPLAIN ANALYZE
SELECT Date, COUNT(*) FROM Attendance_ALL GROUP BY Date ORDER BY Date LIMIT 10;


BEGIN;

-- Insert local row into Attendance_A
INSERT INTO Attendance_A (
    AttendanceID,
    Memberid,
    CheckInTime,
    CheckOutTime,
    Date
) VALUES (
    1001,
    301,
    '2025-10-28 08:30:00',
    '2025-10-28 09:30:00',
    '2025-10-28'
);

SELECT * FROM payment;
-- Insert remote row into Payment (foreign table from Node_B)
INSERT INTO Payment (
    paymentId,
    subscriptionId,
    amount,
    paymentDate,
    method
) VALUES (
    7001,
    4001,
    15000.00,
    '2025-10-28',
    'Mobile Money'
);


ROLLBACK;

COMMIT;

BEGIN;
INSERT INTO Attendance_A (
    AttendanceID,
    MemberID,
    CheckInTime,
    CheckOutTime,
    Date
) VALUES (
    1002,
    302,
    '2025-10-29 08:00:00',
    '2025-10-29 09:00:00',
    '2025-10-29'
);

---\ use invalid remot
INSERT INTO Payment (
    PaymentID,
    MemberID,
    Amount,
    PaymentDate
) VALUES (
    7002,
    NULL,         --  violates NOT NULL constraint
    'invalid',    --  wrong data type
    '2025-10-29'
);

INSERT INTO Payment (...);  -- will fail due to unreachable server

---\ PostgreSQL Aborts the Transaction
ROLLBACK;

---\ Begin a Transaction
BEGIN;
-- Your inserts here
---\ PREPARE TRANSACTION 'txn_001';

PREPARE TRANSACTION 'txn_001';
---\ View Pending Prepared Transactions
SELECT * FROM pg_prepared_xacts;

---\ Resolve the Transaction
COMMIT PREPARED 'txn_001';

ROLLBACK PREPARED 'txn_001';

---\ Start a Clean Transaction

BEGIN;

-- Insert local row into Attendance_A
INSERT INTO Attendance_A (
    AttendanceID,
    Memberid,
    CheckInTime,
    CheckOutTime,
    Date
) VALUES (
    1003,
    303,
    '2025-10-30 08:00:00',
    '2025-10-30 09:00:00',
    '2025-10-30'
);

-- Insert remote row into Payment (via FDW)
INSERT INTO Payment (
    paymentId,
    subscriptionId,
    amount,
    paymentDate,
    method
) VALUES (
    7003,
    4003,
    12000.00,
    '2025-10-30',
    'Bank Transfer'
);

COMMIT;
---\ Verify No Pending Transactions
SELECT * FROM pg_prepared_xacts;
SELECT * FROM Attendance_A WHERE AttendanceID = 1003;
SELECT * FROM Payment WHERE paymentId = 7003;

---\SQL for Session 1 (Node_A): Update + Hold Transaction
BEGIN;

-- Update a single row in Subscription (local or foreign via FDW)
UPDATE Subscription
SET status = 'Suspended'
WHERE subscriptionId = 4003;


BEGIN;
UPDATE Payment
SET method = 'Suspended'
WHERE paymentId = 7003;

---\ Update via FDW from Node_B
-- On Node_B, using FDW to access Payment on Node_A
UPDATE Payment
SET method = 'Credit Card'
WHERE paymentId = 7003;


ROLLBACK;
 ---\ Query lock views (DBA_BLOCKERS/DBA_WAITERS/V$LOCK) from Node_A to show the 
---\ waiting session.
SELECT * FROM pg_locks;

---\ Show Blocking and Blocked Sessions
SELECT
  blocked_locks.pid AS blocked_pid,
  blocked_activity.usename AS blocked_user,
  blocking_locks.pid AS blocking_pid,
  blocking_activity.usename AS blocking_user,
  blocked_activity.query AS blocked_query,
  blocking_activity.query AS blocking_query,
  blocked_activity.application_name,
  blocked_activity.client_addr
FROM pg_locks blocked_locks
JOIN pg_stat_activity blocked_activity
  ON blocked_activity.pid = blocked_locks.pid
JOIN pg_locks blocking_locks
  ON blocking_locks.locktype = blocked_locks.locktype
  AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
  AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
  AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
  AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
  AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
  AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
  AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
  AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
  AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
  AND blocking_locks.pid != blocked_locks.pid
JOIN pg_stat_activity blocking_activity
  ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;

---\ View All Active Sessions
SELECT pid, usename, query, state, wait_event_type, wait_event
FROM pg_stat_activity
WHERE state != 'idle';

---\  Release the lock; show Session 2 completes. Do not insert more rows; reuse the existing ≤10.
---\Release the Lock in Session 1 (Node_A)
BEGIN;

UPDATE Payment
SET method = 'Suspended'
WHERE paymentId = 7003;
COMMIT;

---\Session 2 (Node_B via FDW) Completes
UPDATE Payment
SET method = 'Credit Card'
WHERE paymentId = 7003;

SELECT paymentId, method FROM Payment WHERE paymentId = 7003;

---\ B6 :Declarative Rules Hardening (≤10 committed rows)
---\  On tables Subscription and Payment, add/verify NOT NULL and domain CHECK constraints 
---\suitable for trainer sessions and membership revenue (e.g., positive amounts, valid statuses, date 
---\ order).

ALTER TABLE Subscription
-- Ensure critical fields are not null
ALTER COLUMN memberId SET NOT NULL,
ALTER COLUMN startDate SET NOT NULL,
ALTER COLUMN endDate SET NOT NULL,
ALTER COLUMN status SET NOT NULL;

-- Ensure valid status values
-- Set NOT NULL constraints
ALTER TABLE Subscription ALTER COLUMN memberId SET NOT NULL;
ALTER TABLE Subscription ALTER COLUMN startDate SET NOT NULL;
ALTER TABLE Subscription ALTER COLUMN endDate SET NOT NULL;
ALTER TABLE Subscription ALTER COLUMN status SET NOT NULL;

-- Add CHECK constraint for valid status
ALTER TABLE Subscription
ADD CONSTRAINT chk_subscription_status
CHECK (status IN ('Active', 'Expired', 'Suspended'));

-- Add CHECK constraint for date logic
ALTER TABLE Subscription
ADD CONSTRAINT chk_subscription_dates
CHECK (startDate < endDate);

-- Set NOT NULL constraints
ALTER TABLE Payment ALTER COLUMN subscriptionId SET NOT NULL;
ALTER TABLE Payment ALTER COLUMN amount SET NOT NULL;
ALTER TABLE Payment ALTER COLUMN paymentDate SET NOT NULL;
ALTER TABLE Payment ALTER COLUMN method SET NOT NULL;

-- Add CHECK constraint for positive amount
ALTER TABLE Payment
ADD CONSTRAINT chk_payment_amount
CHECK (amount > 0);

-- Add CHECK constraint for valid method
ALTER TABLE Payment
ADD CONSTRAINT chk_payment_method
CHECK (method IN ('Mobile Money', 'Bank Transfer', 'Cash', 'Credit Card'));
---\ Verify Constraints
SELECT conname, contype, convalidated
FROM pg_constraint
WHERE conrelid = 'subscription'::regclass
   OR conrelid = 'payment'::regclass;

   ---\ On tables Subscription and Payment, add/verify NOT NULL and domain CHECK constraints 
---\suitable for trainer sessions and membership revenue (e.g., positive amounts, valid statuses, date 
---\ order).
---\ Passing INSERTs — Subscription
-- Valid subscription: Active, correct date order
INSERT INTO Subscription (
    subscriptionId,
    memberId,
    startDate,
    endDate,
    plantype,
    status
) VALUES (
    4004,
    304,
    '2025-10-15',
    '2025-11-15',
    'Monthly',
    'Active' 
);
---\ add constraint in column plantype
ALTER TABLE Subscription
ALTER COLUMN plantype SET NOT NULL;

---\ add suspended in subscription
ALTER TABLE Subscription
DROP CONSTRAINT subscription_status_check;

ALTER TABLE Subscription
ADD CONSTRAINT subscription_status_check
CHECK (status IN ('Active', 'Expired', 'Suspended'));

]SELECT conname, convalidated
FROM pg_constraint
WHERE conrelid = 'subscription'::regclass;

ALTER TABLE Subscription
DROP CONSTRAINT IF EXISTS subscription_status_check;

ALTER TABLE Subscription
ADD CONSTRAINT subscription_status_check
CHECK (status IN ('Active', 'Suspended', 'Expired'));


-- Active subscription
INSERT INTO Subscription (
    subscriptionId,
    memberId,
    startDate,
    endDate,
    plantype,
    status
) VALUES (
    4010,
    310,
    '2025-11-01',
    '2025-12-01',
    'Monthly',
    'Active'
);

-- Suspended subscription
INSERT INTO Subscription (
    subscriptionId,
    memberId,
    startDate,
    endDate,
    plantype,
    status
) VALUES (
    4011,
    311,
    '2025-11-01',
    '2025-12-01',
    'Monthly',
    'Suspended'
);

SELECT * FROM subscription;

---\ Passing INSERTs — Payment
-- Valid payment: positive amount, valid method
INSERT INTO Payment (
    paymentid,
    subscriptionid,
    amount,
    paymentdate,
    method
) VALUES (
    7004,
    4004,
    10000.00,
    '2025-11-02',
    'Mobile Money'
);
SELECT * FROM payment;
-- Valid payment: bank transfer
ALTER TABLE Payment
DROP CONSTRAINT payment_method_check;
---\ add new contraints
ALTER TABLE Payment
ADD CONSTRAINT payment_method_check
CHECK (method IN ('Mobile Money', 'Bank Transfer', 'Cash', 'Credit Card'));


INSERT INTO Payment (
    paymentId,
    subscriptionId,
    amount,
    paymentDate,
    method
) VALUES (
    7005,
    4005,
    8000.00,
    '2025-11-03',
    'Bank Transfer'
);
-- Valid payment: positive amount, valid method
INSERT INTO Payment (
    paymentId,
    subscriptionId,
    amount,
    paymentDate,
    method
) VALUES (
    7004,
    4004,
    10000.00,
    '2025-11-02',
    'Mobile Money'
);

---\ Failing INSERTs — Wrapped in ROLLBACK
BEGIN;

-- Invalid subscription: status not allowed
INSERT INTO Subscription (
    subscriptionId,
    memberId,
    startDate,
    endDate,
    status
) VALUES (
    4006,
    306,
    '2025-11-01',
    '2025-12-01',
    'Paused'  --  not in allowed list
);

-- Invalid subscription: startDate after endDate
INSERT INTO Subscription (
    subscriptionId,
    memberId,
    startDate,
    endDate,
    status
) VALUES (
    4007,
    307,
    '2025-12-01',
    '2025-11-01',
    'Active'  --  date logic fails
);

-- Invalid payment: negative amount
INSERT INTO Payment (
    paymentId,
    subscriptionId,
    amount,
    paymentDate,
    method
) VALUES (
    7006,
    4004,
    -5000.00,  --  violates CHECK (amount > 0)
    '2025-11-04',
    'Cash'
);

-- Invalid payment: method not allowed
INSERT INTO Payment (
    paymentId,
    subscriptionId,
    amount,
    paymentDate,
    method
) VALUES (
    7007,
    4005,
    9000.00,
    '2025-11-05',
    'Bitcoin'  --  not in allowed list
);

ROLLBACK;

---\  PostgreSQL Error Handling Block
---\Failing INSERTs with Error Logging 

DO $$
BEGIN
    -- Failing Subscription: invalid status
    BEGIN
        INSERT INTO Subscription (
            subscriptionId,
            memberId,
            startDate,
            endDate,
            plantype,
            status
        ) VALUES (
            4012,
            312,
            '2025-11-01',
            '2025-12-01',
            'Monthly',
            'Paused'  --  Not allowed
        );
    EXCEPTION WHEN others THEN
        RAISE NOTICE 'Subscription INSERT failed: %', SQLERRM;
    END;

    -- Failing Payment: negative amount
    BEGIN
        INSERT INTO Payment (
            paymentId,
            subscriptionId,
            amount,
            paymentDate,
            method
        ) VALUES (
            7008,
            4004,
            -5000.00,  -- Violates CHECK (amount > 0)
            '2025-11-04',
            'Cash'
        );
    EXCEPTION WHEN others THEN
        RAISE NOTICE 'Payment INSERT failed: %', SQLERRM;
    END;
END;
$$;


SELECT COUNT(*) FROM Subscription;
SELECT COUNT(*) FROM Payment;

---\ PostgreSQL-Compatible Audit Table Definition
CREATE TABLE Subscription_AUDIT (
    bef_total INTEGER,              -- Total rows before change
    aft_total INTEGER,              -- Total rows after change
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- When the change occurred
    key_col VARCHAR(64)            -- Optional key or reason for change
);

---\ You can populate this table using triggers or manual logging
-- Example manual audit entry
INSERT INTO Subscription_AUDIT (
    bef_total,
    aft_total,
    key_col
) VALUES (
    (SELECT COUNT(*) FROM Subscription),
    (SELECT COUNT(*) FROM Subscription) + 1,
    'INSERT 4012'
);

---\ Create the Trigger Function
CREATE OR REPLACE FUNCTION recompute_subscription_totals()
RETURNS TRIGGER AS $$
BEGIN
    -- Recalculate total_paid for all affected subscriptions
    UPDATE Subscription s
    SET total_paid = COALESCE((
        SELECT SUM(p.amount)
        FROM Payment p
        WHERE p.subscriptionId = s.subscriptionId
    ), 0)
    WHERE s.subscriptionId IN (
        SELECT DISTINCT subscriptionId FROM Payment
    );

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

---\ Create the Statement-Level Trigger

CREATE TRIGGER trg_recompute_totals
AFTER INSERT OR UPDATE OR DELETE ON Payment
FOR EACH STATEMENT
EXECUTE FUNCTION recompute_subscription_totals();

---\ Add total_paid Column to Subscription
ALTER TABLE Subscription
ADD COLUMN total_paid NUMERIC DEFAULT 0;

---\  Execute a small mixed DML script on CHILD affecting at most 4 rows in total; ensure net 
---\ committed rows across the project remain ≤10.
---\ Create the CHILD Table
CREATE TABLE Child (
    childId INTEGER PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    age INTEGER CHECK (age > 0),
    subscriptionId INTEGER REFERENCES Subscription(subscriptionId)
);

--  Insert 2 valid rows
INSERT INTO Child (childId, name, age, subscriptionId)
VALUES
    (105, 'Aline', 8, 4004),
    (103, 'Eric', 10, 4005);

--  Update 1 row
UPDATE Child
SET age = age + 1
WHERE childId = 104;
--\ Delete 1 row
DELETE FROM Child
WHERE childId = 103;

-- Failing block (not committed)
BEGIN;

-- Invalid insert: NULL name
INSERT INTO Child (childId, name, age, subscriptionId)
VALUES (106, NULL, 8, 4005);

-- Invalid update: non-existent childId
UPDATE Child
SET age = 13
WHERE childId = 999;

ROLLBACK;
---\ 
SELECT column_name, is_nullable
FROM information_schema.columns
WHERE table_name = 'child';

---\. Log before/after totals to the audit table (2–3 audit rows).
    ---\  Confirm Audit Table Exists
	CREATE TABLE IF NOT EXISTS Subscription_AUDIT (
    bef_total INTEGER,
    aft_total INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64)
);

DROP TABLE IF EXISTS Subscription_AUDIT;

CREATE TABLE Subscription_AUDIT (
    bef_total INTEGER,
    aft_total INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    key_col VARCHAR(64)
);
---\ Skip creation and just insert audit rows
INSERT INTO Subscription_AUDIT (bef_total, aft_total, key_col)
VALUES (
    (SELECT COUNT(*) FROM Subscription),
    (SELECT COUNT(*) FROM Subscription) + 1,
    'INSERT 4006'
);

-- Audit 2: Before and after deleting a subscription
INSERT INTO Subscription_AUDIT (
    bef_total,
    aft_total,
    key_col
) VALUES (
    (SELECT COUNT(*) FROM Subscription),
    (SELECT COUNT(*) FROM Subscription) - 1,
    'DELETE 4005'
);

-- Audit 3: After Payment trigger recomputes totals
INSERT INTO Subscription_AUDIT (
    bef_total,
    aft_total,
    key_col
) VALUES (
    (SELECT COUNT(*) FROM Subscription),
    (SELECT COUNT(*) FROM Subscription),
    'AFTER Payment Trigger'
);

---\ View Audit Log
SELECT * FROM Subscription_AUDIT ORDER BY changed_at DESC;

---\ B8 :Recursive Hierarchy Roll-Up (6–10 rows)
---\  Create table HIER(parent_id, child_id) for a natural hierarchy (domain-specific).
CREATE TABLE HIER (
    parent_id INTEGER REFERENCES Member(memberId),
    child_id INTEGER PRIMARY KEY,
    FOREIGN KEY (child_id) REFERENCES Member(memberId)
);


INSERT INTO Member (memberId, fullname, joinDate, gender, city)
VALUES (305, 'Parent Account', '2025-10-01', 'female', 'Musanze');
INSERT INTO Member (memberId, fullname, joinDate, gender, city)
VALUES (401, 'Parent Account', '2025-10-01', 'male', 'huye');

SELECT * FROM Member;

INSERT INTO HIER (parent_id, child_id)
VALUES (305, 401);
UPDATE HIER
SET parent_id = 305
WHERE child_id = 401;

SELECT * FROM HIER WHERE child_id = 401;




INSERT INTO HIER (parent_id, child_id) VALUES
(305, 401),  -- Parent 305 sponsors child 401
(305, 402),
(401, 403),  -- Nested: 401 is parent of 403
(402, 404),
(310, 405),
(310, 406);

---\ Insert 6–10 rows forming a 3-level hierarchy.
SELECT column_name
FROM information_schema.columns
WHERE table_name = 'member';
SELECT * FROM Member;
ALTER TABLE Member
ADD COLUMN planType VARCHAR(20);
ALTER TABLE Member
ADD COLUMN status VARCHAR(20);

---\Insert Members into Member Table
INSERT INTO Member (memberId, fullname, joinDate, planType, status) VALUES
(100, 'Parent A', '2025-10-01', 'Monthly', 'Active'),
(101, 'Parent B', '2025-10-02', 'Monthly', 'Active'),
(200, 'Child A1', '2025-10-03', 'Monthly', 'Active'),
(201, 'Child A2', '2025-10-03', 'Monthly', 'Active'),
(202, 'Child B1', '2025-10-04', 'Monthly', 'Active'),
(300, 'Grandchild A1a', '2025-10-05', 'Monthly', 'Active'),
(301, 'Grandchild A2a', '2025-10-05', 'Monthly', 'Active'),
(302, 'Grandchild B1a', '2025-10-06', 'Monthly', 'Active');

---\ Insert Hierarchy into HIER Table
-- Level 1 → Level 2
INSERT INTO HIER (parent_id, child_id) VALUES
(100, 200),  -- Parent A → Child A1
(100, 201),  -- Parent A → Child A2
(101, 202);  -- Parent B → Child B1

-- Level 2 → Level 3
INSERT INTO HIER (parent_id, child_id) VALUES
(200, 300),  -- Child A1 → Grandchild A1a
(201, 301);  -- Child A2 → Grandchild A2a

INSERT INTO HIER (parent_id, child_id)
VALUES (202, 302);  -- Child B1 → Grandchild B1a

SELECT column_name
FROM information_schema.columns
WHERE table_name = 'attendance';
ALTER TABLE Attendance
ADD COLUMN member_id INTEGER REFERENCES Member(memberId);

ALTER TABLE Attendance
ADD COLUMN attended_on TIMESTAMP;


---\. Write a recursive WITH query to produce (child_id, root_id, depth) and join to Attendance or its 
---\ parent to compute rollups; return 6–10 rows total.
---\ Recursive Roll-Up with Attendance Join

WITH RECURSIVE hierarchy AS (
    -- Anchor: start with direct child-parent links
    SELECT child_id, parent_id AS root_id, 1 AS depth
    FROM HIER

    UNION ALL

    -- Recursive: walk up the hierarchy
    SELECT h.child_id, r.root_id, r.depth + 1
    FROM HIER h
    JOIN hierarchy r ON h.parent_id = r.child_id
),
rollup AS (
    SELECT h.child_id, h.root_id, h.depth, a.attended_on
    FROM hierarchy h
    JOIN Attendance a ON h.child_id = a.member_id  --  corrected column name
)
SELECT root_id,
       COUNT(DISTINCT child_id) AS total_members,
       COUNT(attended_on) AS total_attendance
FROM rollup
GROUP BY root_id
LIMIT 10;
---\  Create table TRIPLE(s VARCHAR2(64), p VARCHAR2(64), o VARCHAR2(64))
CREATE TABLE TRIPLE (
    s VARCHAR(64),
    p VARCHAR(64),
    o VARCHAR(64)
);

---\ . Insert 8–10 domain facts relevant to your project (e.g., simple type hierarchy or rule 
---\implications).
---\ SQL Inserts for TRIPLE Table
INSERT INTO TRIPLE (s, p, o) VALUES
('Member_100', 'hasPlan', 'Monthly'),
('Member_101', 'hasPlan', 'Annual'),
('Member_200', 'isChildOf', 'Member_100'),
('Member_201', 'isChildOf', 'Member_100'),
('Member_300', 'isChildOf', 'Member_200'),
('Member_301', 'isChildOf', 'Member_201'),
('Monthly', 'includesAccessTo', 'Gym'),
('Annual', 'includesAccessTo', 'Gym+Pool'),
('Member_101', 'attendedOn', '2025-10-01'),
('Member_301', 'attendedOn', '2025-10-03');

---\  Write a recursive inference query implementing transitive isA*; apply labels to base records and 
---\ return up to 10 labeled rows.
WITH RECURSIVE isa_chain AS (
    -- Base: direct isA relationships
    SELECT s AS subject, o AS label
    FROM TRIPLE
    WHERE p = 'isA'

    UNION

    -- Recursive: infer transitive isA (A isA B, B isA C → A isA C)
    SELECT base.subject, t.o AS label
    FROM isa_chain base
    JOIN TRIPLE t ON base.label = t.s
    WHERE t.p = 'isA'
)
SELECT DISTINCT subject, label
FROM isa_chain
LIMIT 10;

---\ input tuple
INSERT INTO TRIPLE (s, p, o) VALUES
('Member_100', 'isA', 'Person'),
('Person', 'isA', 'Entity'),
('Member_200', 'isA', 'Person'),
('Trainer_1', 'isA', 'Staff'),
('Staff', 'isA', 'Person'),
('Member_300', 'isA', 'Child'),
('Child', 'isA', 'Person');

---\  Ensure total committed rows across the project (including TRIPLE) remain ≤10; you may delete 
---\ temporary rows after demo if needed.
---\ Member Table — 3 rows
INSERT INTO Member (memberId, fullname, joinDate, planType, status) VALUES
(150, 'Parent A', '2025-10-01', 'Monthly', 'Active'),
(210, 'Child A1', '2025-10-02', 'Monthly', 'Active'),
(308, 'Grandchild A1a', '2025-10-03', 'Monthly', 'Active');

---\ HIER Table — 2 rows
INSERT INTO HIER (parent_id, child_id) VALUES
(150, 305),
(210, 308);
SELECT * FROM Member;
SELECT column_name, is_nullable
FROM information_schema.columns
WHERE table_name = 'attendance' AND column_name = 'sessionid';

INSERT INTO Attendance (sessionid, member_id, attended_on)
VALUES (100, 200, '2025-10-05');

INSERT INTO session (sessionId, trainerId, memberId, sessionDate, duration, type) VALUES
(1, 10, 201, '2025-10-01', 45, 'Yoga'),
(2, 1, 202, '2025-10-01', 45, 'Yoga'),
(3, 2, 203, '2025-10-02', 60, 'Cardio'),
(4, 2, 204, '2025-10-02', 60, 'Cardio'),
(5, 3, 205, '2025-10-03', 30, 'Strength'),
(6, 3, 206, '2025-10-03', 30, 'Strength'),
(7, 4, 207, '2025-10-04', 50, 'Zumba'),
(8, 10, 208, '2025-10-04', 50, 'Zumba'),
(9, 5, 209, '2025-10-05', 40, 'Pilates'),
(10, 5, 210, '2025-10-05', 40, 'Pilates'),
(11, 9, 211, '2025-10-06', 45, 'Yoga'),
(12, 10, 212, '2025-10-06', 45, 'Yoga'),
(13, 2, 213, '2025-10-07', 60, 'Cardio'),
(14, 2, 214, '2025-10-07', 60, 'Cardio'),
(15, 3, 215, '2025-10-08', 30, 'Strength'),
(16, 3, 216, '2025-10-08', 30, 'Strength'),
(17, 4, 217, '2025-10-09', 50, 'Zumba'),
(18, 4, 218, '2025-10-09', 50, 'Zumba'),
(19, 5, 219, '2025-10-10', 40, 'Pilates'),
(20, 10, 220, '2025-10-10', 40, 'Pilates');


INSERT INTO attendance (
    attendanceid, sessionid, status, checkintime, checkouttime, member_id, attended_on
) VALUES
(1, 1, 'Present', '2025-10-01 08:00:00+00', '2025-10-01 09:00:00+00', 201, '2025-10-01'),
(2, 1, 'Present', '2025-10-01 09:30:00+00', '2025-10-01 10:30:00+00', 20, '2025-10-01'),
(4, 2, 'Present', '2025-10-02 08:00:00+00', '2025-10-02 09:00:00+00', 4, '2025-10-02'),
(5, 2, 'Present', '2025-10-02 09:30:00+00', '2025-10-02 10:30:00+00', 20, '2025-10-02'),
(7, 3, 'Present', '2025-10-03 08:00:00+00', '2025-10-03 09:00:00+00', 7, '2025-10-03'),
(8, 3, 'Present', '2025-10-03 09:30:00+00', '2025-10-03 10:30:00+00', 8, '2025-10-03'),
(10, 4, 'Present', '2025-10-04 08:00:00+00', '2025-10-04 09:00:00+00', 210, '2025-10-04'),
(11, 4, 'Present', '2025-10-04 09:30:00+00', '2025-10-04 10:30:00+00', 11, '2025-10-04'),
(13, 5, 'Present', '2025-10-05 08:00:00+00', '2025-10-05 09:00:00+00', 13, '2025-10-05'),
(14, 5, 'Present', '2025-10-05 09:30:00+00', '2025-10-05 10:30:00+00', 14, '2025-10-05'),
(16, 6, 'Present', '2025-10-06 08:00:00+00', '2025-10-06 09:00:00+00', 16, '2025-10-06'),
(17, 6, 'Present', '2025-10-06 09:30:00+00', '2025-10-06 10:30:00+00', 17, '2025-10-06'),
(19, 7, 'Present', '2025-10-07 08:00:00+00', '2025-10-07 09:00:00+00', 19, '2025-10-07'),
(20, 7, 'Present', '2025-10-07 09:30:00+00', '2025-10-07 10:30:00+00', 210, '2025-10-07');


INSERT INTO TRIPLE (s, p, o) VALUES
('Member_100', 'isA', 'Person'),
('Person', 'isA', 'Entity'),
('Member_300', 'isA', 'Child');

---\ B10 :Business Limit Alert (Function + Trigger) (row-budget safe)

---\ Create the BUSINESS_LIMITS Table
CREATE TABLE BUSINESS_LIMITS (
    rule_key VARCHAR(64),
    threshold NUMERIC,
    active CHAR(1) CHECK (active IN ('Y', 'N'))
);
---\ Seed Exactly One Active Rule
INSERT INTO BUSINESS_LIMITS (rule_key, threshold, active)
VALUES ('MAX_SESSIONS_PER_DAY', 5, 'Y');


CREATE OR REPLACE FUNCTION fn_should_alert(member_id_input INTEGER)
RETURNS INTEGER AS $$
DECLARE
    rule_threshold INTEGER;
    session_count INTEGER;
BEGIN
    -- Get the active threshold for MAX_SESSIONS_PER_DAY
    SELECT threshold INTO rule_threshold
    FROM BUSINESS_LIMITS
    WHERE rule_key = 'MAX_SESSIONS_PER_DAY' AND active = 'Y';

    -- Count today's sessions for the member
    SELECT COUNT(*) INTO session_count
    FROM Subscription
    WHERE member_id = member_id_input AND session_date = CURRENT_DATE;

    -- Compare against threshold
    IF session_count > rule_threshold THEN
        RETURN 1;  -- Violation detected
    ELSE
        RETURN 0;  -- No violation
    END IF;
END;
$$ LANGUAGE plpgsql;

---\ . Create a BEFORE INSERT OR UPDATE trigger on Payment (or relevant table) that raises an 
---\application error when fn_should_alert returns 1.
CREATE OR REPLACE FUNCTION trg_check_payment_violation()
RETURNS TRIGGER AS $$
BEGIN
    -- Call the alert function with the incoming member_id
    IF fn_should_alert(NEW.member_id) = 1 THEN
        RAISE EXCEPTION 'Business rule violation: member % exceeded allowed limit.', NEW.member_id;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


CREATE TRIGGER check_payment_limit
BEFORE INSERT OR UPDATE ON Payment
FOR EACH ROW
EXECUTE FUNCTION trg_check_payment_violation();


---\. Demonstrate 2 failing and 2 passing DML cases; rollback the failing ones so total committed 
---\rows remain within the ≤10 budget.
---\ 2 Passing DML Cases
-- Case 1: Member 201 is within allowed limit
BEGIN;
    INSERT INTO Payment (paymentid, member_id, amount, paymentdate)
    VALUES (1, 201, 50.00, CURRENT_DATE);
COMMIT;
ALTER TABLE Payment
ADD CONSTRAINT fk_payment_member
FOREIGN KEY (member_id) REFERENCES Member(memberId);
SELECT * FROM Member WHERE memberId = 201;


SELECT * FROM payment;
ROLLBACK;
-- Case 2: Member 202 is also within allowed limit
BEGIN;
    INSERT INTO Payment (paymentid, member_id, amount, paymentdate)
    VALUES (2, 202, 75.00, CURRENT_DATE);
COMMIT;

-- Case 3: Member 203 exceeds session limit
BEGIN;
    BEGIN
        INSERT INTO Payment (paymentid, member_id, amount, paymentdate)
        VALUES (3, 203, 100.00, CURRENT_DATE);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE NOTICE 'Payment for member 203 rolled back due to rule violation.';
    END;
END;

-- Case 4: Member 204 also violates rule
BEGIN;
    BEGIN
        INSERT INTO Payment (payment_id, member_id, amount, payment_date)
        VALUES (4, 204, 120.00, CURRENT_DATE);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            RAISE NOTICE 'Payment for member 204 rolled back due to rule violation.';
    END;
END;























































SELECT usename FROM pg_user;








