-- =====================================================
-- SNOWFLAKE DATABASE REPLICATION - PRIMARY ACCOUNT SETUP
-- =====================================================
-- This script sets up the primary (source) account for database replication
-- Execute these commands in your PRIMARY Snowflake account

-- =====================================================
-- STEP 1: ENABLE REPLICATION FOR ACCOUNT
-- =====================================================
-- Note: Account admin privileges are required for this operation
-- This must be executed by an ACCOUNTADMIN role

USE ROLE ACCOUNTADMIN;

-- Enable replication for the account
-- This allows the account to participate in replication operations
ALTER ACCOUNT SET ENABLE_ACCOUNT_DATABASE_REPLICATION = TRUE;

-- Verify replication is enabled
SHOW PARAMETERS LIKE 'ENABLE_ACCOUNT_DATABASE_REPLICATION' IN ACCOUNT;

-- =====================================================
-- STEP 2: CREATE SAMPLE DATABASE FOR REPLICATION
-- =====================================================
-- Create a sample database that we'll replicate
-- Replace with your actual database name

CREATE DATABASE IF NOT EXISTS SALES_DB
    COMMENT = 'Primary database for replication to secondary account';

-- Use the database
USE DATABASE SALES_DB;

-- Create sample schema and objects
CREATE SCHEMA IF NOT EXISTS TRANSACTIONS
    COMMENT = 'Schema containing transaction data';

USE SCHEMA SALES_DB.TRANSACTIONS;

-- Create sample tables
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID NUMBER NOT NULL,
    ORDER_DATE DATE NOT NULL,
    TOTAL_AMOUNT DECIMAL(10,2) NOT NULL,
    STATUS VARCHAR(20) DEFAULT 'PENDING',
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    FIRST_NAME VARCHAR(50) NOT NULL,
    LAST_NAME VARCHAR(50) NOT NULL,
    EMAIL VARCHAR(100) UNIQUE NOT NULL,
    PHONE VARCHAR(20),
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample data
INSERT INTO CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, PHONE) VALUES
    ('John', 'Doe', 'john.doe@email.com', '555-0123'),
    ('Jane', 'Smith', 'jane.smith@email.com', '555-0124'),
    ('Bob', 'Johnson', 'bob.johnson@email.com', '555-0125');

INSERT INTO ORDERS (CUSTOMER_ID, ORDER_DATE, TOTAL_AMOUNT, STATUS) VALUES
    (1, '2023-01-15', 299.99, 'COMPLETED'),
    (2, '2023-01-16', 149.50, 'COMPLETED'),
    (3, '2023-01-17', 599.00, 'PENDING'),
    (1, '2023-01-18', 75.25, 'COMPLETED');

-- =====================================================
-- STEP 3: ENABLE DATABASE FOR REPLICATION
-- =====================================================
-- Enable the database for replication
-- This allows the database to be replicated to other accounts

ALTER DATABASE SALES_DB ENABLE REPLICATION TO ACCOUNTS ('TARGET_ACCOUNT_LOCATOR');

-- Note: Replace 'TARGET_ACCOUNT_LOCATOR' with the actual account locator of your target account
-- Example: 'AB12345.us-east-1' or 'myorg-targetaccount'

-- Verify replication is enabled for the database
SHOW DATABASES LIKE 'SALES_DB';

-- =====================================================
-- STEP 4: CREATE REPLICATION GROUP (OPTIONAL)
-- =====================================================
-- Replication groups allow you to replicate multiple databases together
-- This ensures consistency across related databases

CREATE REPLICATION GROUP IF NOT EXISTS SALES_REPLICATION_GROUP
    OBJECT_TYPES = ('DATABASES')
    ALLOWED_DATABASES = ('SALES_DB')
    ALLOWED_ACCOUNTS = ('TARGET_ACCOUNT_LOCATOR')
    COMMENT = 'Replication group for sales databases';

-- Add database to replication group
ALTER REPLICATION GROUP SALES_REPLICATION_GROUP 
    ADD ALLOWED_DATABASES ('SALES_DB');

-- =====================================================
-- STEP 5: GRANT PRIVILEGES FOR REPLICATION
-- =====================================================
-- Create a role for managing replication
CREATE ROLE IF NOT EXISTS REPLICATION_ADMIN
    COMMENT = 'Role for managing database replication';

-- Grant necessary privileges
GRANT USAGE ON DATABASE SALES_DB TO ROLE REPLICATION_ADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SALES_DB TO ROLE REPLICATION_ADMIN;
GRANT SELECT ON ALL TABLES IN DATABASE SALES_DB TO ROLE REPLICATION_ADMIN;
GRANT SELECT ON ALL VIEWS IN DATABASE SALES_DB TO ROLE REPLICATION_ADMIN;

-- Grant replication privileges
GRANT REPLICATE ON DATABASE SALES_DB TO ROLE REPLICATION_ADMIN;
GRANT MONITOR ON REPLICATION GROUP SALES_REPLICATION_GROUP TO ROLE REPLICATION_ADMIN;

-- Grant role to appropriate users
-- GRANT ROLE REPLICATION_ADMIN TO USER 'YOUR_USERNAME';

-- =====================================================
-- STEP 6: VERIFY PRIMARY ACCOUNT SETUP
-- =====================================================
-- Check account replication status
SELECT 
    'ACCOUNT_REPLICATION_ENABLED' as CHECK_TYPE,
    SYSTEM$GET_ACCOUNT_PARAMETER('ENABLE_ACCOUNT_DATABASE_REPLICATION') as STATUS;

-- Show databases enabled for replication
SHOW DATABASES;

-- Show replication groups
SHOW REPLICATION GROUPS;

-- Check database replication configuration
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    PRIMARY_DATABASE_NAME,
    REPLICATION_SCHEDULE,
    COMMENT
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME = 'SALES_DB';

-- =====================================================
-- STEP 7: GET ACCOUNT INFORMATION FOR SECONDARY SETUP
-- =====================================================
-- Get account locator for sharing with secondary account
SELECT 
    CURRENT_ACCOUNT() as ACCOUNT_LOCATOR,
    CURRENT_REGION() as REGION,
    CURRENT_ORGANIZATION_NAME() as ORGANIZATION;

-- Get replication configuration details
DESCRIBE REPLICATION GROUP SALES_REPLICATION_GROUP;

-- =====================================================
-- NOTES FOR PRIMARY ACCOUNT SETUP:
-- =====================================================
/*
1. ACCOUNT ADMIN REQUIRED: All replication setup commands require ACCOUNTADMIN privileges

2. ACCOUNT LOCATORS: 
   - Use the format 'ORGNAME-ACCOUNTNAME' for accounts in the same organization
   - Use 'ACCOUNT_LOCATOR.REGION' for accounts in different organizations

3. SUPPORTED OBJECTS:
   - Databases and all contained objects (schemas, tables, views, etc.)
   - User-defined functions and procedures
   - Sequences
   - File formats and stages

4. NOT REPLICATED:
   - Users, roles, and grants
   - Warehouses
   - Resource monitors
   - Network policies
   - Shares

5. BILLING:
   - Replication incurs compute costs for data transfer
   - Secondary databases are read-only and don't incur storage costs in target account

6. REFRESH FREQUENCY:
   - Manual refresh: On-demand using ALTER DATABASE...REFRESH
   - Scheduled refresh: Can be configured (daily, hourly, etc.)
*/