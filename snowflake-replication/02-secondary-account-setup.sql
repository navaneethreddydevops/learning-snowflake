-- =====================================================
-- SNOWFLAKE DATABASE REPLICATION - SECONDARY ACCOUNT SETUP
-- =====================================================
-- This script sets up the secondary (target) account for database replication
-- Execute these commands in your SECONDARY Snowflake account

-- =====================================================
-- STEP 1: ENABLE REPLICATION FOR ACCOUNT
-- =====================================================
-- Note: Account admin privileges are required for this operation
-- This must be executed by an ACCOUNTADMIN role

USE ROLE ACCOUNTADMIN;

-- Enable replication for the account
ALTER ACCOUNT SET ENABLE_ACCOUNT_DATABASE_REPLICATION = TRUE;

-- Verify replication is enabled
SHOW PARAMETERS LIKE 'ENABLE_ACCOUNT_DATABASE_REPLICATION' IN ACCOUNT;

-- =====================================================
-- STEP 2: CREATE SECONDARY DATABASE FROM PRIMARY
-- =====================================================
-- Create a secondary database that replicates from the primary account
-- Replace placeholders with actual values from your primary account

-- Syntax: CREATE DATABASE <secondary_db_name> AS REPLICA OF <primary_account>.<primary_db_name>
CREATE DATABASE SALES_DB_REPLICA AS REPLICA OF 'PRIMARY_ACCOUNT_LOCATOR.SALES_DB'
    COMMENT = 'Secondary replica of SALES_DB from primary account';

-- Alternative syntax with refresh schedule
CREATE DATABASE SALES_DB_REPLICA_SCHEDULED AS REPLICA OF 'PRIMARY_ACCOUNT_LOCATOR.SALES_DB'
    REFRESH_INTERVAL = 60 -- Refresh every 60 minutes
    COMMENT = 'Secondary replica with scheduled refresh';

-- =====================================================
-- STEP 3: VERIFY SECONDARY DATABASE CREATION
-- =====================================================
-- Check the secondary database status
SHOW DATABASES LIKE '%REPLICA%';

-- Get detailed information about the replica
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    PRIMARY_DATABASE_NAME,
    REPLICATION_SCHEDULE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    COMMENT
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME LIKE '%REPLICA%';

-- =====================================================
-- STEP 4: MANUAL REFRESH OF SECONDARY DATABASE
-- =====================================================
-- Perform manual refresh to get latest data from primary
ALTER DATABASE SALES_DB_REPLICA REFRESH;

-- Check refresh status
SELECT SYSTEM$DATABASE_REFRESH_PROGRESS('SALES_DB_REPLICA');

-- =====================================================
-- STEP 5: CONFIGURE REFRESH SCHEDULES
-- =====================================================
-- Set up automatic refresh schedule (if not set during creation)
ALTER DATABASE SALES_DB_REPLICA SET REFRESH_INTERVAL = 60; -- Every hour

-- Set up daily refresh at specific time
ALTER DATABASE SALES_DB_REPLICA SET REFRESH_INTERVAL = 1440; -- Daily (1440 minutes)

-- Disable automatic refresh (manual only)
ALTER DATABASE SALES_DB_REPLICA UNSET REFRESH_INTERVAL;

-- =====================================================
-- STEP 6: CREATE ROLES AND GRANT PERMISSIONS
-- =====================================================
-- Create roles for accessing replicated data
CREATE ROLE IF NOT EXISTS REPLICA_READER
    COMMENT = 'Role for reading data from replica databases';

CREATE ROLE IF NOT EXISTS REPLICA_ADMIN
    COMMENT = 'Role for managing replica databases';

-- Grant permissions to access replica database
GRANT USAGE ON DATABASE SALES_DB_REPLICA TO ROLE REPLICA_READER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE SALES_DB_REPLICA TO ROLE REPLICA_READER;
GRANT SELECT ON ALL TABLES IN DATABASE SALES_DB_REPLICA TO ROLE REPLICA_READER;
GRANT SELECT ON ALL VIEWS IN DATABASE SALES_DB_REPLICA TO ROLE REPLICA_READER;

-- Grant admin permissions for managing replica
GRANT ALL ON DATABASE SALES_DB_REPLICA TO ROLE REPLICA_ADMIN;
GRANT MONITOR ON DATABASE SALES_DB_REPLICA TO ROLE REPLICA_ADMIN;

-- Grant roles to users
-- GRANT ROLE REPLICA_READER TO USER 'READ_ONLY_USER';
-- GRANT ROLE REPLICA_ADMIN TO USER 'ADMIN_USER';

-- =====================================================
-- STEP 7: CREATE WAREHOUSES FOR ACCESSING REPLICA DATA
-- =====================================================
-- Create dedicated warehouse for replica queries
CREATE WAREHOUSE IF NOT EXISTS REPLICA_WH WITH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for querying replica databases';

-- Grant warehouse usage to replica roles
GRANT USAGE ON WAREHOUSE REPLICA_WH TO ROLE REPLICA_READER;
GRANT USAGE ON WAREHOUSE REPLICA_WH TO ROLE REPLICA_ADMIN;

-- =====================================================
-- STEP 8: TEST REPLICA DATABASE ACCESS
-- =====================================================
-- Switch to replica reader role and test access
USE ROLE REPLICA_READER;
USE WAREHOUSE REPLICA_WH;
USE DATABASE SALES_DB_REPLICA;
USE SCHEMA TRANSACTIONS;

-- Test queries on replicated data
SELECT COUNT(*) as TOTAL_CUSTOMERS FROM CUSTOMERS;
SELECT COUNT(*) as TOTAL_ORDERS FROM ORDERS;

-- Test data freshness
SELECT 
    MAX(CREATED_AT) as LATEST_CUSTOMER_RECORD,
    MIN(CREATED_AT) as EARLIEST_CUSTOMER_RECORD
FROM CUSTOMERS;

SELECT 
    MAX(ORDER_DATE) as LATEST_ORDER,
    MIN(ORDER_DATE) as EARLIEST_ORDER,
    COUNT(*) as TOTAL_ORDERS
FROM ORDERS;

-- =====================================================
-- STEP 9: MONITOR REPLICATION STATUS
-- =====================================================
-- Switch back to admin role for monitoring
USE ROLE ACCOUNTADMIN;

-- Check database replication status
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    PRIMARY_DATABASE_NAME,
    REPLICATION_SCHEDULE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    SECONDARY_STATE
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO';

-- Check refresh history
SELECT 
    DATABASE_NAME,
    REFRESH_START_TIME,
    REFRESH_END_TIME,
    REFRESH_TRIGGER,
    BYTES_TRANSFERRED,
    CREDITS_USED,
    REFRESH_STATUS
FROM INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY 
WHERE DATABASE_NAME = 'SALES_DB_REPLICA'
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;

-- =====================================================
-- STEP 10: ADVANCED REPLICATION CONFIGURATIONS
-- =====================================================

-- Create multiple replicas with different refresh schedules
CREATE DATABASE SALES_DB_HOURLY AS REPLICA OF 'PRIMARY_ACCOUNT_LOCATOR.SALES_DB'
    REFRESH_INTERVAL = 60
    COMMENT = 'Hourly replica for near real-time analytics';

CREATE DATABASE SALES_DB_DAILY AS REPLICA OF 'PRIMARY_ACCOUNT_LOCATOR.SALES_DB'
    REFRESH_INTERVAL = 1440
    COMMENT = 'Daily replica for batch reporting';

-- Create replica from replication group
CREATE DATABASE SALES_GROUP_REPLICA AS REPLICA OF REPLICATION GROUP 'PRIMARY_ACCOUNT_LOCATOR.SALES_REPLICATION_GROUP'
    COMMENT = 'Replica of entire replication group';

-- =====================================================
-- STEP 11: FAILOVER PREPARATION (DISASTER RECOVERY)
-- =====================================================
-- For disaster recovery scenarios, prepare to promote replica to primary

-- Check if replica can be promoted
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    SECONDARY_STATE,
    FAILOVER_ALLOWED
FROM INFORMATION_SCHEMA.DATABASES 
WHERE DATABASE_NAME = 'SALES_DB_REPLICA';

-- Promote replica to primary (USE ONLY IN DISASTER RECOVERY)
-- ALTER DATABASE SALES_DB_REPLICA ENABLE FAILOVER TO ACCOUNTS ('FAILBACK_ACCOUNT_LOCATOR');

-- =====================================================
-- VERIFICATION QUERIES
-- =====================================================
-- Summary of all replica databases in account
SELECT 
    DATABASE_NAME,
    PRIMARY_DATABASE_NAME,
    REPLICATION_SCHEDULE,
    LAST_REFRESH_TIME,
    SECONDARY_STATE,
    COMMENT
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO'
ORDER BY DATABASE_NAME;

-- Check account replication settings
SELECT 
    'ACCOUNT_REPLICATION_ENABLED' as SETTING,
    SYSTEM$GET_ACCOUNT_PARAMETER('ENABLE_ACCOUNT_DATABASE_REPLICATION') as VALUE;

-- =====================================================
-- NOTES FOR SECONDARY ACCOUNT SETUP:
-- =====================================================
/*
1. ACCOUNT LOCATOR FORMAT:
   - Same organization: 'ORGNAME-ACCOUNTNAME'
   - Different organization: 'ACCOUNT_LOCATOR.REGION'
   - Example: 'myorg-prodaccount' or 'AB12345.us-east-1'

2. REPLICA LIMITATIONS:
   - Secondary databases are READ-ONLY
   - Cannot create new objects in replica databases
   - Cannot modify data in replica databases
   - All writes must happen in primary database

3. REFRESH INTERVALS:
   - Minimum: 1 minute (for Enterprise edition)
   - Maximum: No limit
   - Consider costs vs. data freshness requirements

4. DISASTER RECOVERY:
   - Replicas can be promoted to primary during outages
   - Plan failover and failback procedures
   - Test disaster recovery scenarios regularly

5. MONITORING:
   - Monitor refresh success/failure
   - Track data freshness
   - Monitor credit usage for replication

6. SECURITY:
   - Replica inherits security settings from primary
   - Row-level security and column-level security are replicated
   - User access must be configured separately in secondary account

7. BILLING:
   - Replication incurs compute costs during refresh
   - Storage costs are minimal (metadata only)
   - Data transfer costs may apply for cross-region replication
*/