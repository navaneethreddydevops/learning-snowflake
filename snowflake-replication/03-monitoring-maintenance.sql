-- =====================================================
-- SNOWFLAKE DATABASE REPLICATION - MONITORING & MAINTENANCE
-- =====================================================
-- This script provides comprehensive monitoring and maintenance operations
-- for Snowflake database replication

-- =====================================================
-- SECTION 1: REPLICATION STATUS MONITORING
-- =====================================================

-- Switch to appropriate role
USE ROLE ACCOUNTADMIN;

-- 1.1 Overall Account Replication Status
SELECT 
    'Account Replication Status' as CHECK_TYPE,
    CURRENT_ACCOUNT() as ACCOUNT_LOCATOR,
    CURRENT_REGION() as REGION,
    SYSTEM$GET_ACCOUNT_PARAMETER('ENABLE_ACCOUNT_DATABASE_REPLICATION') as REPLICATION_ENABLED,
    CURRENT_TIMESTAMP() as CHECK_TIME;

-- 1.2 All Databases Replication Overview
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    PRIMARY_DATABASE_NAME,
    REPLICATION_SCHEDULE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    SECONDARY_STATE,
    CREATED,
    COMMENT
FROM INFORMATION_SCHEMA.DATABASES 
ORDER BY IS_PRIMARY DESC, DATABASE_NAME;

-- 1.3 Replication Groups Status
SHOW REPLICATION GROUPS;

-- 1.4 Detailed Replica Database Status
SELECT 
    DATABASE_NAME,
    CASE WHEN IS_PRIMARY = 'YES' THEN 'PRIMARY' ELSE 'SECONDARY' END as DATABASE_TYPE,
    PRIMARY_DATABASE_NAME,
    REPLICATION_SCHEDULE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    SECONDARY_STATE,
    DATEDIFF('MINUTE', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) as MINUTES_SINCE_LAST_REFRESH,
    CASE 
        WHEN NEXT_REFRESH_TIME IS NULL THEN 'MANUAL REFRESH ONLY'
        WHEN NEXT_REFRESH_TIME > CURRENT_TIMESTAMP() THEN 'SCHEDULED'
        ELSE 'OVERDUE'
    END as REFRESH_STATUS
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO' OR PRIMARY_DATABASE_NAME IS NOT NULL
ORDER BY DATABASE_NAME;

-- =====================================================
-- SECTION 2: REFRESH HISTORY AND PERFORMANCE
-- =====================================================

-- 2.1 Recent Refresh History (Last 24 hours)
SELECT 
    DATABASE_NAME,
    REFRESH_START_TIME,
    REFRESH_END_TIME,
    DATEDIFF('MINUTE', REFRESH_START_TIME, REFRESH_END_TIME) as REFRESH_DURATION_MINUTES,
    REFRESH_TRIGGER,
    BYTES_TRANSFERRED,
    BYTES_TRANSFERRED / (1024*1024*1024) as GB_TRANSFERRED,
    CREDITS_USED,
    REFRESH_STATUS,
    ERROR_MESSAGE
FROM INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY 
WHERE REFRESH_START_TIME >= DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
ORDER BY REFRESH_START_TIME DESC;

-- 2.2 Refresh Performance Summary (Last 7 days)
SELECT 
    DATABASE_NAME,
    COUNT(*) as TOTAL_REFRESHES,
    COUNT(CASE WHEN REFRESH_STATUS = 'SUCCESS' THEN 1 END) as SUCCESSFUL_REFRESHES,
    COUNT(CASE WHEN REFRESH_STATUS != 'SUCCESS' THEN 1 END) as FAILED_REFRESHES,
    ROUND(COUNT(CASE WHEN REFRESH_STATUS = 'SUCCESS' THEN 1 END) * 100.0 / COUNT(*), 2) as SUCCESS_RATE_PERCENT,
    AVG(DATEDIFF('MINUTE', REFRESH_START_TIME, REFRESH_END_TIME)) as AVG_DURATION_MINUTES,
    MAX(DATEDIFF('MINUTE', REFRESH_START_TIME, REFRESH_END_TIME)) as MAX_DURATION_MINUTES,
    SUM(BYTES_TRANSFERRED) / (1024*1024*1024) as TOTAL_GB_TRANSFERRED,
    SUM(CREDITS_USED) as TOTAL_CREDITS_USED
FROM INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY 
WHERE REFRESH_START_TIME >= DATEADD('DAY', -7, CURRENT_TIMESTAMP())
GROUP BY DATABASE_NAME
ORDER BY DATABASE_NAME;

-- 2.3 Failed Refresh Analysis
SELECT 
    DATABASE_NAME,
    REFRESH_START_TIME,
    REFRESH_TRIGGER,
    REFRESH_STATUS,
    ERROR_MESSAGE,
    DATEDIFF('MINUTE', REFRESH_START_TIME, REFRESH_END_TIME) as DURATION_MINUTES
FROM INFORMATION_SCHEMA.DATABASE_REFRESH_HISTORY 
WHERE REFRESH_STATUS != 'SUCCESS'
    AND REFRESH_START_TIME >= DATEADD('DAY', -30, CURRENT_TIMESTAMP())
ORDER BY REFRESH_START_TIME DESC;

-- =====================================================
-- SECTION 3: MANUAL REFRESH OPERATIONS
-- =====================================================

-- 3.1 Check Current Refresh Progress
-- Replace 'DATABASE_NAME' with your actual replica database name
SELECT SYSTEM$DATABASE_REFRESH_PROGRESS('SALES_DB_REPLICA');

-- 3.2 Manual Refresh Commands (Execute as needed)
-- Uncomment and modify the database name as needed

-- Single database refresh
-- ALTER DATABASE SALES_DB_REPLICA REFRESH;

-- Refresh with specific progress tracking
-- ALTER DATABASE SALES_DB_REPLICA REFRESH;
-- SELECT SYSTEM$DATABASE_REFRESH_PROGRESS('SALES_DB_REPLICA');

-- 3.3 Bulk Refresh for Multiple Replicas
-- This creates a script to refresh all replica databases
SELECT 
    'ALTER DATABASE ' || DATABASE_NAME || ' REFRESH;' as REFRESH_COMMAND
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO'
    AND SECONDARY_STATE = 'AVAILABLE'
ORDER BY DATABASE_NAME;

-- =====================================================
-- SECTION 4: DATA FRESHNESS MONITORING
-- =====================================================

-- 4.1 Data Freshness Check
-- Compare last refresh time with current time for all replicas
SELECT 
    DATABASE_NAME,
    LAST_REFRESH_TIME,
    CURRENT_TIMESTAMP() as CURRENT_TIME,
    DATEDIFF('MINUTE', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) as MINUTES_BEHIND,
    DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) as HOURS_BEHIND,
    CASE 
        WHEN LAST_REFRESH_TIME IS NULL THEN 'NEVER REFRESHED'
        WHEN DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 24 THEN 'STALE (>24H)'
        WHEN DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 12 THEN 'OLD (>12H)'
        WHEN DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 4 THEN 'AGING (>4H)'
        ELSE 'FRESH'
    END as FRESHNESS_STATUS
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO'
ORDER BY LAST_REFRESH_TIME DESC;

-- 4.2 Data Comparison Between Primary and Replica
-- This requires access to both primary and replica databases
-- Modify table names and database names as needed

/*
-- Example data comparison query (run in secondary account)
USE DATABASE SALES_DB_REPLICA;
USE SCHEMA TRANSACTIONS;

SELECT 
    'REPLICA' as SOURCE,
    COUNT(*) as RECORD_COUNT,
    MAX(CREATED_AT) as LATEST_RECORD,
    MIN(CREATED_AT) as EARLIEST_RECORD
FROM ORDERS

UNION ALL

-- Run equivalent query in primary account and compare results
SELECT 
    'PRIMARY' as SOURCE,
    COUNT(*) as RECORD_COUNT,
    MAX(CREATED_AT) as LATEST_RECORD,
    MIN(CREATED_AT) as EARLIEST_RECORD
FROM PRIMARY_ACCOUNT.SALES_DB.TRANSACTIONS.ORDERS;
*/

-- =====================================================
-- SECTION 5: TROUBLESHOOTING QUERIES
-- =====================================================

-- 5.1 Identify Databases with Refresh Issues
SELECT 
    DATABASE_NAME,
    SECONDARY_STATE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    CASE 
        WHEN SECONDARY_STATE != 'AVAILABLE' THEN 'STATE_ISSUE'
        WHEN LAST_REFRESH_TIME IS NULL THEN 'NEVER_REFRESHED'
        WHEN NEXT_REFRESH_TIME IS NOT NULL AND NEXT_REFRESH_TIME < CURRENT_TIMESTAMP() THEN 'OVERDUE_REFRESH'
        WHEN DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 24 THEN 'STALE_DATA'
        ELSE 'OK'
    END as ISSUE_TYPE
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO'
    AND (SECONDARY_STATE != 'AVAILABLE' 
         OR LAST_REFRESH_TIME IS NULL 
         OR (NEXT_REFRESH_TIME IS NOT NULL AND NEXT_REFRESH_TIME < CURRENT_TIMESTAMP())
         OR DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 24)
ORDER BY DATABASE_NAME;

-- 5.2 Check for Privilege Issues
-- Verify that necessary privileges are in place
SHOW GRANTS ON DATABASE SALES_DB_REPLICA;

-- 5.3 Network and Connectivity Check
-- Check if there are any network policies that might affect replication
SHOW NETWORK POLICIES;

-- =====================================================
-- SECTION 6: MAINTENANCE OPERATIONS
-- =====================================================

-- 6.1 Update Refresh Schedules
-- Examples of modifying refresh intervals

-- Set hourly refresh
-- ALTER DATABASE SALES_DB_REPLICA SET REFRESH_INTERVAL = 60;

-- Set daily refresh
-- ALTER DATABASE SALES_DB_REPLICA SET REFRESH_INTERVAL = 1440;

-- Disable automatic refresh (manual only)
-- ALTER DATABASE SALES_DB_REPLICA UNSET REFRESH_INTERVAL;

-- 6.2 Replica Database Maintenance
-- Check and optimize replica database settings

SELECT 
    DATABASE_NAME,
    'ALTER DATABASE ' || DATABASE_NAME || ' SET REFRESH_INTERVAL = 60;' as HOURLY_REFRESH_CMD,
    'ALTER DATABASE ' || DATABASE_NAME || ' SET REFRESH_INTERVAL = 1440;' as DAILY_REFRESH_CMD,
    'ALTER DATABASE ' || DATABASE_NAME || ' UNSET REFRESH_INTERVAL;' as MANUAL_REFRESH_CMD
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO'
ORDER BY DATABASE_NAME;

-- =====================================================
-- SECTION 7: ALERTING AND NOTIFICATIONS
-- =====================================================

-- 7.1 Create View for Monitoring Dashboard
CREATE OR REPLACE VIEW REPLICATION_MONITORING_DASHBOARD AS
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    PRIMARY_DATABASE_NAME,
    SECONDARY_STATE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    DATEDIFF('MINUTE', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) as MINUTES_SINCE_REFRESH,
    CASE 
        WHEN IS_PRIMARY = 'YES' THEN 'PRIMARY_DB'
        WHEN SECONDARY_STATE != 'AVAILABLE' THEN 'REPLICA_UNAVAILABLE'
        WHEN LAST_REFRESH_TIME IS NULL THEN 'NEVER_REFRESHED'
        WHEN DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 24 THEN 'STALE_DATA'
        WHEN NEXT_REFRESH_TIME IS NOT NULL AND NEXT_REFRESH_TIME < CURRENT_TIMESTAMP() THEN 'REFRESH_OVERDUE'
        ELSE 'HEALTHY'
    END as STATUS,
    CURRENT_TIMESTAMP() as CHECK_TIME
FROM INFORMATION_SCHEMA.DATABASES 
ORDER BY IS_PRIMARY DESC, DATABASE_NAME;

-- 7.2 Query for Automated Monitoring
-- This can be used by external monitoring systems
SELECT 
    COUNT(*) as TOTAL_REPLICAS,
    COUNT(CASE WHEN STATUS = 'HEALTHY' THEN 1 END) as HEALTHY_REPLICAS,
    COUNT(CASE WHEN STATUS != 'HEALTHY' AND STATUS != 'PRIMARY_DB' THEN 1 END) as UNHEALTHY_REPLICAS,
    LISTAGG(CASE WHEN STATUS != 'HEALTHY' AND STATUS != 'PRIMARY_DB' THEN DATABASE_NAME || '(' || STATUS || ')' END, ', ') as UNHEALTHY_DATABASES
FROM REPLICATION_MONITORING_DASHBOARD
WHERE IS_PRIMARY = 'NO';

-- =====================================================
-- SECTION 8: CLEANUP AND HOUSEKEEPING
-- =====================================================

-- 8.1 List All Replication-Related Objects
SELECT 'DATABASE' as OBJECT_TYPE, DATABASE_NAME as OBJECT_NAME, COMMENT
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO' OR PRIMARY_DATABASE_NAME IS NOT NULL

UNION ALL

SELECT 'REPLICATION_GROUP' as OBJECT_TYPE, NAME as OBJECT_NAME, COMMENT
FROM (SHOW REPLICATION GROUPS);

-- 8.2 Generate Cleanup Commands (if needed)
-- Commands to drop replica databases (USE WITH CAUTION)
SELECT 
    'DROP DATABASE ' || DATABASE_NAME || ';' as CLEANUP_COMMAND
FROM INFORMATION_SCHEMA.DATABASES 
WHERE IS_PRIMARY = 'NO'
    AND DATABASE_NAME LIKE '%REPLICA%'
ORDER BY DATABASE_NAME;

-- =====================================================
-- SCHEDULED MONITORING SCRIPT
-- =====================================================
-- This section provides a template for regular monitoring

-- Create a stored procedure for regular monitoring
CREATE OR REPLACE PROCEDURE MONITOR_REPLICATION_HEALTH()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    result_summary STRING;
    unhealthy_count INTEGER;
BEGIN
    -- Count unhealthy replicas
    SELECT COUNT(*) INTO unhealthy_count
    FROM REPLICATION_MONITORING_DASHBOARD
    WHERE IS_PRIMARY = 'NO' AND STATUS != 'HEALTHY';
    
    -- Generate summary message
    IF (unhealthy_count > 0) THEN
        result_summary := 'ALERT: ' || unhealthy_count || ' replica database(s) require attention. Check REPLICATION_MONITORING_DASHBOARD view for details.';
    ELSE
        result_summary := 'SUCCESS: All replica databases are healthy.';
    END IF;
    
    RETURN result_summary;
END;
$$;

-- Test the monitoring procedure
CALL MONITOR_REPLICATION_HEALTH();

-- =====================================================
-- NOTES FOR MONITORING AND MAINTENANCE:
-- =====================================================
/*
1. REGULAR MONITORING:
   - Check replication status daily
   - Monitor refresh success rates
   - Track data freshness requirements
   - Review credit usage for replication

2. ALERTING THRESHOLDS:
   - Failed refreshes: Alert immediately
   - Stale data: Alert if >4-24 hours (based on requirements)
   - High credit usage: Alert if >expected baseline

3. PERFORMANCE OPTIMIZATION:
   - Optimize refresh schedules based on usage patterns
   - Monitor bandwidth usage for cross-region replication
   - Consider replication groups for related databases

4. DISASTER RECOVERY:
   - Test failover procedures regularly
   - Document failback procedures
   - Monitor primary account health

5. MAINTENANCE WINDOWS:
   - Plan maintenance during low-usage periods
   - Coordinate with primary account maintenance
   - Test refresh after primary account changes

6. TROUBLESHOOTING CHECKLIST:
   - Verify account replication is enabled
   - Check network connectivity between accounts
   - Verify privileges and roles
   - Review error messages in refresh history
   - Check primary database changes that might affect replication
*/