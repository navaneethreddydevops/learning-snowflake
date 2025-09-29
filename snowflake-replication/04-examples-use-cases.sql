-- =====================================================
-- SNOWFLAKE REPLICATION EXAMPLES AND USE CASES
-- =====================================================
-- This script provides practical examples and common use cases
-- for Snowflake database replication

-- =====================================================
-- EXAMPLE 1: DISASTER RECOVERY SETUP
-- =====================================================

-- Primary Account (Production - US East)
USE ROLE ACCOUNTADMIN;

-- Create production database
CREATE DATABASE PROD_SALES_DB
    COMMENT = 'Production sales database in US East region';

-- Enable for replication to DR site
ALTER DATABASE PROD_SALES_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-dr-uswest');

-- Secondary Account (DR - US West)
-- Execute in DR account
CREATE DATABASE PROD_SALES_DB_DR AS REPLICA OF 'myorg-prod-useast.PROD_SALES_DB'
    REFRESH_INTERVAL = 15 -- 15 minutes for near real-time DR
    COMMENT = 'Disaster recovery replica in US West';

-- Test failover capability
-- In DR account, promote to primary during disaster
-- ALTER DATABASE PROD_SALES_DB_DR ENABLE FAILOVER TO ACCOUNTS ('myorg-prod-useast');

-- =====================================================
-- EXAMPLE 2: ANALYTICS SEPARATION
-- =====================================================

-- Production Account
-- Enable replication to analytics account
ALTER DATABASE CUSTOMER_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-analytics');

-- Analytics Account
-- Create replica for analytical workloads
CREATE DATABASE CUSTOMER_DB_ANALYTICS AS REPLICA OF 'myorg-prod.CUSTOMER_DB'
    REFRESH_INTERVAL = 360 -- 6 hours, sufficient for most analytics
    COMMENT = 'Customer database replica for analytics and reporting';

-- Create analytics-specific views
USE DATABASE CUSTOMER_DB_ANALYTICS;
USE SCHEMA PUBLIC;

CREATE OR REPLACE VIEW CUSTOMER_ANALYTICS AS
SELECT 
    CUSTOMER_ID,
    CUSTOMER_SEGMENT,
    REGISTRATION_DATE,
    LAST_PURCHASE_DATE,
    TOTAL_LIFETIME_VALUE,
    COUNTRY,
    REGION
FROM CUSTOMERS
WHERE STATUS = 'ACTIVE';

-- Create aggregated reporting views
CREATE OR REPLACE VIEW MONTHLY_CUSTOMER_METRICS AS
SELECT 
    DATE_TRUNC('MONTH', REGISTRATION_DATE) as MONTH,
    CUSTOMER_SEGMENT,
    COUNTRY,
    COUNT(*) as NEW_CUSTOMERS,
    AVG(TOTAL_LIFETIME_VALUE) as AVG_LTV,
    SUM(TOTAL_LIFETIME_VALUE) as TOTAL_LTV
FROM CUSTOMERS
WHERE REGISTRATION_DATE >= DATEADD('YEAR', -2, CURRENT_DATE())
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2, 3;

-- =====================================================
-- EXAMPLE 3: DEVELOPMENT AND TESTING
-- =====================================================

-- Production Account
-- Enable replication to dev/test environments
ALTER DATABASE SALES_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-dev', 'myorg-test');

-- Development Account
-- Create replica with daily refresh (cost optimization)
CREATE DATABASE SALES_DB_DEV AS REPLICA OF 'myorg-prod.SALES_DB'
    REFRESH_INTERVAL = 1440 -- Daily refresh for development
    COMMENT = 'Development replica of sales database';

-- Test Account
-- Create replica with weekly refresh
CREATE DATABASE SALES_DB_TEST AS REPLICA OF 'myorg-prod.SALES_DB'
    COMMENT = 'Test replica of sales database - manual refresh only';

-- Manual refresh for testing cycles
-- ALTER DATABASE SALES_DB_TEST REFRESH;

-- =====================================================
-- EXAMPLE 4: MULTI-REGION DEPLOYMENT
-- =====================================================

-- Global setup with multiple regional replicas

-- Primary Account (Global HQ)
CREATE DATABASE GLOBAL_CUSTOMER_DB
    COMMENT = 'Global customer database - primary';

-- Enable replication to all regions
ALTER DATABASE GLOBAL_CUSTOMER_DB ENABLE REPLICATION TO ACCOUNTS (
    'myorg-europe',
    'myorg-asia',
    'myorg-americas'
);

-- European Account
CREATE DATABASE GLOBAL_CUSTOMER_DB_EU AS REPLICA OF 'myorg-global.GLOBAL_CUSTOMER_DB'
    REFRESH_INTERVAL = 120 -- 2 hours
    COMMENT = 'European replica for low-latency access';

-- Asian Account
CREATE DATABASE GLOBAL_CUSTOMER_DB_ASIA AS REPLICA OF 'myorg-global.GLOBAL_CUSTOMER_DB'
    REFRESH_INTERVAL = 120 -- 2 hours
    COMMENT = 'Asian replica for low-latency access';

-- Americas Account
CREATE DATABASE GLOBAL_CUSTOMER_DB_AMERICAS AS REPLICA OF 'myorg-global.GLOBAL_CUSTOMER_DB'
    REFRESH_INTERVAL = 60 -- 1 hour (closer to primary)
    COMMENT = 'Americas replica for low-latency access';

-- =====================================================
-- EXAMPLE 5: REPLICATION GROUPS FOR RELATED DATABASES
-- =====================================================

-- Primary Account
-- Create replication group for related databases
CREATE REPLICATION GROUP FINANCIAL_DATA_GROUP
    OBJECT_TYPES = ('DATABASES')
    ALLOWED_DATABASES = ('ACCOUNTING_DB', 'PAYROLL_DB', 'BUDGET_DB')
    ALLOWED_ACCOUNTS = ('myorg-finance-replica')
    COMMENT = 'Group for all financial databases';

-- Enable databases for replication
ALTER DATABASE ACCOUNTING_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-finance-replica');
ALTER DATABASE PAYROLL_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-finance-replica');
ALTER DATABASE BUDGET_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-finance-replica');

-- Add databases to replication group
ALTER REPLICATION GROUP FINANCIAL_DATA_GROUP 
    ADD ALLOWED_DATABASES ('ACCOUNTING_DB', 'PAYROLL_DB', 'BUDGET_DB');

-- Secondary Account
-- Create replica from replication group (ensures consistency)
CREATE DATABASE FINANCIAL_REPLICA AS REPLICA OF REPLICATION GROUP 'myorg-primary.FINANCIAL_DATA_GROUP'
    REFRESH_INTERVAL = 240 -- 4 hours
    COMMENT = 'Replica of all financial databases';

-- =====================================================
-- EXAMPLE 6: STAGED DATA PIPELINE
-- =====================================================

-- Use case: Raw -> Staging -> Analytics pipeline with replication

-- Raw Data Account (Data Lake)
CREATE DATABASE RAW_DATA_DB
    COMMENT = 'Raw data from various sources';

-- Enable replication to staging
ALTER DATABASE RAW_DATA_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-staging');

-- Staging Account
CREATE DATABASE RAW_DATA_STAGING AS REPLICA OF 'myorg-raw.RAW_DATA_DB'
    REFRESH_INTERVAL = 30 -- 30 minutes for near real-time staging
    COMMENT = 'Staging replica of raw data';

-- Create transformed/curated data
CREATE DATABASE CURATED_DATA_DB
    COMMENT = 'Curated and transformed data';

-- Process data from replica
CREATE OR REPLACE TASK DAILY_DATA_PROCESSING
    WAREHOUSE = 'PROCESSING_WH'
    SCHEDULE = 'USING CRON 0 2 * * * UTC' -- Daily at 2 AM UTC
AS
    INSERT INTO CURATED_DATA_DB.ANALYTICS.DAILY_METRICS
    SELECT 
        DATE_TRUNC('DAY', CREATED_AT) as DATE,
        SOURCE_SYSTEM,
        COUNT(*) as RECORD_COUNT,
        SUM(AMOUNT) as TOTAL_AMOUNT
    FROM RAW_DATA_STAGING.PUBLIC.TRANSACTIONS
    WHERE CREATED_AT >= DATEADD('DAY', -1, CURRENT_DATE())
    GROUP BY 1, 2;

-- Enable curated data for replication to analytics
ALTER DATABASE CURATED_DATA_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-analytics');

-- =====================================================
-- EXAMPLE 7: COMPLIANCE AND GOVERNANCE
-- =====================================================

-- Use case: Data residency and compliance requirements

-- Primary Account (Global)
CREATE DATABASE CUSTOMER_DATA_GLOBAL
    COMMENT = 'Global customer data with residency requirements';

-- Create region-specific schemas
CREATE SCHEMA CUSTOMER_DATA_GLOBAL.EU_CUSTOMERS
    COMMENT = 'European customer data';

CREATE SCHEMA CUSTOMER_DATA_GLOBAL.US_CUSTOMERS
    COMMENT = 'US customer data';

CREATE SCHEMA CUSTOMER_DATA_GLOBAL.ASIA_CUSTOMERS
    COMMENT = 'Asian customer data';

-- EU Account (GDPR Compliance)
-- Only replicate EU customer data
CREATE DATABASE EU_CUSTOMER_DATA AS REPLICA OF 'myorg-global.CUSTOMER_DATA_GLOBAL'
    COMMENT = 'EU-only replica for GDPR compliance';

-- Create filtered views for compliance
CREATE OR REPLACE VIEW EU_CUSTOMER_DATA.PUBLIC.EU_CUSTOMERS_ONLY AS
SELECT * FROM EU_CUSTOMER_DATA.EU_CUSTOMERS.CUSTOMER_DETAILS
WHERE GDPR_CONSENT = TRUE AND DATA_RESIDENCY = 'EU';

-- =====================================================
-- EXAMPLE 8: BACKUP AND ARCHIVAL STRATEGY
-- =====================================================

-- Long-term backup strategy using replication

-- Primary Account
CREATE DATABASE TRANSACTIONAL_DB
    COMMENT = 'High-frequency transactional database';

-- Enable replication to backup account
ALTER DATABASE TRANSACTIONAL_DB ENABLE REPLICATION TO ACCOUNTS ('myorg-backup');

-- Backup Account
-- Create replica for backup purposes
CREATE DATABASE TRANSACTIONAL_DB_BACKUP AS REPLICA OF 'myorg-prod.TRANSACTIONAL_DB'
    REFRESH_INTERVAL = 720 -- 12 hours for backup purposes
    COMMENT = 'Backup replica with extended retention';

-- Create time-travel extended backup
CREATE OR REPLACE TABLE TRANSACTIONAL_DB_BACKUP.ARCHIVE.HISTORICAL_TRANSACTIONS
    CLUSTER BY (TRANSACTION_DATE)
AS
SELECT 
    *,
    CURRENT_TIMESTAMP() as BACKUP_TIMESTAMP
FROM TRANSACTIONAL_DB_BACKUP.PUBLIC.TRANSACTIONS;

-- =====================================================
-- EXAMPLE 9: CROSS-ORGANIZATIONAL DATA SHARING
-- =====================================================

-- Share data with partner organizations

-- Your Organization Account
CREATE DATABASE PARTNER_SHARED_DATA
    COMMENT = 'Data shared with business partners';

-- Create curated views for external sharing
CREATE SCHEMA PARTNER_SHARED_DATA.PUBLIC_VIEWS;

CREATE OR REPLACE VIEW PARTNER_SHARED_DATA.PUBLIC_VIEWS.PRODUCT_CATALOG AS
SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    LIST_PRICE,
    AVAILABILITY_STATUS
FROM INTERNAL_PRODUCT_DB.CATALOG.PRODUCTS
WHERE IS_PUBLIC = TRUE;

-- Enable replication to partner account
ALTER DATABASE PARTNER_SHARED_DATA ENABLE REPLICATION TO ACCOUNTS ('partner-org-account');

-- Partner Organization Account
CREATE DATABASE YOUR_ORG_SHARED_DATA AS REPLICA OF 'your-org-account.PARTNER_SHARED_DATA'
    REFRESH_INTERVAL = 60
    COMMENT = 'Shared data from business partner';

-- =====================================================
-- EXAMPLE 10: MONITORING AND ALERTING SETUP
-- =====================================================

-- Comprehensive monitoring for all replication scenarios

-- Create monitoring database
CREATE DATABASE REPLICATION_MONITORING
    COMMENT = 'Centralized monitoring for all replications';

-- Create monitoring schema
CREATE SCHEMA REPLICATION_MONITORING.DASHBOARDS;

-- Comprehensive monitoring view
CREATE OR REPLACE VIEW REPLICATION_MONITORING.DASHBOARDS.REPLICATION_STATUS AS
SELECT 
    DATABASE_NAME,
    IS_PRIMARY,
    PRIMARY_DATABASE_NAME,
    SECONDARY_STATE,
    LAST_REFRESH_TIME,
    NEXT_REFRESH_TIME,
    DATEDIFF('MINUTE', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) as MINUTES_SINCE_REFRESH,
    CASE 
        WHEN IS_PRIMARY = 'YES' THEN 'PRIMARY_DATABASE'
        WHEN SECONDARY_STATE = 'PROVISIONING' THEN 'INITIALIZING'
        WHEN SECONDARY_STATE = 'AVAILABLE' AND LAST_REFRESH_TIME IS NULL THEN 'NEVER_REFRESHED'
        WHEN SECONDARY_STATE = 'AVAILABLE' AND DATEDIFF('HOUR', LAST_REFRESH_TIME, CURRENT_TIMESTAMP()) > 24 THEN 'STALE_DATA'
        WHEN SECONDARY_STATE = 'AVAILABLE' AND NEXT_REFRESH_TIME < CURRENT_TIMESTAMP() THEN 'REFRESH_OVERDUE'
        WHEN SECONDARY_STATE = 'AVAILABLE' THEN 'HEALTHY'
        ELSE 'UNKNOWN_STATE'
    END as HEALTH_STATUS,
    REPLICATION_SCHEDULE,
    COMMENT
FROM INFORMATION_SCHEMA.DATABASES 
ORDER BY IS_PRIMARY DESC, DATABASE_NAME;

-- Create alerting procedure
CREATE OR REPLACE PROCEDURE REPLICATION_MONITORING.DASHBOARDS.CHECK_REPLICATION_HEALTH()
RETURNS TABLE (ALERT_TYPE STRING, MESSAGE STRING, DATABASE_NAME STRING)
LANGUAGE SQL
AS
$$
BEGIN
    LET alerts RESULTSET := (
        SELECT 
            CASE 
                WHEN HEALTH_STATUS = 'NEVER_REFRESHED' THEN 'CRITICAL'
                WHEN HEALTH_STATUS = 'STALE_DATA' THEN 'WARNING'
                WHEN HEALTH_STATUS = 'REFRESH_OVERDUE' THEN 'WARNING'
                WHEN HEALTH_STATUS = 'UNKNOWN_STATE' THEN 'CRITICAL'
                ELSE 'INFO'
            END as ALERT_TYPE,
            'Database ' || DATABASE_NAME || ' status: ' || HEALTH_STATUS || 
            '. Last refresh: ' || COALESCE(LAST_REFRESH_TIME::STRING, 'NEVER') as MESSAGE,
            DATABASE_NAME
        FROM REPLICATION_MONITORING.DASHBOARDS.REPLICATION_STATUS
        WHERE HEALTH_STATUS IN ('NEVER_REFRESHED', 'STALE_DATA', 'REFRESH_OVERDUE', 'UNKNOWN_STATE')
    );
    RETURN TABLE(alerts);
END;
$$;

-- =====================================================
-- USAGE EXAMPLES SUMMARY
-- =====================================================

/*
1. DISASTER RECOVERY:
   - Cross-region replication with 15-minute intervals
   - Failover capabilities for business continuity

2. ANALYTICS SEPARATION:
   - Dedicated analytics replicas with 6-hour refresh
   - Optimized views for reporting workloads

3. DEVELOPMENT/TESTING:
   - Daily or manual refresh for cost optimization
   - Isolated environments for safe testing

4. MULTI-REGION:
   - Regional replicas for performance optimization
   - Reduced latency for global applications

5. REPLICATION GROUPS:
   - Consistent replication of related databases
   - Simplified management of multiple databases

6. DATA PIPELINE:
   - Staged processing with replication between stages
   - Raw -> Staging -> Curated -> Analytics flow

7. COMPLIANCE:
   - Region-specific replicas for data residency
   - Filtered views for regulatory compliance

8. BACKUP/ARCHIVAL:
   - Long-term backup with extended retention
   - Historical data preservation

9. PARTNER SHARING:
   - Secure data sharing across organizations
   - Curated views for external consumption

10. MONITORING:
    - Comprehensive health monitoring
    - Automated alerting for issues

Choose the appropriate pattern based on your specific requirements
for data freshness, cost optimization, compliance, and performance.
*/