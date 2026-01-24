-- ============================================================
-- METADATA UPDATE REQUESTS TABLE
-- Store user requests for new columns or metadata updates
-- ============================================================

-- Create the table
CREATE TABLE IF NOT EXISTS prd_optumrx_orxfdmprdsa.rag.metadata_update_requests (
    request_id STRING NOT NULL COMMENT 'Unique request identifier (e.g., REQ-2025-01-24-001)',
    request_type STRING NOT NULL COMMENT 'Type of request: NEW_COLUMN or UPDATE_METADATA',
    domain STRING NOT NULL COMMENT 'Domain: PBM Network or Optum Pharmacy',
    dataset_name STRING NOT NULL COMMENT 'Functional name of the dataset',
    table_name STRING NOT NULL COMMENT 'Actual table name in Databricks',
    column_name STRING NOT NULL COMMENT 'New column name or existing column name to update',
    current_description STRING COMMENT 'Current description (NULL for new columns)',
    new_description STRING COMMENT 'Proposed/updated description',
    synonyms STRING COMMENT 'Comma-separated synonyms (optional)',
    business_justification STRING COMMENT 'Reason for the request',
    requested_by STRING NOT NULL COMMENT 'Email of the user who submitted the request',
    request_status STRING DEFAULT 'PENDING' COMMENT 'Status: PENDING, APPROVED, REJECTED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Request submission timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP() COMMENT 'Last update timestamp',
    reviewer_comments STRING COMMENT 'Comments from the reviewer'
)
COMMENT 'Stores metadata update requests from DANA users'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);

-- ============================================================
-- SAMPLE QUERIES FOR ADMINISTRATORS
-- ============================================================

-- View all pending requests
-- SELECT * FROM prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
-- WHERE request_status = 'PENDING' 
-- ORDER BY created_at DESC;

-- View requests by domain
-- SELECT * FROM prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
-- WHERE domain = 'PBM Network' 
-- ORDER BY created_at DESC;

-- Approve a request
-- UPDATE prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
-- SET request_status = 'APPROVED', 
--     updated_at = CURRENT_TIMESTAMP(), 
--     reviewer_comments = 'Approved by admin'
-- WHERE request_id = 'REQ-2025-01-24-001';

-- Reject a request
-- UPDATE prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
-- SET request_status = 'REJECTED', 
--     updated_at = CURRENT_TIMESTAMP(), 
--     reviewer_comments = 'Column already exists'
-- WHERE request_id = 'REQ-2025-01-24-001';

-- Count requests by status
-- SELECT request_status, COUNT(*) as count 
-- FROM prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
-- GROUP BY request_status;

-- Count requests by user
-- SELECT requested_by, COUNT(*) as count 
-- FROM prd_optumrx_orxfdmprdsa.rag.metadata_update_requests 
-- GROUP BY requested_by 
-- ORDER BY count DESC;
