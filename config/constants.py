"""Application-wide constants."""

# Job names
JOBS_HMIS_TO_DMS_CUSTOMER = "hmis_to_dms_customer_sync"
JOBS_HMIS_TO_DMS_DEALER = "hmis_to_dms_dealer_sync"
JOBS_HMIS_TO_DMS_PRODUCT = "hmis_to_dms_product_sync"
JOBS_HMIS_TO_DMS_STOCK = "hmis_to_dms_stock_sync"
JOBS_HMIS_TO_DMS_ORDER = "hmis_to_dms_order_sync"
JOBS_HMIS_TO_DMS_INVOICE = "hmis_to_dms_invoice_sync"

JOBS_DMS_TO_HMIS_CUSTOMER = "dms_to_hmis_customer_sync"
JOBS_DMS_TO_HMIS_DEALER = "dms_to_hmis_dealer_sync"
JOBS_DMS_TO_HMIS_PRODUCT = "dms_to_hmis_product_sync"
JOBS_DMS_TO_HMIS_STOCK = "dms_to_hmis_stock_sync"
JOBS_DMS_TO_HMIS_ORDER = "dms_to_hmis_order_sync"
JOBS_DMS_TO_HMIS_INVOICE = "dms_to_hmis_invoice_sync"

JOBS_HEALTH_CHECK = "health_check"
JOBS_RETRY_FAILED = "retry_failed_records"
JOBS_RECONCILIATION = "reconciliation"

ALL_JOB_NAMES = [
    JOBS_HMIS_TO_DMS_CUSTOMER,
    JOBS_HMIS_TO_DMS_DEALER,
    JOBS_HMIS_TO_DMS_PRODUCT,
    JOBS_HMIS_TO_DMS_STOCK,
    JOBS_HMIS_TO_DMS_ORDER,
    JOBS_HMIS_TO_DMS_INVOICE,
    JOBS_DMS_TO_HMIS_CUSTOMER,
    JOBS_DMS_TO_HMIS_DEALER,
    JOBS_DMS_TO_HMIS_PRODUCT,
    JOBS_DMS_TO_HMIS_STOCK,
    JOBS_DMS_TO_HMIS_ORDER,
    JOBS_DMS_TO_HMIS_INVOICE,
    JOBS_HEALTH_CHECK,
    JOBS_RETRY_FAILED,
    JOBS_RECONCILIATION,
]

# Table names
TABLE_INTEGRATION_JOBS = "IntegrationJobs"
TABLE_INTEGRATION_JOB_LOGS = "IntegrationJobLogs"
TABLE_INTEGRATION_JOB_QUEUE = "IntegrationJobQueue"
TABLE_INTEGRATION_SYNC_STATE = "IntegrationSyncState"
TABLE_INTEGRATION_FAILED_RECORDS = "IntegrationFailedRecords"
TABLE_JOB_LOCK = "JobLock"
TABLE_API_CLIENTS = "ApiClients"
TABLE_API_AUDIT_LOGS = "ApiAuditLogs"

# Header names
HEADER_CLIENT_ID = "X-Client-Id"
HEADER_API_KEY = "X-API-Key"

# Endpoint paths
ENDPOINT_HEALTH = "/api/health"
ENDPOINT_JOBS_TRIGGER = "/api/jobs/trigger"
ENDPOINT_JOBS_GET = "/api/jobs"
ENDPOINT_QUEUE_LIST = "/api/queue"

# Run types
RUN_TYPE_SCHEDULED = "scheduled"
RUN_TYPE_ON_DEMAND = "on_demand"
RUN_TYPE_RETRY = "retry"
RUN_TYPE_RECONCILIATION = "reconciliation"

# Batch size for incremental sync
DEFAULT_BATCH_SIZE = 500
MAX_BATCH_SIZE = 5000
