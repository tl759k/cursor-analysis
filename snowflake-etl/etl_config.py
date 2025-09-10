"""
Configuration for the Snowflake ETL pipeline.

Edit this file to change schedule, retries, warehouse overrides, and Slack settings.
Environment variables still take precedence at runtime when present.
"""

import os
from typing import List, Dict, Optional

# Steps to run (in order). Edit the SQL filenames if needed.
ETL_STEPS: List[Dict[str, str]] = [
    {"id": "step_0", "sql": "s0_tbl_applicant_funnel_timestamp_with_backfill_snowflake.sql"},
    {"id": "step_1", "sql": "s1_tbl_major_steps_conversion_analysis_applied_L7D_cohort_snowflake.sql"},
    {"id": "step_2", "sql": "s2_tbl_cvr_reporting_metric_variances_snowflake.sql"},
    {"id": "step_3", "sql": "s3_tbl_conversion_funnel_idv_substeps_all_timestamps_snowflake.sql"},
]

# Schedule configuration (used by setup_schedule.py)
CRON_MINUTE = 0
CRON_HOUR = 4
CRON_DOM = "*"
CRON_MON = "*"
CRON_DOW = "*"
CRON_TZ = "America/Los_Angeles"

# Retry configuration
MAX_RETRIES: int = 5                # number of retries after first attempt
RETRY_WAIT_SECONDS: int = 20 * 60   # 20 minutes

# Timezone for displaying timestamps
TIMEZONE_NAME: str = "America/Los_Angeles"

# Warehouse override for ETL runs (optional). If None, defaults from SnowflakeHook env config are used.
WAREHOUSE_OVERRIDE: Optional[str] = None

# Slack settings
# Option 1: Keep empty here and set via environment variable SLACK_WEBHOOK_URL in config/.env
# Option 2: Paste your webhook here (not recommended to commit secrets)
SLACK_WEBHOOK_URL: str = os.getenv("SLACK_WEBHOOK_URL", "")


