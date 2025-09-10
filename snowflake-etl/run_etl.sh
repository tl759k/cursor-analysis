#!/bin/zsh

# Absolute project root
PROJECT_ROOT="/Users/tl759k/Documents/GitHub/work/cursor-analytics"
cd "$PROJECT_ROOT" || exit 1

# Activate virtual environment
source "$PROJECT_ROOT/venv/bin/activate"

# Move to ETL directory
cd "$PROJECT_ROOT/user-analysis/snowflake-etl" || exit 1

# Optional overrides via env vars
# export SLACK_WEBHOOK_URL=""             # overrides etl_config.py
# export SNOWFLAKE_WAREHOUSE_ETL=""        # overrides etl_config.py
# export ETL_MAX_RETRIES=5                  # overrides etl_config.py
# export ETL_RETRY_WAIT_SECONDS=$((20*60))  # overrides etl_config.py

# Run
python etl_runner.py

exit $?


