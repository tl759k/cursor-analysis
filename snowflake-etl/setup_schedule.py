#!/usr/bin/env python3
"""
Install or update the crontab entry for the ETL based on etl_config.py.

This script sets the schedule to run the ETL at the configured time in the configured timezone.

Usage:
  python setup_schedule.py
"""

import subprocess
from pathlib import Path
from typing import List

from etl_config import CRON_MINUTE, CRON_HOUR, CRON_DOM, CRON_MON, CRON_DOW, CRON_TZ


def main():
    project_root = Path(__file__).resolve().parents[2]
    etl_script = project_root / "user-analysis" / "snowflake-etl" / "run_etl.sh"
    cron_log = project_root / "user-analysis" / "snowflake-etl" / "cron.log"

    cron_line = (
        f"{CRON_MINUTE} {CRON_HOUR} {CRON_DOM} {CRON_MON} {CRON_DOW} TZ={CRON_TZ} "
        f"{etl_script} >> {cron_log} 2>&1"
    )

    # Merge with existing crontab
    cmd: List[str] = [
        "bash",
        "-lc",
        f"(crontab -l 2>/dev/null | grep -v '{etl_script}'; echo \"{cron_line}\") | crontab -",
    ]
    subprocess.run(cmd, check=True)
    print("Installed/updated cron entry:")
    print(cron_line)


if __name__ == "__main__":
    main()


