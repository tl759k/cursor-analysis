#!/usr/bin/env python3
"""
ETL Orchestrator for Snowflake SQL steps (step_0 -> step_1 -> step_2 -> step_3)

Features:
- Sequential dependency execution
- Auto-retry failed steps (configurable)
- Timing and status capture per step
- Slack notification (via incoming webhook)
- Persisted logs and run summaries (CSV + JSON)

Usage:
  python etl_runner.py

Environment variables:
- SLACK_WEBHOOK_URL: Optional. If set, a Slack message will be posted on completion
- SNOWFLAKE_WAREHOUSE_ETL: Optional. Overrides Snowflake warehouse for this job
"""

import os
import re
import sys
import json
import time
import csv
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

# Ensure project root on path
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from utils.logger import setup_logger  # noqa: E402
from utils.snowflake_connection import SnowflakeHook  # noqa: E402
from dotenv import load_dotenv  # noqa: E402
from etl_config import (
    ETL_STEPS,
    MAX_RETRIES,
    RETRY_WAIT_SECONDS,
    WAREHOUSE_OVERRIDE,
    TIMEZONE_NAME,
)
import pandas as pd  # noqa: E402


def ensure_directories(base_dir: Path) -> Tuple[Path, Path]:
    """Ensure logs/ and outputs/ directories exist under base_dir."""
    logs_dir = base_dir / "logs"
    outputs_dir = base_dir / "outputs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir, outputs_dir


def format_duration(seconds: float) -> str:
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    return f"{minutes:02d}:{secs:02d}"


def parse_target_table(sql_text: str) -> Optional[str]:
    """
    Attempt to extract the table name from a CREATE OR REPLACE TABLE statement.
    Returns the first match if found.
    """
    # Remove multiline comments
    cleaned = re.sub(r"/\*.*?\*/", " ", sql_text, flags=re.DOTALL)
    # Search for CREATE OR REPLACE TABLE <name>
    match = re.search(r"create\s+or\s+replace\s+table\s+([^\s(]+)", cleaned, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    return None


def split_sql_statements(sql_text: str) -> List[str]:
    """
    Split SQL into statements on semicolons, filtering out empty/comment-only chunks.
    """
    # Remove BOM
    if sql_text and sql_text[0] == "\ufeff":
        sql_text = sql_text[1:]

    # Remove multiline comments first
    no_block_comments = re.sub(r"/\*.*?\*/", " ", sql_text, flags=re.DOTALL)

    # Split by semicolons
    raw_statements = [s.strip() for s in no_block_comments.split(";")]

    def is_effective(stmt: str) -> bool:
        # Remove line comments
        lines = []
        for line in stmt.splitlines():
            stripped = line.strip()
            if stripped.startswith("--") or stripped == "":
                continue
            lines.append(stripped)
        return len(" ".join(lines).strip()) > 0

    statements = [s for s in raw_statements if is_effective(s)]
    return statements


def run_sql_job(sql_file: Path, warehouse_override: Optional[str], logger) -> Tuple[bool, Optional[str]]:
    """
    Execute all statements in the given SQL file using SnowflakeHook.
    Returns (success, error_message_if_any).
    """
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_text = f.read()

    statements = split_sql_statements(sql_text)
    logger.info(f"Executing {len(statements)} statement(s) from {sql_file.name}")

    hook_kwargs: Dict[str, str] = {}
    if warehouse_override:
        hook_kwargs["warehouse"] = warehouse_override

    snowhook = SnowflakeHook(**hook_kwargs)

    try:
        for idx, stmt in enumerate(statements, 1):
            preview = stmt[:100].replace("\n", " ")
            logger.info(f"[{idx}/{len(statements)}] Executing: {preview}{'...' if len(stmt) > 100 else ''}")
            snowhook.query_without_result(stmt)
        return True, None
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        logger.error(f"Failure executing statements from {sql_file.name}: {err}")
        logger.debug("\n" + traceback.format_exc())
        return False, err
    finally:
        try:
            snowhook.close()
        except Exception:
            pass


def send_slack_message(text: str, logger) -> bool:
    """
    Send a Slack message using an incoming webhook defined in SLACK_WEBHOOK_URL.
    Returns True if sent, False if not configured or failed.
    """
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        logger.warning("SLACK_WEBHOOK_URL not set; skipping Slack notification")
        return False
    try:
        import urllib.request
        import urllib.error
        data = json.dumps({"text": text}).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            _ = resp.read()
        return True
    except Exception as exc:
        logger.error(f"Failed to send Slack message: {exc}")
        return False


def build_markdown_table(rows: List[Dict[str, str]]) -> str:
    headers = ["query", "table_name", "duration", "last_updated_at", "status", "error"]
    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for r in rows:
        line = "| " + " | ".join([
            str(r.get("query", "")),
            str(r.get("table_name", "")),
            str(r.get("duration", "")),
            str(r.get("last_updated_at", "")),
            str(r.get("status", "")),
            (str(r.get("error", "")).replace("\n", " ")[:180] if r.get("error") else ""),
        ]) + " |"
        lines.append(line)
    return "\n".join(lines)


def get_table_last_updated_at(table_name: str, logger) -> Optional[str]:
    """
    Try to fetch last updated timestamp for a fully qualified table using Snowflake metadata.
    Returns a string in local timezone if possible, else None.
    """
    if not table_name or "." not in table_name:
        return None
    try:
        # Expecting database.schema.table format
        parts = table_name.split(".")
        if len(parts) == 2:
            # schema.table -> assume current database
            database = None
            schema, table = parts
        elif len(parts) == 3:
            database, schema, table = parts
        else:
            return None

        # Use SnowflakeHook to query information schema
        hook = SnowflakeHook()
        if database:
            query = f"""
                select max(greatest(created, last_altered)) as last_ts
                from {database}.information_schema.tables
                where table_schema = '{schema.upper()}' and table_name = '{table.upper()}'
            """
        else:
            query = f"""
                select max(greatest(created, last_altered)) as last_ts
                from information_schema.tables
                where table_schema = '{schema.upper()}' and table_name = '{table.upper()}'
            """
        df = hook.query_snowflake(query, method='pandas')
        hook.close()
        if df is not None and not df.empty and df.loc[0, "last_ts"] is not None:
            # Localize to configured timezone
            try:
                import pytz
                tz = pytz.timezone(TIMEZONE_NAME)
                ts = df.loc[0, "last_ts"]
                if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
                    local_ts = ts.astimezone(tz)
                else:
                    local_ts = tz.localize(ts)
                return local_ts.strftime("%Y-%m-%d %H:%M:%S %Z")
            except Exception:
                return str(df.loc[0, "last_ts"])
        return None
    except Exception as exc:
        logger.warning(f"Could not retrieve last_updated_at for {table_name}: {exc}")
        return None


def main():
    base_dir = Path(__file__).resolve().parent
    logs_dir, outputs_dir = ensure_directories(base_dir)
    start_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"etl_run_{start_ts}.log"
    logger = setup_logger(
        name="etl_runner",
        log_file=str(log_file),
        log_to_console=True,
    )

    # Load environment variables from config/.env for cron context
    env_path = Path(PROJECT_ROOT) / "config" / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=False)
        logger.info(f"Loaded environment from {env_path}")

    # Prefer config values, allow env overrides if present
    warehouse_override = os.getenv("SNOWFLAKE_WAREHOUSE_ETL", WAREHOUSE_OVERRIDE or "") or None
    retry_attempts = int(os.getenv("ETL_MAX_RETRIES", str(MAX_RETRIES)))
    retry_wait_seconds = int(os.getenv("ETL_RETRY_WAIT_SECONDS", str(RETRY_WAIT_SECONDS)))
    steps: List[Dict[str, str]] = ETL_STEPS

    results: List[Dict[str, str]] = []
    overall_success = True

    logger.info("Starting ETL run for 4 steps with sequential dependencies")
    for step in steps:
        step_id = step["id"]
        sql_path = base_dir / step["sql"]
        if not sql_path.exists():
            err = f"SQL file not found: {sql_path}"
            logger.error(err)
            results.append({
                "query": step_id,
                "table_name": "",
                "duration": "00:00",
                "status": "fail",
                "error": err,
            })
            overall_success = False
            break

        table_name = None
        try:
            sql_text_for_parse = sql_path.read_text(encoding="utf-8")
            table_name = parse_target_table(sql_text_for_parse) or ""
        except Exception:
            table_name = ""

        attempt = 0
        start_time = time.time()
        last_error = None
        success = False

        while True:
            attempt += 1
            logger.info(f"Executing {step_id} (attempt {attempt}) → {sql_path.name}")
            ok, err_msg = run_sql_job(sql_path, warehouse_override, logger)
            if ok:
                success = True
                break
            last_error = err_msg or "Unknown error"
            if attempt > retry_attempts:
                break
            logger.warning(f"{step_id} failed. Retrying in {retry_wait_seconds//60} minutes...")
            time.sleep(retry_wait_seconds)

        duration_seconds = time.time() - start_time
        # Insert placeholder; last_updated_at populated after success
        result_row = {
            "query": step_id,
            "table_name": table_name,
            "duration": format_duration(duration_seconds),
            "last_updated_at": "",
            "status": "success" if success else "fail",
            "error": None if success else last_error,
        }

        # If step succeeded and we have a target table, try to fetch last_updated_at
        if success and table_name:
            last_ts = get_table_last_updated_at(table_name, logger)
            if last_ts:
                result_row["last_updated_at"] = last_ts

        results.append(result_row)

        if not success:
            overall_success = False
            logger.error(f"Stopping pipeline after {step_id} failure; downstream steps will not run.")
            break

    # If a failure occurred, mark remaining steps as skipped/fail with a note
    if not overall_success:
        failed_index = len(results) - 1
        for remaining in steps[failed_index + 1:]:
            results.append({
                "query": remaining["id"],
                "table_name": "",
                "duration": "00:00",
                "status": "fail",
                "error": "Skipped due to previous failure",
            })

    # Persist summaries
    summary_basename = f"etl_summary_{start_ts}"
    summary_json = outputs_dir / f"{summary_basename}.json"
    summary_csv = outputs_dir / f"{summary_basename}.csv"

    with open(summary_json, "w", encoding="utf-8") as jf:
        json.dump(results, jf, indent=2)

    with open(summary_csv, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=[
            "query",
            "table_name",
            "duration",
            "last_updated_at",
            "status",
            "error",
        ])
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    # Print markdown table to stdout and log
    md_table = build_markdown_table(results)
    print(md_table)
    logger.info("\n" + md_table)

    # Slack notification
    status_emoji = "✅" if overall_success else "❌"
    title = f"{status_emoji} ETL run {'succeeded' if overall_success else 'failed'} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    slack_text = title + "\n\n" + md_table
    slack_sent = send_slack_message(slack_text, logger)

    # Fallback when Slack is not configured: print DataFrame and save notification CSV
    if not slack_sent:
        df = pd.DataFrame(results, columns=["query", "table_name", "duration", "last_updated_at", "status", "error"])
        print("\nNotification content (DataFrame):")
        print(df)
        notif_csv = outputs_dir / f"notification_{start_ts}.csv"
        df.to_csv(notif_csv, index=False)
        logger.info(f"Wrote notification CSV to {notif_csv}")

    # Exit code
    sys.exit(0 if overall_success else 1)


if __name__ == "__main__":
    main()


