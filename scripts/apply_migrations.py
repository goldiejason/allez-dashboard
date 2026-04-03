"""
scripts/apply_migrations.py — Apply database migrations via Supabase.

Uses the Supabase REST API to apply SQL migrations stored in
database/migrations/*.sql.  Requires the service role key.

Usage:
    python scripts/apply_migrations.py
    python scripts/apply_migrations.py --dry-run
"""

import sys
import os
import logging
import argparse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def run_sql(sql: str, dry_run: bool = False) -> bool:
    """
    Execute a SQL statement against Supabase using the REST /rpc endpoint.
    Falls back to psql if available.
    """
    url    = os.environ.get("SUPABASE_URL", "").rstrip("/")
    key    = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")
    db_url = os.environ.get("SUPABASE_DB_URL", "")

    if dry_run:
        logger.info(f"[DRY RUN] Would execute:\n{sql[:200]}…")
        return True

    # Try psql first (available in CI and local with postgres client installed)
    if db_url:
        import subprocess
        result = subprocess.run(
            ["psql", db_url, "-c", sql],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            logger.info(f"psql: {result.stdout.strip()}")
            return True
        else:
            logger.warning(f"psql failed: {result.stderr.strip()}")

    # Fall back to Supabase exec endpoint (requires pg_execute extension)
    headers = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }
    resp = requests.post(
        f"{url}/rest/v1/rpc/exec_sql",
        json={"query": sql},
        headers=headers,
        timeout=30,
    )
    if resp.ok:
        logger.info("SQL executed successfully via RPC")
        return True

    # Last resort: use supabase-py table creation via a known-schema approach
    logger.error(
        f"Could not execute SQL automatically (status={resp.status_code}). "
        "Please apply database/migrations/001_identity_and_attribution.sql "
        "manually in the Supabase SQL Editor."
    )
    return False


def main():
    parser = argparse.ArgumentParser(description="Apply Allez Dashboard DB migrations")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    migrations_dir = Path(__file__).parent.parent / "database" / "migrations"
    sql_files = sorted(migrations_dir.glob("*.sql"))

    if not sql_files:
        logger.info("No migration files found.")
        return

    for sql_file in sql_files:
        logger.info(f"Applying {sql_file.name}")
        sql = sql_file.read_text()

        # Split on semicolons and run each statement
        statements = [s.strip() for s in sql.split(";") if s.strip() and not s.strip().startswith("--")]
        all_ok = True
        for stmt in statements:
            if stmt:
                ok = run_sql(stmt + ";", dry_run=args.dry_run)
                if not ok:
                    all_ok = False
                    break

        if all_ok:
            logger.info(f"✓ {sql_file.name} applied")
        else:
            logger.error(f"✗ {sql_file.name} failed — stopping")
            sys.exit(1)


if __name__ == "__main__":
    main()
