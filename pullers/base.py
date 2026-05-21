"""Base classes for Garmin pullers."""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def get_db_conn():
    return psycopg2.connect(
        host=os.environ["SUPABASE_DB_HOST"],
        port=int(os.environ["SUPABASE_DB_PORT"]),
        dbname=os.environ["SUPABASE_DB_NAME"],
        user=os.environ["SUPABASE_DB_USER"],
        password=os.environ["SUPABASE_DB_PASSWORD"],
    )


class BasePuller:
    pull_type: str = ""
    overlap_minutes: int = 60
    backfill_days: int = 7

    def __init__(self, garmin_client):
        self.garmin = garmin_client

    def get_state(self, conn) -> Optional[datetime]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_data_timestamp FROM garmin_pull_state WHERE pull_type = %s",
                (self.pull_type,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def update_state(
        self,
        conn,
        status: str,
        last_data_ts: Optional[datetime] = None,
        error: Optional[str] = None,
    ):
        now = datetime.now(timezone.utc)
        success = status == "success"

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO garmin_pull_state
                    (pull_type, last_attempt_at, last_success_at,
                     last_data_timestamp, last_status, last_error,
                     consecutive_errors, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (pull_type) DO UPDATE SET
                    last_attempt_at = EXCLUDED.last_attempt_at,
                    last_success_at = CASE
                        WHEN %s THEN EXCLUDED.last_attempt_at
                        ELSE garmin_pull_state.last_success_at
                    END,
                    last_data_timestamp = COALESCE(
                        EXCLUDED.last_data_timestamp,
                        garmin_pull_state.last_data_timestamp
                    ),
                    last_status = EXCLUDED.last_status,
                    last_error = EXCLUDED.last_error,
                    consecutive_errors = CASE
                        WHEN %s THEN 0
                        ELSE garmin_pull_state.consecutive_errors + 1
                    END,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    self.pull_type, now, now if success else None,
                    last_data_ts, status, error,
                    0 if success else 1, now,
                    success, success,
                ),
            )
        conn.commit()

    def determine_window(self, conn) -> tuple[datetime, datetime]:
        last_ts = self.get_state(conn)
        now = datetime.now(timezone.utc)

        if last_ts is None:
            date_from = now - timedelta(days=self.backfill_days)
            logger.info(f"[{self.pull_type}] First run, backfill {self.backfill_days} days")
        else:
            date_from = last_ts - timedelta(minutes=self.overlap_minutes)
            logger.info(f"[{self.pull_type}] Incremental from {date_from}")

        return date_from, now

    def run(self, conn) -> bool:
        try:
            date_from, date_to = self.determine_window(conn)
            data = self.fetch(date_from, date_to)
            max_ts = self.write(conn, data)
            self.update_state(conn, "success", last_data_ts=max_ts)
            logger.info(f"[{self.pull_type}] Done, max_ts={max_ts}")
            return True
        except Exception as e:
            logger.exception(f"[{self.pull_type}] Failed")
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                self.update_state(conn, "error", error=str(e)[:500])
            except Exception:
                logger.exception(f"[{self.pull_type}] Failed to write error state")
            return False

    def fetch(self, date_from: datetime, date_to: datetime):
        raise NotImplementedError

    def write(self, conn, data) -> Optional[datetime]:
        raise NotImplementedError
