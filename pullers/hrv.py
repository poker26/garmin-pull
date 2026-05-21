"""Pull HRV daily summary."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class HrvPuller(BasePuller):
    pull_type = "hrv"
    overlap_minutes = 0  # daily, overlap не нужен
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        rows = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[hrv] Fetching {date_str}")
            try:
                hrv_data = self.garmin.get_hrv_data(date_str)
                if not hrv_data:
                    cur_date += timedelta(days=1)
                    continue

                summary = hrv_data.get("hrvSummary") or {}
                baseline = summary.get("baseline") or {}

                rows.append((
                    cur_date,
                    "garmin",
                    summary.get("lastNightAvg"),
                    summary.get("lastNight5MinHigh"),
                    baseline.get("lowUpper"),
                    baseline.get("balancedLow"),
                    baseline.get("balancedUpper"),
                    summary.get("weeklyAvg"),
                    summary.get("status"),
                ))
            except Exception as e:
                logger.warning(f"[hrv] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[hrv] Days: {len(rows)}")
        return rows

    def write(self, conn, rows) -> Optional[datetime]:
        if not rows:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO hrv_daily
                    (date, source, last_night_avg, last_night_5min_high,
                     baseline_low_upper, baseline_balanced_low, baseline_balanced_high,
                     weekly_avg, status)
                VALUES %s
                ON CONFLICT (date, source) DO UPDATE SET
                    last_night_avg          = EXCLUDED.last_night_avg,
                    last_night_5min_high    = EXCLUDED.last_night_5min_high,
                    baseline_low_upper      = EXCLUDED.baseline_low_upper,
                    baseline_balanced_low   = EXCLUDED.baseline_balanced_low,
                    baseline_balanced_high  = EXCLUDED.baseline_balanced_high,
                    weekly_avg              = EXCLUDED.weekly_avg,
                    status                  = EXCLUDED.status,
                    updated_at              = NOW()
                """,
                rows,
            )
        conn.commit()

        max_date = max(r[0] for r in rows)
        return datetime.combine(max_date, datetime.min.time(), tzinfo=timezone.utc)
