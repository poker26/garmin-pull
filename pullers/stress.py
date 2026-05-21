"""Pull stress intraday + daily."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class StressPuller(BasePuller):
    pull_type = "stress"
    overlap_minutes = 60
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        intraday = []
        daily = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[stress] Fetching {date_str}")
            try:
                # get_stress_data возвращает {stressValuesArray: [[ts_ms, value], ...],
                #                             avgStressLevel, restStressDuration,
                #                             lowStressDuration, mediumStressDuration, highStressDuration, ...}
                s_data = self.garmin.get_stress_data(date_str)
                if not s_data:
                    cur_date += timedelta(days=1)
                    continue

                values_arr = s_data.get("stressValuesArray") or []
                for row in values_arr:
                    if len(row) < 2:
                        continue
                    ts_ms, value = row[0], row[1]
                    if value is None:
                        continue
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    # Garmin: -1=invalid/sleep, -2=no data, 0..100 = реальные значения
                    if date_from <= ts <= date_to:
                        intraday.append((ts, int(value), "garmin"))

                # Daily summary
                def _sec_to_min(s):
                    return int(s / 60) if s and s > 0 else 0

                daily.append((
                    cur_date,
                    "garmin",
                    s_data.get("avgStressLevel"),
                    _sec_to_min(s_data.get("restStressDuration")),
                    _sec_to_min(s_data.get("lowStressDuration")),
                    _sec_to_min(s_data.get("mediumStressDuration")),
                    _sec_to_min(s_data.get("highStressDuration")),
                ))
            except Exception as e:
                logger.warning(f"[stress] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[stress] intraday={len(intraday)}, daily={len(daily)}")
        return intraday, daily

    def write(self, conn, data) -> Optional[datetime]:
        intraday, daily = data
        max_ts = None

        if intraday:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO stress_intraday (measured_at, value, source)
                    VALUES %s
                    ON CONFLICT (measured_at, source) DO NOTHING
                    """,
                    intraday,
                )
            max_ts = max(s[0] for s in intraday)

        if daily:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO stress_daily
                        (date, source, overall_avg, rest_minutes, low_minutes, medium_minutes, high_minutes)
                    VALUES %s
                    ON CONFLICT (date, source) DO UPDATE SET
                        overall_avg    = EXCLUDED.overall_avg,
                        rest_minutes   = EXCLUDED.rest_minutes,
                        low_minutes    = EXCLUDED.low_minutes,
                        medium_minutes = EXCLUDED.medium_minutes,
                        high_minutes   = EXCLUDED.high_minutes,
                        updated_at     = NOW()
                    """,
                    daily,
                )

        conn.commit()
        return max_ts
