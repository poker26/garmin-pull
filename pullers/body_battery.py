"""Pull Body Battery intraday + daily."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class BodyBatteryPuller(BasePuller):
    pull_type = "body_battery"
    overlap_minutes = 60
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        """Возвращает (intraday_samples, daily_summaries)."""
        intraday = []
        daily = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[body_battery] Fetching {date_str}")
            try:
                bb_data = self.garmin.get_body_battery(date_str)
                # bb_data: список [{ "date": "...", "bodyBatteryValuesArray": [[ts_ms, status, value, ?], ...],
                #                   "charged": int, "drained": int, ... }]
                for day_entry in (bb_data or []):
                    values_arr = day_entry.get("bodyBatteryValuesArray") or []
                    day_min = None
                    day_max = None
                    day_start = None
                    day_end = None

                    for row in values_arr:
                        if len(row) < 2:
                            continue
                        ts_ms,value = row[0], row[1]
                        if value is None:
                            continue
                        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                        value = int(value)
                        if not (0 <= value <= 100):
                            continue

                        if date_from <= ts <= date_to:
                            intraday.append((ts, value, "garmin"))

                        # для daily-агрегата
                        day_min = value if day_min is None else min(day_min, value)
                        day_max = value if day_max is None else max(day_max, value)
                        if day_start is None:
                            day_start = value
                        day_end = value

                    daily.append((
                        cur_date,
                        "garmin",
                        day_start,
                        day_end,
                        day_max,
                        day_min,
                        day_entry.get("charged"),
                        day_entry.get("drained"),
                    ))
            except Exception as e:
                logger.warning(f"[body_battery] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[body_battery] intraday={len(intraday)}, daily={len(daily)}")
        return intraday, daily

    def write(self, conn, data) -> Optional[datetime]:
        intraday, daily = data
        max_ts = None

        if intraday:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO body_battery_intraday (measured_at, value, source)
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
                    INSERT INTO body_battery_daily
                        (date, source, start_value, end_value, max_value, min_value, charged, drained)
                    VALUES %s
                    ON CONFLICT (date, source) DO UPDATE SET
                        start_value = EXCLUDED.start_value,
                        end_value   = EXCLUDED.end_value,
                        max_value   = EXCLUDED.max_value,
                        min_value   = EXCLUDED.min_value,
                        charged     = EXCLUDED.charged,
                        drained     = EXCLUDED.drained,
                        updated_at  = NOW()
                    """,
                    daily,
                )

        conn.commit()
        return max_ts
