"""Pull sleep daily summary."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


def _ms_to_datetime(timestamp_ms: Optional[int]) -> Optional[datetime]:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _parse_calendar_date(value: Optional[str]):
    if not value:
        return None
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


class SleepPuller(BasePuller):
    pull_type = "sleep"
    overlap_minutes = 0
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        rows = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[sleep] Fetching {date_str}")
            try:
                sleep_data = self.garmin.get_sleep_data(date_str)
                if not sleep_data:
                    cur_date += timedelta(days=1)
                    continue

                dto = sleep_data.get("dailySleepDTO") or {}
                calendar_date = _parse_calendar_date(dto.get("calendarDate")) or cur_date

                total_seconds = dto.get("sleepTimeSeconds")
                sleep_start = _ms_to_datetime(dto.get("sleepStartTimestampGMT"))
                sleep_end = _ms_to_datetime(dto.get("sleepEndTimestampGMT"))

                if total_seconds is None and sleep_start is None and sleep_end is None:
                    cur_date += timedelta(days=1)
                    continue

                scores = dto.get("sleepScores") or {}
                overall = scores.get("overall") or {}

                rows.append((
                    calendar_date,
                    "garmin",
                    sleep_start,
                    sleep_end,
                    total_seconds,
                    dto.get("deepSleepSeconds"),
                    dto.get("lightSleepSeconds"),
                    dto.get("remSleepSeconds"),
                    dto.get("awakeSleepSeconds"),
                    dto.get("napTimeSeconds"),
                    dto.get("unmeasurableSleepSeconds"),
                    overall.get("value"),
                    overall.get("qualifierKey"),
                    sleep_data.get("restlessMomentsCount"),
                    dto.get("avgHeartRate"),
                    sleep_data.get("avgOvernightHrv"),
                    dto.get("averageRespirationValue"),
                    sleep_data.get("restingHeartRate"),
                    sleep_data.get("bodyBatteryChange"),
                    dto.get("averageSpO2Value"),
                    dto.get("lowestSpO2Value"),
                    dto.get("awakeCount"),
                ))
            except Exception as e:
                logger.warning(f"[sleep] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[sleep] Days: {len(rows)}")
        return rows

    def write(self, conn, rows) -> Optional[datetime]:
        if not rows:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO sleep_daily
                    (date, source, sleep_start_at, sleep_end_at,
                     total_seconds, deep_seconds, light_seconds, rem_seconds,
                     awake_seconds, nap_seconds, unmeasurable_seconds,
                     score_overall, score_qualifier, restless_moments,
                     avg_heart_rate, avg_overnight_hrv, avg_respiration,
                     resting_heart_rate, body_battery_change,
                     avg_spo2, lowest_spo2, awake_count)
                VALUES %s
                ON CONFLICT (date, source) DO UPDATE SET
                    sleep_start_at       = EXCLUDED.sleep_start_at,
                    sleep_end_at         = EXCLUDED.sleep_end_at,
                    total_seconds        = EXCLUDED.total_seconds,
                    deep_seconds         = EXCLUDED.deep_seconds,
                    light_seconds        = EXCLUDED.light_seconds,
                    rem_seconds          = EXCLUDED.rem_seconds,
                    awake_seconds        = EXCLUDED.awake_seconds,
                    nap_seconds          = EXCLUDED.nap_seconds,
                    unmeasurable_seconds = EXCLUDED.unmeasurable_seconds,
                    score_overall        = EXCLUDED.score_overall,
                    score_qualifier      = EXCLUDED.score_qualifier,
                    restless_moments     = EXCLUDED.restless_moments,
                    avg_heart_rate       = EXCLUDED.avg_heart_rate,
                    avg_overnight_hrv    = EXCLUDED.avg_overnight_hrv,
                    avg_respiration      = EXCLUDED.avg_respiration,
                    resting_heart_rate   = EXCLUDED.resting_heart_rate,
                    body_battery_change  = EXCLUDED.body_battery_change,
                    avg_spo2             = EXCLUDED.avg_spo2,
                    lowest_spo2          = EXCLUDED.lowest_spo2,
                    awake_count          = EXCLUDED.awake_count,
                    updated_at           = NOW()
                """,
                rows,
            )
        conn.commit()

        max_date = max(r[0] for r in rows)
        return datetime.combine(max_date, datetime.min.time(), tzinfo=timezone.utc)
