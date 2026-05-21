"""Pull sleep sessions into shared sleep_sessions table."""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from psycopg2.extras import Json, execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)

SOURCE = "garmin"
DEFAULT_PACKAGE_NAME = "garmin_connect"

SLEEP_STAGE_BY_LEVEL = {
    0: "deep",
    1: "light",
    2: "rem",
    3: "awake",
}


def _ms_to_datetime(timestamp_ms: Optional[int]) -> Optional[datetime]:
    if timestamp_ms is None:
        return None
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)


def _to_epoch_ms(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric_value = int(value)
        if numeric_value > 1_000_000_000_000:
            return numeric_value
        return numeric_value * 1000
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1000)
    return None


def _seconds_to_minutes(seconds: Optional[int]) -> Optional[int]:
    if seconds is None:
        return None
    return int(round(seconds / 60))


def _map_sleep_stage(activity_level: Any) -> str:
    if isinstance(activity_level, str):
        return activity_level.lower()
    if isinstance(activity_level, int):
        return SLEEP_STAGE_BY_LEVEL.get(activity_level, str(activity_level))
    return str(activity_level)


def _build_stages_raw(sleep_levels: Optional[list]) -> list[dict]:
    stages = []
    for level in sleep_levels or []:
        start_ms = _to_epoch_ms(level.get("startGMT"))
        end_ms = _to_epoch_ms(level.get("endGMT"))
        if start_ms is None or end_ms is None:
            continue
        stages.append({
            "stage": _map_sleep_stage(level.get("activityLevel")),
            "start": start_ms,
            "end": end_ms,
        })
    return stages


def _build_hr_samples(sleep_heart_rate: Optional[list]) -> list[dict]:
    samples = []
    for point in sleep_heart_rate or []:
        start_ms = _to_epoch_ms(point.get("startGMT"))
        value = point.get("value")
        if start_ms is None or value is None:
            continue
        samples.append({"start": start_ms, "value": value})
    return samples


def _extract_hr_stats(
    sleep_heart_rate: Optional[list],
    avg_heart_rate: Optional[float],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    values = [
        float(point["value"])
        for point in (sleep_heart_rate or [])
        if point.get("value") is not None
    ]
    if not values:
        return avg_heart_rate, None, None
    computed_avg = sum(values) / len(values)
    return (
        float(avg_heart_rate) if avg_heart_rate is not None else computed_avg,
        min(values),
        max(values),
    )


def _extract_spo2(sleep_data: dict, dto: dict) -> tuple[Optional[float], Optional[float], Optional[list]]:
    spo2_summary = sleep_data.get("wellnessSpO2SleepSummaryDTO") or {}
    avg_spo2 = dto.get("averageSpO2Value") or spo2_summary.get("averageSPO2")
    min_spo2 = dto.get("lowestSpO2Value") or spo2_summary.get("lowestSPO2")
    spo2_samples = sleep_data.get("wellnessEpochSPO2DataDTOList")
    return avg_spo2, min_spo2, spo2_samples


class SleepPuller(BasePuller):
    pull_type = "sleep"
    overlap_minutes = 0
    backfill_days = 7

    def __init__(self, garmin_client):
        super().__init__(garmin_client)
        self.package_name = os.environ.get("GARMIN_PACKAGE_NAME", DEFAULT_PACKAGE_NAME)
        device_id = os.environ.get("GARMIN_DEVICE_ID", "").strip()
        self.device_id = device_id or None

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
                sleep_start = _ms_to_datetime(dto.get("sleepStartTimestampGMT"))
                sleep_end = _ms_to_datetime(dto.get("sleepEndTimestampGMT"))
                total_seconds = dto.get("sleepTimeSeconds")

                if sleep_start is None or sleep_end is None:
                    if total_seconds is None:
                        cur_date += timedelta(days=1)
                        continue
                    sleep_end = datetime.combine(
                        cur_date + timedelta(days=1),
                        datetime.min.time(),
                        tzinfo=timezone.utc,
                    )
                    sleep_start = sleep_end - timedelta(seconds=total_seconds)

                scores = dto.get("sleepScores") or {}
                overall = scores.get("overall") or {}
                sleep_heart_rate = sleep_data.get("sleepHeartRate")
                avg_hr, min_hr, max_hr = _extract_hr_stats(
                    sleep_heart_rate,
                    dto.get("avgHeartRate"),
                )
                avg_spo2, min_spo2, spo2_samples = _extract_spo2(sleep_data, dto)
                stages_raw = _build_stages_raw(sleep_data.get("sleepLevels"))
                hr_samples = _build_hr_samples(sleep_heart_rate)

                duration_minutes = _seconds_to_minutes(total_seconds)
                if duration_minutes is None:
                    duration_minutes = int(
                        (sleep_end - sleep_start).total_seconds() // 60
                    )

                rows.append((
                    self.device_id,
                    sleep_start,
                    sleep_end,
                    duration_minutes,
                    overall.get("value"),
                    _seconds_to_minutes(dto.get("awakeSleepSeconds")),
                    _seconds_to_minutes(dto.get("remSleepSeconds")),
                    _seconds_to_minutes(dto.get("lightSleepSeconds")),
                    _seconds_to_minutes(dto.get("deepSleepSeconds")),
                    avg_spo2,
                    min_spo2,
                    None,
                    avg_hr,
                    min_hr,
                    max_hr,
                    Json(stages_raw),
                    Json(spo2_samples) if spo2_samples is not None else None,
                    Json(hr_samples) if hr_samples else None,
                    SOURCE,
                    Json(sleep_data),
                    self.package_name,
                ))
            except Exception as error:
                logger.warning(f"[sleep] Skip {date_str}: {error}")
            cur_date += timedelta(days=1)

        logger.info(f"[sleep] Sessions: {len(rows)}")
        return rows

    def write(self, conn, rows) -> Optional[datetime]:
        if not rows:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO sleep_sessions
                    (device_id, start_time, end_time, duration_minutes, sleep_score,
                     awake_minutes, rem_minutes, light_minutes, deep_minutes,
                     avg_spo2, min_spo2, avg_skin_temp,
                     avg_heart_rate, min_heart_rate, max_heart_rate,
                     stages_raw, spo2_samples, hr_samples,
                     source, raw_payload, package_name)
                VALUES %s
                ON CONFLICT (package_name, source, start_time) DO UPDATE SET
                    device_id        = EXCLUDED.device_id,
                    end_time         = EXCLUDED.end_time,
                    duration_minutes = EXCLUDED.duration_minutes,
                    sleep_score      = EXCLUDED.sleep_score,
                    awake_minutes    = EXCLUDED.awake_minutes,
                    rem_minutes      = EXCLUDED.rem_minutes,
                    light_minutes    = EXCLUDED.light_minutes,
                    deep_minutes     = EXCLUDED.deep_minutes,
                    avg_spo2         = EXCLUDED.avg_spo2,
                    min_spo2         = EXCLUDED.min_spo2,
                    avg_skin_temp    = EXCLUDED.avg_skin_temp,
                    avg_heart_rate   = EXCLUDED.avg_heart_rate,
                    min_heart_rate   = EXCLUDED.min_heart_rate,
                    max_heart_rate   = EXCLUDED.max_heart_rate,
                    stages_raw       = EXCLUDED.stages_raw,
                    spo2_samples     = EXCLUDED.spo2_samples,
                    hr_samples       = EXCLUDED.hr_samples,
                    raw_payload      = EXCLUDED.raw_payload
                """,
                rows,
            )
        conn.commit()

        return max(row[2] for row in rows)
