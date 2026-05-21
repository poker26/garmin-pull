"""Pull exercises (activities) with intraday samples."""
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import psycopg2
from psycopg2.extras import Json

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


SAMPLE_KEYS = {
    "directHeartRate":   "hr",
    "directSpeed":       "speed_mps",
    "directElevation":   "elevation_m",
    "directLatitude":    "lat",
    "directLongitude":   "lon",
    "directRunCadence":  "cadence",
    "directDoubleCadence": "cadence_double",
    "directBodyBattery": "body_battery",
}
TIMESTAMP_KEY = "directTimestamp"


def _parse_garmin_ts(ts_str: str) -> Optional[datetime]:
    if not ts_str:
        return None
    try:
        dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _extract_samples(details: dict) -> Optional[list]:
    descriptors = details.get("metricDescriptors") or []
    metrics_data = details.get("activityDetailMetrics") or []

    if not descriptors or not metrics_data:
        return None

    idx_map = {d["key"]: d["metricsIndex"] for d in descriptors}
    ts_idx = idx_map.get(TIMESTAMP_KEY)
    if ts_idx is None:
        return None

    field_indices = {
        out_name: idx_map[in_name]
        for in_name, out_name in SAMPLE_KEYS.items()
        if in_name in idx_map
    }

    samples = []
    for entry in metrics_data:
        m = entry.get("metrics") or []
        if len(m) <= ts_idx:
            continue
        ts_ms = m[ts_idx]
        if ts_ms is None:
            continue

        sample = {"t": int(ts_ms)}
        for out_name, i in field_indices.items():
            if i < len(m) and m[i] is not None:
                sample[out_name] = m[i]
        samples.append(sample)

    return samples or None


class ExercisesPuller(BasePuller):
    pull_type = "exercises"
    overlap_minutes = 0
    backfill_days = 30

    BATCH_SIZE = 20
    DETAILS_DELAY_SEC = 0.5

    def __init__(self, garmin_client, backfill_override: Optional[int] = None):
        super().__init__(garmin_client)
        self.force_backfill = backfill_override is not None
        if backfill_override is not None:
            self.backfill_days = backfill_override
            self.overlap_minutes = 0

    def determine_window(self, conn) -> tuple[datetime, datetime]:
        """В режиме force_backfill игнорируем сохранённый state."""
        if self.force_backfill:
            now = datetime.now(timezone.utc)
            date_from = now - timedelta(days=self.backfill_days)
            logger.info(
                f"[{self.pull_type}] FORCE BACKFILL {self.backfill_days} days "
                f"(ignoring saved state)"
            )
            return date_from, now
        return super().determine_window(conn)

    def fetch(self, date_from: datetime, date_to: datetime):
        results = []
        offset = 0
        seen_ids = set()

        while True:
            logger.info(f"[exercises] Fetching list batch offset={offset}")
            try:
                batch = self.garmin.get_activities(offset, self.BATCH_SIZE)
            except Exception as e:
                logger.warning(f"[exercises] List fetch failed: {e}")
                break

            if not batch:
                break

            stop = False
            for act in batch:
                aid = act.get("activityId")
                if aid is None or aid in seen_ids:
                    continue
                seen_ids.add(aid)

                start_ts = _parse_garmin_ts(act.get("startTimeGMT"))
                if start_ts is None:
                    continue

                if start_ts < date_from:
                    stop = True
                    break
                if start_ts > date_to:
                    continue

                logger.info(
                    f"[exercises] Details for {aid} "
                    f"({(act.get('activityName') or '')[:40]})"
                )
                try:
                    details = self.garmin.get_activity_details(aid)
                except Exception as e:
                    logger.warning(f"[exercises] Details {aid} failed: {e}")
                    details = None

                results.append((act, details))
                time.sleep(self.DETAILS_DELAY_SEC)

            if stop or len(batch) < self.BATCH_SIZE:
                break
            offset += self.BATCH_SIZE

        logger.info(f"[exercises] Activities fetched: {len(results)}")
        return results

    def write(self, conn, results) -> Optional[datetime]:
        if not results:
            return None

        max_ts = None
        with conn.cursor() as cur:
            for act, details in results:
                aid = act.get("activityId")
                start_ts = _parse_garmin_ts(act.get("startTimeGMT"))
                if start_ts is None or aid is None:
                    continue
                duration_sec = act.get("duration")
                end_ts = start_ts + timedelta(seconds=duration_sec) if duration_sec else start_ts

                samples = _extract_samples(details) if details else None

                cur.execute(
                    """
                    INSERT INTO exercises (
                        garmin_activity_id, source, exercise_type,
                        start_time, end_time,
                        duration_minutes, duration_seconds,
                        distance_meters, calories,
                        avg_hr, max_hr,
                        avg_speed_mps, max_speed_mps,
                        elevation_gain_m, elevation_loss_m,
                        avg_cadence, max_cadence,
                        training_effect_aerobic, training_effect_anaerobic,
                        training_load,
                        samples
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (garmin_activity_id) DO UPDATE SET
                        exercise_type             = EXCLUDED.exercise_type,
                        start_time                = EXCLUDED.start_time,
                        end_time                  = EXCLUDED.end_time,
                        duration_minutes          = EXCLUDED.duration_minutes,
                        duration_seconds          = EXCLUDED.duration_seconds,
                        distance_meters           = EXCLUDED.distance_meters,
                        calories                  = EXCLUDED.calories,
                        avg_hr                    = EXCLUDED.avg_hr,
                        max_hr                    = EXCLUDED.max_hr,
                        avg_speed_mps             = EXCLUDED.avg_speed_mps,
                        max_speed_mps             = EXCLUDED.max_speed_mps,
                        elevation_gain_m          = EXCLUDED.elevation_gain_m,
                        elevation_loss_m          = EXCLUDED.elevation_loss_m,
                        avg_cadence               = EXCLUDED.avg_cadence,
                        max_cadence               = EXCLUDED.max_cadence,
                        training_effect_aerobic   = EXCLUDED.training_effect_aerobic,
                        training_effect_anaerobic = EXCLUDED.training_effect_anaerobic,
                        training_load             = EXCLUDED.training_load,
                        samples                   = COALESCE(EXCLUDED.samples, exercises.samples)
                    """,
                    (
                        aid,
                        "garmin",
                        (act.get("activityType") or {}).get("typeKey") or "unknown",
                        start_ts,
                        end_ts,
                        int(duration_sec / 60) if duration_sec else None,
                        int(duration_sec) if duration_sec else None,
                        act.get("distance"),
                        act.get("calories"),
                        int(act["averageHR"]) if act.get("averageHR") else None,
                        int(act["maxHR"]) if act.get("maxHR") else None,
                        act.get("averageSpeed"),
                        act.get("maxSpeed"),
                        act.get("elevationGain"),
                        act.get("elevationLoss"),
                        int(act["averageRunningCadenceInStepsPerMinute"])
                            if act.get("averageRunningCadenceInStepsPerMinute") else None,
                        int(act["maxRunningCadenceInStepsPerMinute"])
                            if act.get("maxRunningCadenceInStepsPerMinute") else None,
                        act.get("aerobicTrainingEffect"),
                        act.get("anaerobicTrainingEffect"),
                        act.get("activityTrainingLoad"),
                        Json(samples) if samples else None,
                    ),
                )

                if max_ts is None or start_ts > max_ts:
                    max_ts = start_ts

        conn.commit()
        return max_ts

    def update_state(self, conn, status: str, last_data_ts=None, error=None):
        """В force_backfill режиме НЕ обновляем last_data_timestamp,
        чтобы не сломать инкрементальный pull в дальнейшем."""
        if self.force_backfill and status == "success":
            # сохраняем существующий state, только обновляем last_attempt_at
            super().update_state(conn, status, last_data_ts=None, error=None)
        else:
            super().update_state(conn, status, last_data_ts, error)
