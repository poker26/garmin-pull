"""Pull training load (acute/chronic) — current snapshot only."""
import logging
from datetime import datetime, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class TrainingLoadPuller(BasePuller):
    pull_type = "training_load"
    overlap_minutes = 0
    backfill_days = 0  # тянем только сегодня — снапшот

    def fetch(self, date_from: datetime, date_to: datetime):
        rows = []
        today = datetime.now(timezone.utc).date()
        date_str = today.isoformat()
        logger.info(f"[training_load] Fetching {date_str} (current snapshot)")

        try:
            ts = self.garmin.get_training_status(date_str)
            if not ts:
                logger.info("[training_load] Empty response")
                return []

            tsd = (ts.get("mostRecentTrainingStatus") or {}).get("latestTrainingStatusData") or {}
            latest = next(iter(tsd.values()), {}) if tsd else {}
            acute_dto = latest.get("acuteTrainingLoadDTO") or {}

            acute  = acute_dto.get("dailyTrainingLoadAcute")
            chronic = acute_dto.get("dailyTrainingLoadChronic")
            ratio  = acute_dto.get("dailyAcuteChronicWorkloadRatio")
            acwr_status = acute_dto.get("acwrStatus")
            ts_phrase = latest.get("trainingStatusFeedbackPhrase")

            if all(v is None for v in (acute, chronic, ratio, acwr_status)):
                logger.info("[training_load] No data fields populated")
                return []

            rows.append((
                today, "garmin",
                float(acute) if acute is not None else None,
                float(chronic) if chronic is not None else None,
                float(ratio) if ratio is not None else None,
                acwr_status,
                ts_phrase,
            ))
        except Exception as e:
            logger.warning(f"[training_load] Failed: {e}")

        logger.info(f"[training_load] Days: {len(rows)}")
        return rows

    def write(self, conn, rows) -> Optional[datetime]:
        if not rows:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO training_load_daily
                    (date, source, acute_load_7d, chronic_load_28d, ratio, status, focus)
                VALUES %s
                ON CONFLICT (date, source) DO UPDATE SET
                    acute_load_7d    = EXCLUDED.acute_load_7d,
                    chronic_load_28d = EXCLUDED.chronic_load_28d,
                    ratio            = EXCLUDED.ratio,
                    status           = EXCLUDED.status,
                    focus            = EXCLUDED.focus,
                    updated_at       = NOW()
                """,
                rows,
            )
        conn.commit()

        max_date = max(r[0] for r in rows)
        return datetime.combine(max_date, datetime.min.time(), tzinfo=timezone.utc)
