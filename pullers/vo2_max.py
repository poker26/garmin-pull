"""Pull VO2 Max daily."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class Vo2MaxPuller(BasePuller):
    pull_type = "vo2_max"
    overlap_minutes = 0
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        rows = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[vo2_max] Fetching {date_str}")
            try:
                # max_metrics возвращает массив словарей с running/cycling
                data = self.garmin.get_max_metrics(date_str)
                if not data:
                    cur_date += timedelta(days=1)
                    continue

                # Структура: [{ "userId": ..., "calendarDate": "...",
                #               "generic": { "vo2MaxPreciseValue": ..., "fitnessAge": ...},
                #               "cycling": { "vo2MaxPreciseValue": ... }, ... }]
                entry = data[0] if isinstance(data, list) else data
                generic = entry.get("generic") or {}
                cycling = entry.get("cycling") or {}

                running_v = generic.get("vo2MaxPreciseValue") or generic.get("vo2MaxValue")
                cycling_v = cycling.get("vo2MaxPreciseValue") or cycling.get("vo2MaxValue")
                fitness_age = generic.get("fitnessAge")

                if running_v is None and cycling_v is None and fitness_age is None:
                    cur_date += timedelta(days=1)
                    continue

                rows.append((
                    cur_date, "garmin",
                    running_v, cycling_v,
                    int(fitness_age) if fitness_age else None,
                ))
            except Exception as e:
                logger.warning(f"[vo2_max] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[vo2_max] Days: {len(rows)}")
        return rows

    def write(self, conn, rows) -> Optional[datetime]:
        if not rows:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO vo2_max_daily
                    (date, source, running_value, cycling_value, fitness_age)
                VALUES %s
                ON CONFLICT (date, source) DO UPDATE SET
                    running_value = EXCLUDED.running_value,
                    cycling_value = EXCLUDED.cycling_value,
                    fitness_age   = EXCLUDED.fitness_age,
                    updated_at    = NOW()
                """,
                rows,
            )
        conn.commit()

        max_date = max(r[0] for r in rows)
        return datetime.combine(max_date, datetime.min.time(), tzinfo=timezone.utc)
