"""Pull intraday steps."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class StepsPuller(BasePuller):
    pull_type = "steps"
    overlap_minutes = 60
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        """Тянет шаги intraday по дням."""
        all_samples = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[steps] Fetching {date_str}")
            try:
                # get_steps_data возвращает массив 15-минутных интервалов
                # вида [{startGMT, endGMT, steps, ...}, ...]
                steps_data = self.garmin.get_steps_data(date_str)
                for entry in (steps_data or []):
                    start_str = entry.get("startGMT")
                    steps = entry.get("steps")
                    if not start_str or steps is None:
                        continue
                    # startGMT обычно в формате "2026-04-25T08:15:00.0"
                    start_ts = datetime.fromisoformat(
                        start_str.replace("Z", "+00:00").rstrip("0").rstrip(".")
                        if "T" in start_str else start_str
                    )
                    if start_ts.tzinfo is None:
                        start_ts = start_ts.replace(tzinfo=timezone.utc)

                    if date_from <= start_ts <= date_to:
                        all_samples.append((start_ts, int(steps), "garmin"))
            except Exception as e:
                logger.warning(f"[steps] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[steps] Total intervals: {len(all_samples)}")
        return all_samples

    def write(self, conn, samples) -> Optional[datetime]:
        if not samples:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO steps (period_start, steps, source)
                VALUES %s
                ON CONFLICT (period_start, source) DO NOTHING
                """,
                samples,
            )
        conn.commit()

        return max(s[0] for s in samples)
