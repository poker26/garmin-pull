"""Pull intraday heart rate samples."""
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from psycopg2.extras import execute_values

from pullers.base import BasePuller

logger = logging.getLogger(__name__)


class HeartRatePuller(BasePuller):
    pull_type = "heart_rate"
    overlap_minutes = 60
    backfill_days = 7

    def fetch(self, date_from: datetime, date_to: datetime):
        """Тянет heart rate intraday по дням, начиная с date_from."""
        all_samples = []
        cur_date = date_from.date()
        end_date = date_to.date()

        while cur_date <= end_date:
            date_str = cur_date.isoformat()
            logger.info(f"[heart_rate] Fetching {date_str}")
            try:
                hr_data = self.garmin.get_heart_rates(date_str)
                # Структура: {'heartRateValues': [[timestamp_ms, value], ...], ...}
                values = hr_data.get("heartRateValues") or []
                for ts_ms, value in values:
                    if value is None:
                        continue
                    ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    if date_from <= ts <= date_to:
                        all_samples.append((ts, int(value), "garmin"))
            except Exception as e:
                logger.warning(f"[heart_rate] Skip {date_str}: {e}")
            cur_date += timedelta(days=1)

        logger.info(f"[heart_rate] Total samples: {len(all_samples)}")
        return all_samples

    def write(self, conn, samples) -> Optional[datetime]:
        if not samples:
            return None

        # Фильтруем по check-constraint: bpm BETWEEN 20 AND 250
        valid = [(ts, bpm, src) for ts, bpm, src in samples if 20 <= bpm <= 250]
        skipped = len(samples) - len(valid)
        if skipped:
            logger.info(f"[heart_rate] Skipped {skipped} out-of-range samples")

        if not valid:
            return None

        with conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO heart_rate (measured_at, bpm, source)
                VALUES %s
                ON CONFLICT (measured_at, source) DO NOTHING
                """,
                valid,
            )
        conn.commit()

        max_ts = max(s[0] for s in valid)
        return max_ts
