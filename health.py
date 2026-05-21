"""HTTP /health endpoint для мониторинга Uptime Kuma.

Свежесть оцениваем по last_success_at (когда puller последний раз УСПЕШНО запускался),
а не по last_data_timestamp (свежесть сэмплов). Сервис может быть здоров,
даже если часы давно не синкались — это не наша проблема, а проблема пользователя.
"""
import logging
from datetime import datetime, timezone

from aiohttp import web

from pullers.base import get_db_conn

logger = logging.getLogger(__name__)


# Максимальный возраст последнего УСПЕШНОГО запуска puller'а в минутах.
# Если pull дольше не запускался — scheduler/auth/Garmin API сломаны.
# Опциональные каналы (None) не валим в degraded.
MAX_LAST_SUCCESS_AGE_MIN = {
    "heart_rate":    30,    # запускается каждые 15 мин, +запас
    "steps":         30,
    "body_battery":  30,
    "stress":        30,
    "exercises":    150,    # запускается каждый час, +запас
    "training_load": 36 * 60,   # запускается раз в сутки в 10:00 МСК
    "hrv":           36 * 60,
    "vo2_max":       36 * 60,
}

# Опциональные каналы — не валим overall_status, даже если что-то не так.
# (HRV и VO2 Max могут не иметь данных неделями — это норма для нашего сетапа.)
OPTIONAL = {"hrv", "vo2_max"}


def _evaluate_pull(row, now: datetime) -> dict:
    pull_type = row["pull_type"]
    threshold_min = MAX_LAST_SUCCESS_AGE_MIN.get(pull_type)
    last_success = row.get("last_success_at")
    last_data = row.get("last_data_timestamp")
    consecutive_errors = row.get("consecutive_errors") or 0
    optional = pull_type in OPTIONAL

    fresh = True
    reason = None

    # Проверка 1: puller вообще запускался успешно?
    if last_success is None:
        fresh = False
        reason = "never succeeded"
    elif threshold_min is not None:
        age_min = (now - last_success).total_seconds() / 60
        if age_min > threshold_min:
            fresh = False
            reason = f"last success {int(age_min)}min ago > {threshold_min}min"

    # Проверка 2: накопились ошибки подряд?
    if consecutive_errors >= 3:
        fresh = False
        reason = f"errors: {consecutive_errors} in a row"

    return {
        "last_success": last_success.isoformat() if last_success else None,
        "last_data": last_data.isoformat() if last_data else None,
        "last_status": row.get("last_status"),
        "consecutive_errors": consecutive_errors,
        "fresh": fresh,
        "reason": reason,
        "optional": optional,
    }


async def health_handler(request: web.Request) -> web.Response:
    now = datetime.now(timezone.utc)
    pulls = {}
    overall_ok = True

    try:
        conn = get_db_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pull_type, last_attempt_at, last_success_at,
                           last_data_timestamp, last_status, last_error,
                           consecutive_errors
                    FROM garmin_pull_state
                """)
                cols = [d.name for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        finally:
            conn.close()

        for row in rows:
            evaluation = _evaluate_pull(row, now)
            pulls[row["pull_type"]] = evaluation
            if not evaluation["fresh"] and not evaluation["optional"]:
                overall_ok = False

        # Обязательные каналы должны существовать
        for pt, threshold in MAX_LAST_SUCCESS_AGE_MIN.items():
            if pt in OPTIONAL:
                continue
            if pt not in pulls:
                pulls[pt] = {
                    "fresh": False, "optional": False,
                    "reason": "never ran", "consecutive_errors": 0,
                    "last_success": None, "last_data": None, "last_status": None,
                }
                overall_ok = False
    except Exception as e:
        logger.exception("Health check failed")
        return web.json_response(
            {"status": "error", "error": str(e)[:200]},
            status=500,
        )

    return web.json_response({
        "status": "ok" if overall_ok else "degraded",
        "checked_at": now.isoformat(),
        "pulls": pulls,
    })


def start_health_server(port: int = 8080):
    import asyncio
    import threading

    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        app = web.Application()
        app.router.add_get("/health", health_handler)
        runner = web.AppRunner(app)
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, "0.0.0.0", port)
        loop.run_until_complete(site.start())
        logger.info(f"Health server listening on :{port}/health")
        loop.run_forever()

    t = threading.Thread(target=run, daemon=True, name="health-server")
    t.start()
    return t
