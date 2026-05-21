"""Garmin pull service — long-running with APScheduler."""
import argparse
import logging
import logging.handlers
import os
import signal
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from garminconnect import Garmin, GarminConnectAuthenticationError

from pullers.base import get_db_conn
from pullers.heart_rate import HeartRatePuller
from pullers.steps import StepsPuller
from pullers.body_battery import BodyBatteryPuller
from pullers.stress import StressPuller
from pullers.hrv import HrvPuller
from pullers.vo2_max import Vo2MaxPuller
from pullers.training_load import TrainingLoadPuller
from pullers.exercises import ExercisesPuller
from health import start_health_server

load_dotenv()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
HEALTH_PORT = int(os.environ.get("HEALTH_PORT", "8080"))
TOKENS_DIR = Path("/app/tokens")
LOGS_DIR = Path("/app/logs")
TOKENS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

PULL_LOCK = threading.Lock()


def setup_logging():
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    logging.basicConfig(level=LOG_LEVEL, format=fmt, stream=sys.stdout)
    file_handler = logging.handlers.RotatingFileHandler(
        LOGS_DIR / "garmin_pull.log",
        maxBytes=10_000_000, backupCount=5
    )
    file_handler.setFormatter(logging.Formatter(fmt))
    logging.getLogger().addHandler(file_handler)


def _prompt_mfa() -> str:
    if not sys.stdin.isatty():
        raise RuntimeError(
            "MFA code requested, but stdin is not a TTY. "
            "Run first login via: docker compose run --rm garmin-pull python garmin_pull.py --once"
        )
    return input("Garmin MFA code (check email): ").strip()


def init_garmin_client() -> Garmin:
    email = os.environ["GARMIN_EMAIL"]
    password = os.environ["GARMIN_PASSWORD"]
    token_dir = str(TOKENS_DIR)

    client = Garmin(email=email, password=password, prompt_mfa=_prompt_mfa)
    client.login(token_dir)
    logging.info(f"Garmin login OK, tokens in {token_dir}")
    return client


def run_pullers(client, puller_classes, label: str):
    log = logging.getLogger("scheduler")
    if not PULL_LOCK.acquire(blocking=False):
        log.warning(f"[{label}] Skip — another job is running")
        return

    try:
        log.info(f"[{label}] Starting {len(puller_classes)} pullers")
        conn = get_db_conn()
        try:
            for cls in puller_classes:
                puller = cls(client)
                puller.run(conn)
        finally:
            conn.close()
        log.info(f"[{label}] Done")
    except Exception:
        log.exception(f"[{label}] Failed")
    finally:
        PULL_LOCK.release()


def main_long_running():
    setup_logging()
    log = logging.getLogger("main")
    log.info("Garmin pull service starting (long-running mode)")

    start_health_server(HEALTH_PORT)

    try:
        client = init_garmin_client()
    except GarminConnectAuthenticationError as e:
        log.error(f"Auth failed: {e}")
        time.sleep(60)
        sys.exit(1)
    except Exception:
        log.exception("Failed to init Garmin client")
        time.sleep(60)
        sys.exit(1)

    scheduler = BackgroundScheduler(timezone="UTC")

    now_utc = datetime.now(timezone.utc)

    # Каждые 15 минут: intraday. Первый запуск через 1 минуту после старта.
    scheduler.add_job(
        run_pullers,
        IntervalTrigger(minutes=15),
        args=[client, [HeartRatePuller, StepsPuller, BodyBatteryPuller, StressPuller], "intraday"],
        id="intraday",
        next_run_time=now_utc + timedelta(minutes=1),
    )

    # Каждый час: exercises. Первый запуск через 5 минут (даём intraday отработать первым).
    scheduler.add_job(
        run_pullers,
        IntervalTrigger(hours=1),
        args=[client, [ExercisesPuller], "exercises"],
        id="exercises",
        next_run_time=now_utc + timedelta(minutes=5),
    )

    # Каждый день в 07:00 UTC (10:00 МСК): daily-метрики
    scheduler.add_job(
        run_pullers,
        CronTrigger(hour=7, minute=0, timezone="UTC"),
        args=[client, [HrvPuller, Vo2MaxPuller, TrainingLoadPuller], "daily"],
        id="daily",
    )

    scheduler.start()
    log.info("Scheduler started. Jobs:")
    for job in scheduler.get_jobs():
        log.info(f"  - {job.id}: next run at {job.next_run_time}")

    log.info("Initial pull on startup")
    run_pullers(
        client,
        [HeartRatePuller, StepsPuller, BodyBatteryPuller, StressPuller,
         ExercisesPuller, HrvPuller, Vo2MaxPuller, TrainingLoadPuller],
        "startup",
    )

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        log.info(f"Got signal {signum}, shutting down")
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    while not stop_event.is_set():
        stop_event.wait(timeout=60)

    log.info("Stopping scheduler")
    scheduler.shutdown(wait=True)
    log.info("Bye")


def main_one_shot():
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true",
                        help="Run all pullers once and exit (manual mode).")
    parser.add_argument("--backfill-exercises", type=int, metavar="DAYS",
                        help="Run ONLY exercises puller with custom backfill depth.")
    args = parser.parse_args()

    setup_logging()
    log = logging.getLogger("main")
    log.info(f"Garmin pull service starting (one-shot mode)")

    try:
        client = init_garmin_client()
    except Exception:
        log.exception("Failed to init Garmin client")
        sys.exit(1)

    if args.backfill_exercises is not None:
        log.info(f"=== MANUAL BACKFILL: exercises, {args.backfill_exercises} days ===")
        pullers = [ExercisesPuller(client, backfill_override=args.backfill_exercises)]
    else:
        pullers = [
            HeartRatePuller(client), StepsPuller(client),
            BodyBatteryPuller(client), StressPuller(client),
            HrvPuller(client), Vo2MaxPuller(client),
            TrainingLoadPuller(client), ExercisesPuller(client),
        ]

    conn = get_db_conn()
    try:
        for puller in pullers:
            puller.run(conn)
    finally:
        conn.close()

    log.info("One-shot run complete")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main_one_shot()
    else:
        main_long_running()
