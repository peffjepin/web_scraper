import time
import argparse
import logging

import mpcontroller as mpc

from . import runtime
from . import workers

_log_levels_map = {
    "info": logging.INFO,
    "debug": logging.DEBUG,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
    "warning": logging.WARNING,
}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--logging", default="warning")
    args = parser.parse_args()
    assert args.logging in _log_levels_map
    level = _log_levels_map[args.logging]
    logging.basicConfig(format="%(asctime)s ::: %(message)s", level=level)


def scrape(*job_objects):
    parse_args()
    for job in job_objects:
        runtime.add_job(job)
    web_worker = workers.WebWorker.spawn()
    cleaning_worker = workers.CleaningWorker.spawn()
    while runtime.has_pending_jobs():
        if web_worker.status == mpc.DEAD or cleaning_worker.status == mpc.DEAD:
            mpc.kill_all()
            raise SystemExit(1)
        time.sleep(0.05)
    web_worker.join()
    cleaning_worker.join()
    return 0
