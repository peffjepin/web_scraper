import time
import argparse

import mpcontroller as mpc

from . import runtime
from . import workers


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--debug", action="store_true")
    args = parser.parse_args()
    if args.debug:
        runtime.config.debug = True


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
