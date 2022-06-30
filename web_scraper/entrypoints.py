import pathlib
import time

import mpcontroller as mpc

from . import runtime
from . import model
from . import workers


def scrape(*job_objects):
    for job in job_objects:
        runtime.add_job(job)
    web_worker = workers.WebWorker.spawn()
    cleaning_worker = workers.CleaningWorker.spawn()
    while runtime.has_pending_jobs():
        if web_worker.status == mpc.DEAD or cleaning_worker.status == mpc.DEAD:
            raise SystemExit(1)
        time.sleep(0.05)
    web_worker.join()
    cleaning_worker.join()
    return 0
