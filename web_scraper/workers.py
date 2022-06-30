import collections
import requests
import time
import typing

import mpcontroller as mpc

from . import runtime
from . import model
from . import resources
from .runtime import debug


class _RequestsTasksWorker(mpc.Worker):
    _TASK_REQUEST_INTERVAL = 0.1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._previous_task_request = 0

    def mainloop(self):
        dt = time.time() - self._previous_task_request
        if dt < self._TASK_REQUEST_INTERVAL:
            time.sleep(self._TASK_REQUEST_INTERVAL - dt)
        if self.status == mpc.IDLE:
            self.request_task()

    def request_task(self):
        raise NotImplementedError("should be implemented in child")


class RequestWebTask(mpc.Signal):
    pass


class WebWorker(_RequestsTasksWorker):
    def request_task(self):
        self.send(RequestWebTask)

    @mpc.handler.main(RequestWebTask)
    def try_to_get_a_new_web_task(self):
        if (task := runtime.get_web_task()) is not None:
            self.send(task)

    @mpc.handler.worker(model.WebTask)
    def fetch_web_content(self, task):
        debug(f"fetching url: {task}")
        r = requests.get(task.url)
        path = resources.write_text(r.text)
        event = model.WebTaskComplete(path, task.job_id)
        self.send(event)

    @mpc.handler.main(model.WebTaskComplete)
    def notify_task_complete(self, event):
        runtime.report_event(event)


class RequestCleaningTask(mpc.Signal):
    pass


class NewJobCreated(mpc.Event):
    job: model.Job


class CleaningWorker(_RequestsTasksWorker):
    def request_task(self):
        self.send(RequestCleaningTask)

    @mpc.handler.main(RequestCleaningTask)
    def try_to_get_a_new_cleaning_task(self):
        if (task := runtime.get_cleaning_task()) is not None:
            self.send(task)

    @mpc.handler.main(NewJobCreated)
    def add_job_to_runtime(self, event):
        runtime.add_job(event.job)

    @mpc.handler.worker(model.CleaningTask)
    def clean_raw_data(self, task):
        debug(f"cleaning data: {task}")
        raw_text = task.path.read_text()
        if isinstance(task.cleaner, model.DataCleaner):
            ret = task.cleaner.clean(raw_text)
            out = task.cleaner.OUTPUT
        else:
            ret = task.cleaner(raw_text)
            out = None
        self._handle_cleaner_return_value(ret, out)
        self.send(model.CleaningTaskComplete(task.job_id))

    @mpc.handler.main(model.CleaningTaskComplete)
    def notify_task_complete(self, event):
        runtime.report_event(event)

    def _handle_cleaner_return_value(self, value, output):
        if isinstance(value, typing.Generator):
            return self._exhaust_generator(value, output)

    def _exhaust_generator(self, gen, output):
        accumulated_records = collections.defaultdict(list)
        for val in gen:
            if isinstance(val, model.Job):
                self.send(NewJobCreated(val))
            else:
                accumulated_records[type(val)].append(val)
        for records in accumulated_records.values():
            resources.save_records(records, output)
