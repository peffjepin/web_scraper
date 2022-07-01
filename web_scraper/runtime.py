import collections

from . import model


class _JobBoard:
    def __init__(self):
        self._web_pending = collections.deque()
        self._cleaning_pending = collections.deque()
        self._downloading = dict()
        self._parsing = dict()

    @property
    def _job_caches(self):
        yield self._web_pending
        yield self._cleaning_pending
        yield self._downloading
        yield self._parsing

    def post(self, job):
        self._web_pending.append(job)

    def report_event(self, event):
        if isinstance(event, model.WebTaskComplete):
            job = self._downloading.pop(event.job_id)
            job.path = event.path
            self._cleaning_pending.append(job)
        elif isinstance(event, model.SnapshotTaken):
            job = self._downloading[event.job_id]
            pending = _SnapshotCleaningPendingWrapper(job, event)
            self._cleaning_pending.append(pending)
        elif isinstance(event, model.SeleniumSnapshotsTaskComplete):
            self._downloading.pop(event.job_id)
        elif isinstance(event, model.CleaningTaskComplete):
            self._parsing.pop(event.job_id)
        else:
            raise TypeError(f"Unrecognized event: {event}")

    def get_web_task(self):
        try:
            job = self._web_pending.popleft()
        except IndexError:
            return None

        if isinstance(job, model.SeleniumSnapshotsJob):
            task = model.SeleniumSnapshotsTask(
                job.url, job.driver_script, job.id
            )
        else:
            task = model.WebTask(job.url, job.id)

        self._downloading[job.id] = job
        return task

    def get_cleaning_task(self):
        try:
            job = self._cleaning_pending.popleft()
        except IndexError:
            return None

        if isinstance(job, _SnapshotCleaningPendingWrapper):
            task = job.task
        else:
            task = model.CleaningTask(job.path, job.cleaner, job.id)

        self._parsing[job.id] = job
        return task

    def has_pending_jobs(self):
        return any(len(cache) != 0 for cache in self._job_caches)


class _SnapshotCleaningPendingWrapper:
    def __init__(
        self, job: model.SeleniumSnapshotsJob, event: model.SnapshotTaken
    ):
        self.id = next(model.job_counter)
        self.task = model.SnapshotCleaningTask(
            event.paths, job.cleaner, self.id
        )

    def __str__(self):
        return str(self.job)

    def __repr__(self):
        return repr(self.job)


_job_board = _JobBoard()
has_pending_jobs = _job_board.has_pending_jobs
add_job = _job_board.post
get_web_task = _job_board.get_web_task
get_cleaning_task = _job_board.get_cleaning_task
report_event = _job_board.report_event
