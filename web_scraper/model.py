import itertools
import collections
import pathlib
import typing

import mpcontroller as mpc


class DataCleaner:
    def clean(self, text):
        raise NotImplementedError("should be implemented in child")

    @classmethod
    def make_jobs(cls, *urls):
        return (Job(url, cls()) for url in urls)


_CleanerType = typing.Callable | DataCleaner
_job_counter = itertools.count(0)


class Job:
    def __init__(self, url: str, cleaner: _CleanerType):
        self.url = url
        self.cleaner = cleaner
        self.id = next(_job_counter)
        self.path = None

    def __str__(self):
        return f"ScraperJob(url={self.url}, cleaner={self.cleaner})"


class WebTask(mpc.Task):
    url: str
    job_id: int


class WebTaskComplete(mpc.Event):
    path: pathlib.Path
    job_id: int


class CleaningTask(mpc.Task):
    path: pathlib.Path
    cleaner: _CleanerType
    job_id: int


class CleaningTaskComplete(mpc.Event):
    job_id: int
