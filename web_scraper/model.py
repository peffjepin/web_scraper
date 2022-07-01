from __future__ import annotations

import itertools
import pathlib
import typing

import mpcontroller as mpc


class DataCleaner:
    OUTPUT = None

    def clean_text(self, text: str):
        raise NotImplementedError("should be implemented in child")

    def clean_snapshot(self, snap: Snapshot):
        raise NotImplementedError("should be implemented in child")

    def __repr__(self):
        return self.__class__.__name__

    @classmethod
    def make_jobs(cls, *urls):
        return (Job(url, cls()) for url in urls)


job_counter = itertools.count(0)


class Job:
    def __init__(self, url: str, cleaner: DataCleaner):
        self.url = url
        self.cleaner = cleaner
        self.id = next(job_counter)
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
    cleaner: DataCleaner
    job_id: int


class CleaningTaskComplete(mpc.Event):
    job_id: int


class _SeleniumJob(Job):
    def __init__(self, url, cleaner, driver_script):
        self.driver_script = driver_script
        super().__init__(url, cleaner)


class SeleniumSnapshotsJob(_SeleniumJob):
    pass


class SeleniumSnapshotsTask(mpc.Task):
    url: str
    driver_script: typing.Callable
    job_id: int


class SeleniumSnapshotsTaskComplete(mpc.Event):
    job_id: int


class Snapshot:
    def __init__(self, **kwargs):
        # keys should be some kind of identifing label
        # values should be html strings
        self.fields = kwargs

    def __getattr__(self, key):
        try:
            return self.fields[key]
        except KeyError:
            raise KeyError(
                f"{key!r} not present in snapshot... "
                f"valid keys = {tuple(self.fields.keys())!r}"
            )


class SnapshotTaken(mpc.Event):
    # record of where snapshots are located on disk
    paths: dict[str, pathlib.Path]
    job_id: int


class SnapshotCleaningTask(mpc.Task):
    paths: dict[str, pathlib.Path]
    cleaner: DataCleaner
    job_id: int
