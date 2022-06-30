import pathlib
import sys
import typing
import itertools


data_dir = pathlib.Path.cwd() / "data"
raw_dir = data_dir / ".raw"
Record = typing.NamedTuple
STDOUT = "stdout"


def ensure_data_dir(fn):
    raw_dir.mkdir(parents=True, exist_ok=True)
    return fn


ids = itertools.count(0)


@ensure_data_dir
def write_text(data: str) -> pathlib.Path:
    path = raw_dir / str(next(ids))
    path.write_text(data)
    return path


@ensure_data_dir
def write_bytes(data: bytes) -> pathlib.Path:
    path = raw_dir / str(next(ids))
    path.write_bytes(data)
    return path


@ensure_data_dir
def save_records(records, output=None) -> None:
    records = tuple(records)
    if output == STDOUT:
        path = STDOUT
    else:
        if output is None:
            path = data_dir / records[0].__class__.__name__
        else:
            path = data_dir / output
        if not path.exists():
            path.write_text(",".join(records[0]._fields) + "\n")
    csv_lines = []
    for record in records:
        csv_lines.append(",".join(str(elem) for elem in record))
    data = "\n".join(csv_lines) + "\n"
    if path == STDOUT:
        print(data)
    else:
        with open(path, "a") as f:
            f.write(data)
