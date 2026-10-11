"""One process per data directory, on Windows, macOS and Linux."""

from contextlib import contextmanager
import os


@contextmanager
def instance_lock(path):
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            handle.seek(0, 2)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                raise ValueError("Помощник уже запущен с этим DATA_DIR.") from None
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ValueError("Помощник уже запущен с этим DATA_DIR.") from None
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)
