import threading

from typing import NamedTuple, Any


class ThreadResult(NamedTuple):
    is_exception: bool
    result: Any


class ThreadWithResult(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, *, daemon=None):
        self.result = None

        def func():
            _kwargs = kwargs if kwargs else {}
            self.result = target(*args, **_kwargs)

        super().__init__(group=group, target=func, name=name, daemon=daemon)
