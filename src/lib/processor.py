from multiprocessing import Queue, Event


class Processor:
    _queue: Queue
    _is_stopping: Event
    _dry_run: bool

    def __init__(self, queue: Queue, is_stopping: Event, dry_run: bool = False):
        self._queue = queue
        self._is_stopping = is_stopping
        self._dry_run = dry_run

    def queue(self) -> Queue:
        return self._queue

    def event(self) -> Event:
        return self._is_stopping

    def dry_run(self):
        return self._dry_run

    def set_stopping(self) -> None:
        self._is_stopping.set()

    def is_stopping(self) -> bool:
        return self._is_stopping.is_set()
