from queue import Queue
from threading import Event, Thread

class TimedBatchWorker:
    """
    A worker class that batches work items and flushes them to the provided
    handler whenever the batch is filled, or when the timeout is reached.
    
    Ref.: https://github.com/Minibrams/timed-batch-worker
    
    Parameters
    ----------
    
    handler: The function that will be called when the batch is flushed.
                    The batch of work items will be passed as a list.
    flush_interval: The interval (seconds) between which the batch is flushed.
    flush_batch_size: The max batch size at which the batch is flushed.
    """
    
    def __init__(self, handler, flush_interval=5, flush_batch_size=20 ):
        """Class constructor

        Args:
            handler (_type_): _description_
            flush_interval (int, optional): _description_. Defaults to 5.
            flush_batch_size (int, optional): _description_. Defaults to 20.
        """
        self.is_running = False

        self._queue = Queue()
        self._handler = handler
        self._flush_interval = flush_interval
        self._flush_batch_size = flush_batch_size
        self._item_enqueued_event = None

        self._timed_worker_thread = None
        self._batch_worker_thread = None
        self._flush_worker_thread = None
    def _timed_worker(self, flush_event: Event):
        while True:
            sleep(self._flush_interval)
            flush_event.set()

    def _batch_worker(self, log_created_event: Event, flush_event: Event):
        while True:
            log_created_event.wait()
            if self._queue.unfinished_tasks > self._flush_batch_size:
                flush_event.set()

            log_created_event.clear()

    def _flush_worker(self, do_flush_event: Event):
        batch = []

        while True:
            do_flush_event.wait()

            while not self._queue.empty():
                batch.append(self._queue.get())
                self._queue.task_done()

            try:
                self._handler(batch)
            except Exception as e:
                logging.error(
                    f'TimedBatchWorker encountered an error while calling handler: {e}'
                )

            batch = []
            do_flush_event.clear()

    def start(self):
        self._item_enqueued_event = Event()

        flush_timeout_event = Event()
        flush_full_batch_event = Event()

        do_flush_event = any_event(
            flush_timeout_event,
            flush_full_batch_event
        )

        self._batch_worker_thread = Thread(target=self._batch_worker, args=(
            self._item_enqueued_event, flush_full_batch_event,), daemon=True)
        self._timed_worker_thread = Thread(target=self._timed_worker, args=(
            flush_timeout_event,), daemon=True)
        self._flush_worker_thread = Thread(target=self._flush_worker, args=(
            do_flush_event,), daemon=True)

        self._timed_worker_thread.start()
        self._batch_worker_thread.start()
        self._flush_worker_thread.start()

        self.is_running = True

    def enqueue(self, item):
        self._queue.put(item)
        self._item_enqueued_event.set()