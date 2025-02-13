import logging
from typing import Dict, List

from qtpy.QtCore import QObject, QThread, Signal, Slot

from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.UserCancellation import UserCancellation
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


class Worker(QObject):
    finished = Signal()
    success = Signal(bool)
    result = Signal(object)
    progress = Signal(int)
    target = None
    args = None

    def __init__(self, target, args=None):
        super().__init__()
        self._dryRun = False
        self._thisThread = None
        self.target = target
        self.args = args

    @Slot()
    def run(self):
        """Long-running task."""
        try:
            self._thisThread = QThread.currentThread()
            if isinstance(self.args, tuple):
                results = self.target(*self.args)
            elif self.args is not None:
                results = self.target(self.args)
            else:
                results = self.target()
            # results.code = 200 # set to 200 for testing
        except ContinueWarning as w:
            results = SNAPResponse(code=ResponseCode.CONTINUE_WARNING, message=w.model.json())
        except UserCancellation as e:
            results = SNAPResponse(code=ResponseCode.USER_CANCELLATION, message=e.model.json())
        except LiveDataState as e:
            results = SNAPResponse(code=ResponseCode.LIVE_DATA_STATE, message=e.model.json())
        except RecoverableException as e:
            results = SNAPResponse(code=ResponseCode.RECOVERABLE, message=e.model.json())
        except Exception as e:  # noqa: BLE001
            logger.error(e)
            if logger.isEnabledFor(logging.DEBUG):
                # print stacktrace
                import traceback

                traceback.print_exc()

            results = SNAPResponse(code=ResponseCode.ERROR, message=str(e))

        if isinstance(results, SNAPResponse):
            self.result.emit(results)
            self.success.emit(results.code < ResponseCode.MAX_OK)
            self.finished.emit()
        else:
            raise ValueError("Worker target must return a SNAPResponse object.")

    @Slot()
    def requestCancellation(self):
        # Request cancellation at next interruption point.

        # IMPORTANT: this method should generally be executed on the MAIN thread, and NOT
        #   on the actual thread-of-execution that it concerns.
        # Otherwise, it won't be executed until the worker thread can get to it,
        #   and that is usually too late for the user to care anymore.

        if self._thisThread is not None and self._thisThread.isRunning():
            self._thisThread.requestInterruption()


class InfiniteWorker(QObject):
    result = Signal(object)
    finished = Signal()

    target = None
    args = None
    _kill = False

    def __init__(self, target, args=None):
        super().__init__()
        self.target = target
        self.args = args

    @Slot()
    def stop(self):
        self._kill = True

    @Slot()
    def run(self):
        """inf running task."""
        while not self._kill:
            self.result.emit(self.target(self.args))
        self.finished.emit()


@Singleton
class WorkerPool:
    max_threads = 8
    threads: Dict[Worker, QThread] = {}
    worker_queue: List[Worker] = []

    def createWorker(self, target, args):
        return Worker(target=target, args=args)

    def createInfiniteWorker(self, target, args):
        return InfiniteWorker(target=target, args=args)

    def _dequeueWorker(self, worker):
        self.threads.pop(worker)
        if len(self.worker_queue) > 0:
            self.submitWorker(self.worker_queue.pop())

    def submitWorker(self, worker):
        if len(self.threads) >= self.max_threads:
            # add to queue
            self.worker_queue.append(worker)
        else:
            # spawn thread and delegate
            thread = QThread()
            # WARNING: maybe the worker shouldn't be a key, not sure how equivalence is solved.
            self.threads[worker] = thread
            worker.moveToThread(thread)

            # Connect signals and slots
            thread.started.connect(worker.run)

            worker.finished.connect(thread.quit)
            worker.finished.connect(worker.deleteLater)

            thread.finished.connect(thread.deleteLater)
            thread.finished.connect(lambda: self._dequeueWorker(worker))

            # Start the thread
            thread.start()
