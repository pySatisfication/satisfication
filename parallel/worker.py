import logging

from .proconex import (
    WorkEnv,
    Consumer,
    Producer
)

_log = logging.getLogger("worker")
_HACK_DELAY = 0.02

class _BaseWorker(object):
    """
    Base for controllers for interaction between producers and consumers.
    """
    def __init__(self, producers, consumers, queueSize=None):
        assert producers is not None
        assert consumers is not None

        # TODO: Consider using tuples instead of lists for producers and
        # consumers.
        if isinstance(producers, Producer):
            self._producers = [producers]
        else:
            self._producers = list(producers)
            
        if isinstance(consumers, Consumer):
            self._consumers = [consumers]
        else:
            self._consumers = list(consumers)

        if queueSize is None:
            actualQueueSize = 2 * len(self._consumers)
        else:
            actualQueueSize = queueSize
        self._workEnv = WorkEnv(len(self._consumers), actualQueueSize)

    def _cancelThreads(self, name, threadsToCancel):
        assert name
        if threadsToCancel is not None:
            _log.debug("canceling all %s", name)
            for threadToCancel in threadsToCancel:
                threadToCancel.cancel()
            _log.debug("waiting for %s to be canceled", name)
            for possiblyCanceledThread in threadsToCancel:
                # In this case, we ignore possible errors because there
                # already is an exc_info to report.
                possiblyCanceledThread.join(_HACK_DELAY)
                if possiblyCanceledThread.isAlive():
                    threadsToCancel.append(possiblyCanceledThread)

    def _cancelAllConsumersAndProducers(self):
        self._cancelThreads("consumers", self._consumers)
        self._consumers = None
        self._cancelThreads("producers", self._producers)
        self._producers = None

    def close(self):
        """
        Close the whole production and consumption, releasing all resources
        and stopping all threads (in case there are still any running).

        The simplest way to call this is by wrapping the worker in a ``with``
        statement.
        """
        self._cancelAllConsumersAndProducers()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class Worker(_BaseWorker):
    """
    Controller for interaction between producers and consumers.
    """
    def work(self):
        """
        Launch consumer threads and produce items. In case any consumer or the
        producer raise an exception, fail by raising this exception.
        """
        assert self._consumers is not None, "work() must be called only once"

        _log.debug("starting producers")
        for producerToStart in self._producers:
            producerToStart.workEnv = self._workEnv
            producerToStart.start()
        _log.debug("starting consumers")
        for consumerToStart in self._consumers:
            consumerToStart.workEnv = self._workEnv
            consumerToStart.start()

        _log.debug("waiting for producers to finish")
        for possiblyFinishedProducer in self._producers:
            self._workEnv.possiblyRaiseError()
            while possiblyFinishedProducer.isAlive():
                possiblyFinishedProducer.join(_HACK_DELAY)
                self._workEnv.possiblyRaiseError()
        self._producers = None

        _log.debug("telling consumers to finish the remaining items")
        for consumerToFinish in self._consumers:
            consumerToFinish.finish()
        _log.debug("waiting for consumers to finish")
        for possiblyFinishedConsumer in self._consumers:
            self._workEnv.possiblyRaiseError()
            possiblyFinishedConsumer.join(_HACK_DELAY)
            if possiblyFinishedConsumer.isAlive():
                self._consumers.append(possiblyFinishedConsumer)
        self._consumers = None

    def close(self):
        """
        Close the whole production and consumption, releasing all resources
        and stopping all threads (in case there are still any running).

        The simplest way to call this is by wrapping the worker in a ``with``
        statement.
        """
        self._cancelAllConsumersAndProducers()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

class Converter(_BaseWorker):
    """
    A producer/consumer where consumers convert the producer's item and make
    them available to the caller using `items()`.
    """
    def __init__(self, producers, convertingConsumers, queueSize=None):
        assert producers is not None
        assert convertingConsumers is not None
        super(Converter, self).__init__(producers, convertingConsumers, queueSize)
        self._itemQueue = Queue.Queue()

    def _aliveCount(self, threads):
        assert threads is not None
        result = 0
        for thread in threads:
            if thread.isAlive():
                result += 1
        return result

    def items(self):
        """
        The items generated by the consumers.
        """
        assert self._consumers is not None, "items() must be called only once"

        _log.debug("starting producers")
        for producerToStart in self._producers:
            producerToStart.workEnv = self._workEnv
            producerToStart.start()
        _log.debug("starting consumers")
        for consumerToStart in self._consumers:
            consumerToStart.workEnv = self._workEnv
            consumerToStart.itemQueue = self._itemQueue
            consumerToStart.start()

        _log.debug("converting items")

        producersAreFinished = False
        consumersAreFinished = False
        # TODO: Replace ugly thread counting by some kind of notification.
        while not (producersAreFinished and consumersAreFinished and self._itemQueue.empty()):
            self._workEnv.possiblyRaiseError()
            if producersAreFinished:
                if self._aliveCount(self._consumers) == 0:
                    _log.debug("consumers are finished")
                    consumersAreFinished = True
            elif self._aliveCount(self._producers) == 0:
                _log.debug("producers are finished, telling consumers to finish the remaining items")
                for consumerToFinish in self._consumers:
                    consumerToFinish.finish()
                producersAreFinished = True
            try:
                item = self._itemQueue.get(timeout=_HACK_DELAY)
                yield item
            except Queue.Empty:
                pass

        self._producers = None
        self._consumers = None

if __name__ == "__main__":
    import doctest
    doctest.testmod()
