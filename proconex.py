# Copyright (C) 2012 Thomas Aglassinger
#
# This program is free software: you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at your
# option) any later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
from __future__ import with_statement

import logging
#import Queue
import sys
import threading
import traceback

if sys.version > '3':
    import queue as Queue
else:
    import Queue

__version__ = "0.4"

# Delay in seconds for ugly polling hacks.
_HACK_DELAY = 0.02

_log = logging.getLogger("proconex")


class WorkEnv(object):
    """
    Environment in which production and consumption takes place and
    information about a possible exc_info is stored.
    """
    def __init__(self, queueSize):
        self._queue = Queue.Queue(queueSize)
        self._exc_info = None
        self._failedConsumers = Queue.Queue()

    def fail(self, failedConsumer):
        """
        Inform environment that ``consumer`` failed because of ``exc_info``.
        """
        assert failedConsumer is not None
        self._exc_info = sys.exc_info()
        self._failedConsumers.put(failedConsumer)

    def possiblyRaiseError(self):
        """"Provided that `hasFailed` raise `exc_info`, otherwise do nothing."""
        if self.hasFailed:
            _log.debug(u"raising notified error: %s %s", self.exc_info[0], self.exc_info[1])
            for filename, linenum, funcname, source in traceback.extract_tb(self.exc_info[2]):
                _log.debug("%-23s:%s '%s' in %s", filename, linenum, source, funcname)

    @property
    def hasFailed(self):
        """``True`` after `fail()` has been called."""
        return not self._failedConsumers.empty()

    @property
    def queue(self):
        """Queue containing items exchanged between producers and consumers."""
        return self._queue

    @property
    def exc_info(self):
        """First exc_info that prevents work from continuing."""
        return self._exc_info


class _CancelableThread(threading.Thread):
    """
    Thread that can be canceled using `cancel()`.
    """
    def __init__(self, name):
        assert name is not None

        super(_CancelableThread, self).__init__(name=name)
        self._log = logging.getLogger(u"proconex.%s" % name)
        self._isCanceled = False
        self.workEnv = None

    def cancel(self):
        self._isCanceled = True

    @property
    def isCanceled(self):
        return self._isCanceled

    @property
    def log(self):
        return self._log


class Producer(_CancelableThread):
    """
    Producer putting items on a `WorkEnv`'s queue.
    """
    def __init__(self, name="producer"):
        assert name is not None

        super(Producer, self).__init__(name)

    def items(self):
        """
        A sequence of items to produce. Typically this is implemented as
        generator.
        """
        raise NotImplementedError()

    def run(self):
        """
        Process `items()`. Normally there is no need to modify this procedure.
        """
        assert self.workEnv is not None
        try:
            for item in self.items():
                itemHasBeenPut = False
                while not itemHasBeenPut:
                    self.workEnv.possiblyRaiseError()
                    try:
                        self.workEnv.queue.put(item, True, _HACK_DELAY)
                        itemHasBeenPut = True
                    except Queue.Full:
                        pass
        except Exception as error:
            self.log.warning(u"cannot continue to produce: %s", error)
            self.workEnv.fail(self)


class Consumer(_CancelableThread):
    """
    Thread to consume items from an item queue filled by a producer, which can
    be told to terminate in two ways:

    1. using `finish()`, which keeps processing the remaining items on the
       queue until it is empty
    2. using `cancel()`, which finishes consuming the current item and then
       terminates
    """
    def __init__(self, name="consumer"):
        assert name is not None

        super(Consumer, self).__init__(name)
        self._isFinishing = False
        self.log.debug(u"waiting for items to consume")

    def finish(self):
        self._isFinishing = True

    def consume(self, item):
        raise NotImplementedError("Consumer.consume()")

    def run(self):
        assert self.workEnv is not None
        try:
            while not (self._isFinishing and self.workEnv.queue.empty()) \
                    and not self._isCanceled:
                # HACK: Use a timeout when getting the item from the queue
                # because between `empty()` and `get()` another consumer might
                # have removed it.
                try:
                    item = self.workEnv.queue.get(timeout=_HACK_DELAY)
                    self.consume(item)
                except Queue.Empty:
                    pass
            if self._isCanceled:
                self.log.debug(u"canceled")
            if self._isFinishing:
                self.log.debug(u"finished")
        except Exception as error:
            self.log.warning(u"cannot continue to consume: %s", error)
            self.workEnv.fail(self)


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
        self._workEnv = WorkEnv(actualQueueSize)

    def _cancelThreads(self, name, threadsToCancel):
        assert name
        if threadsToCancel is not None:
            _log.debug(u"canceling all %s", name)
            for threadToCancel in threadsToCancel:
                threadToCancel.cancel()
            _log.debug(u"waiting for %s to be canceled", name)
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

        _log.debug(u"starting producers")
        for producerToStart in self._producers:
            producerToStart.workEnv = self._workEnv
            producerToStart.start()
        _log.debug(u"starting consumers")
        for consumerToStart in self._consumers:
            consumerToStart.workEnv = self._workEnv
            consumerToStart.start()

        _log.debug(u"waiting for producers to finish")
        for possiblyFinishedProducer in self._producers:
            self._workEnv.possiblyRaiseError()
            while possiblyFinishedProducer.isAlive():
                possiblyFinishedProducer.join(_HACK_DELAY)
                self._workEnv.possiblyRaiseError()
        self._producers = None

        _log.debug(u"telling consumers to finish the remaining items")
        for consumerToFinish in self._consumers:
            consumerToFinish.finish()
        _log.debug(u"waiting for consumers to finish")
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


class ConvertingConsumer(Consumer):
    def __init__(self, name):
        assert name is not None

        super(ConvertingConsumer, self).__init__(name)
        self.itemQueue = None

    def addItem(self, item):
        """
        Add ``item`` to the `Converter`'s item queue thus making it available
        to the caller. The `consume()` method must call this for each
        converted item.
        """
        assert self.itemQueue is not None, "Converter.items() must be called before addItems()"
        self.itemQueue.put(item)


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

        _log.debug(u"starting producers")
        for producerToStart in self._producers:
            producerToStart.workEnv = self._workEnv
            producerToStart.start()
        _log.debug(u"starting consumers")
        for consumerToStart in self._consumers:
            consumerToStart.workEnv = self._workEnv
            consumerToStart.itemQueue = self._itemQueue
            consumerToStart.start()

        _log.debug(u"converting items")

        producersAreFinished = False
        consumersAreFinished = False
        # TODO: Replace ugly thread counting by some kind of notification.
        while not (producersAreFinished and consumersAreFinished and self._itemQueue.empty()):
            self._workEnv.possiblyRaiseError()
            if producersAreFinished:
                if self._aliveCount(self._consumers) == 0:
                    _log.debug(u"consumers are finished")
                    consumersAreFinished = True
            elif self._aliveCount(self._producers) == 0:
                _log.debug(u"producers are finished, telling consumers to finish the remaining items")
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
