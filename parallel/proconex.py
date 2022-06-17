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
    def __init__(self, queueNum, queueSize):
        self._queues = [Queue.Queue(queueSize) for i in range(queueNum)]
        #self._queue = Queue.Queue(queueSize)
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
            _log.debug("raising notified error: %s %s", self.exc_info[0], self.exc_info[1])
            for filename, linenum, funcname, source in traceback.extract_tb(self.exc_info[2]):
                _log.debug("%-23s:%s '%s' in %s", filename, linenum, source, funcname)

    @property
    def hasFailed(self):
        """``True`` after `fail()` has been called."""
        return not self._failedConsumers.empty()

    @property
    def queues(self):
        """Queue containing items exchanged between producers and consumers."""
        return self._queues

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
        self._log = logging.getLogger("proconex.%s" % name)
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
            for b_id, item in self.items():
                itemHasBeenPut = False
                while not itemHasBeenPut:
                    self.workEnv.possiblyRaiseError()
                    try:
                        self.workEnv.queues[b_id].put(item, True, _HACK_DELAY)
                        itemHasBeenPut = True
                    except Queue.Full:
                        pass
        except Exception as error:
            self.log.warning("cannot continue to produce: %s", error)
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
    def __init__(self, c_id):
        assert c_id is not None

        super(Consumer, self).__init__("customer %d" % c_id)
        self._id = c_id
        self._isFinishing = False
        self.log.debug("waiting for items to consume")

    def finish(self):
        self._isFinishing = True

    def consume(self, item):
        raise NotImplementedError("Consumer.consume()")

    def run(self):
        assert self.workEnv is not None
        try:
            d_id = 0
            while not (self._isFinishing and self.workEnv.queues[self._id].empty()) \
                    and not self._isCanceled:
                # HACK: Use a timeout when getting the item from the queue
                # because between `empty()` and `get()` another consumer might
                # have removed it.
                try:
                    d_id += 1
                    if d_id % 1000 == 0:
                        print('consume id: %s, processed: %s' % (self._id, d_id))
                    item = self.workEnv.queues[self._id].get(timeout=_HACK_DELAY)
                    self.consume(item)
                except Queue.Empty:
                    pass
            if self._isCanceled:
                self.log.debug("canceled")
            if self._isFinishing:
                self.log.debug("finished")
        except Exception as error:
            self.log.warning("cannot continue to consume: %s", error)
            self.log.warning(error.args)
            self.workEnv.fail(self)

            exc_info = sys.exc_info()
            self.log.debug("raising notified error: %s %s", exc_info[0], exc_info[1])
            for filename, linenum, funcname, source in traceback.extract_tb(exc_info[2]):
                _log.debug("%-23s:%s '%s' in %s", filename, linenum, source, funcname)

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

