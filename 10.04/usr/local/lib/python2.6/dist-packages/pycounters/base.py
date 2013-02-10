from exceptions import NotImplementedError, Exception
import logging
from threading import RLock, local as thread_local
import re


class CounterRegistry(object):

    def __init__(self, dispatcher):
        super(CounterRegistry, self).__init__()
        self.lock = RLock()
        self.dispatcher = dispatcher
        self.registry = dict()

    def get_values(self):
        values = CounterValueCollection()
        with self.lock:
            for name, c in self.registry.iteritems():
                values[name] = c.get_value()

        return values

    def add_counter(self, counter, throw=True):
        with self.lock:
            if counter.name in self.registry:
                if throw:
                    raise Exception("A counter named %s is already defined" % (counter.name))
                return False

            self.registry[counter.name] = counter
            self.dispatcher.add_listener(counter)
            return True

    def remove_counter(self, counter=None, name=None):
        with self.lock:
            if counter:
                name = counter.name

            if not name:
                raise Exception("trying to remove a counter from perfomance registry but no counter or name supplied.")

            self.dispatcher.remove_listener(counter)
            self.registry.pop(name)

    def get_counter(self, name, throw=True):

        with self.lock:
            c = self.registry.get(name)

            if not c and throw:
                raise Exception("No counter named '%s' found " % (name, ))

            return c


class BaseListener(object):

    def __init__(self, events=None):
        """
            events - a list of events name to listen to. Use None for all.
        """
        self.events = events

    def report_event(self, name, property, param):
        """ reports an event to this listener """
        raise NotImplementedError("report_event is not implemented")


class EventLogger(BaseListener):

    def __init__(self, logger, name_filter=None, property_filter=None, logging_level=logging.DEBUG):
        super(EventLogger, self).__init__()
        self.logger = logger
        self.logging_level = logging_level
        self.name_filter = None
        if name_filter:
            if isinstance(name_filter, basestring):
                self.name_filter = re.compile(name_filter)
            else:
                self.name_filter = name_filter

        if property_filter:
            if isinstance(property_filter, basestring):
                self.property_filter = re.compile(property_filter)
            else:
                self.property_filter = property_filter

    def report_event(self, name, property, param):
        if self.name_filter and not self.name_filter.match(name):
            return
        if self.property_filter and not self.property_filter.match(property):
            return
        self.logger.log(self.logging_level, "Event: name=%s property=%s param=%s", name, property, param)


class RegistryListener(BaseListener):

    def __init__(self, registry):
        """ Registry = CounterRegistry to dispatch events to """
        self.registry = registry

    def report_event(self, name, property, param):
        c = self.registry.get_counter(name, throw=False)
        if c:
            c.report_event(name, property, param)


class EventDispatcher(object):

    def __init__(self):
        self.listeners = dict()
        self.listeners[None] = set()
        self.lock = RLock()

    def dispatch_event(self, name, property, param):
        with self.lock:

            ## dispatch a all registraar os None
            for l in self.listeners.get(None, []):
                l.report_event(name, property, param)

            for l in self.listeners.get(name, []):
                l.report_event(name, property, param)

    def add_listener(self, listener):
        with self.lock:
            if listener.events is None:
                self.listeners[None].add(listener)
            else:
                for event in listener.events:
                    s = self.listeners.get(event)
                    if s is None:
                        s = set()
                        self.listeners[event] = s
                    s.add(listener)

    def remove_listener(self, listener):
        with self.lock:
            if listener.events is None:
                self.listeners[None].remove(listener)
            else:
                for event in listener.events:
                    s = self.listeners.get(event)
                    s.remove(listener)


class ThreadSpecificDispatcher(thread_local):
    """ A dispatcher handle thread specific dispatching. Also percolates to Global event"""
    ## TODO: work in progress. no clean solution yet.

    def _get_listner_set(self):
        if not hasattr(self, "listeners"):
            self.listeners = set()  # new thread

        return self.listeners

    def add_listener(self, listener):
        self._get_listner_set().add(listener)

    def remove_listener(self, listener):
        self._get_listner_set().remove(listener)

    def dispatch_event(self, name, property, param):
        # first event specific
        ls = self._get_listner_set()
        if ls:
            for l in ls:
                l.report_event(name, property, param)

        # finally dispatch it globally..
        global GLOBAL_DISPATCHER
        GLOBAL_DISPATCHER.dispatch_event(name, property, param)


GLOBAL_DISPATCHER = EventDispatcher()
GLOBAL_REGISTRY = CounterRegistry(GLOBAL_DISPATCHER)

THREAD_DISPATCHER = ThreadSpecificDispatcher()


class CounterValueBase(object):
    """ a base class for counter values. Deals with defining merge semantics etc.
    """

    def __init__(self, value):
        self.value = value

    def merge_with(self, other_counter_value):
        """ updates this CounterValue with information of another. Used for multiprocess reporting
        """
        raise NotImplementedError("merge_with should be implemented in class inheriting from CounterValueBase")


class CounterValueCollection(dict):
    """ a dictionary of counter values, adding support for dictionary merges and getting a value only dict.
    """

    @property
    def values(self):
        r = {}
        for k, v in self.iteritems():
            r[k] = v.value if hasattr(v, "value") else v

        return r

    def merge_with(self, other_counter_value_collection):
        for k, v in other_counter_value_collection.iteritems():
            mv = self.get(k)
            if mv is None:
                # nothing local, just set it
                self[k] = v
            elif isinstance(mv, CounterValueBase):
                if not isinstance(v, CounterValueBase):
                    raise Exception("Can't merge with CounterValueCollection. Other Collection doesn't have a mergeable value for key %s" % (k, ))
                mv.merge_with(v)
            else:
                raise Exception("Can't merge with CounterValueCollection. Local key $s doesn't have a mergeable value." % (k, ))
