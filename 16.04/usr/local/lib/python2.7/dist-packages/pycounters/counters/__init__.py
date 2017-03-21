from collections import deque
from copy import copy
from time import time
from pycounters.base import THREAD_DISPATCHER, BaseListener
from pycounters.counters.base import BaseCounter, AutoDispatch, Timer, TimerMixin, TriggerMixin, AccumulativeCounterValue, AverageCounterValue

__author__ = 'boaz'


class EventCounter(TriggerMixin, BaseCounter):
    """ Counts the number of times an end event has fired.
    """

    def __init__(self, name, events=None):
        self.value = None
        super(EventCounter, self).__init__(name, events=events)

    def _get_value(self):
        return AccumulativeCounterValue(self.value)

    def _report_event_value(self, name, value):

        if self.value:
            self.value += value
        else:
            self.value = long(value)

    def _clear(self):
        self.value = 0L


class TotalCounter(AutoDispatch, BaseCounter):
    """ Counts the total of events' values.
    """

    def __init__(self, name, events=None):
        self.value = None
        super(TotalCounter, self).__init__(name, events=events)

    def _get_value(self):
        return AccumulativeCounterValue(self.value)

    def _report_event_value(self, name, value):

        if self.value:
            self.value += value
        else:
            self.value = long(value)

    def _clear(self):
        self.value = 0L


class AverageWindowCounter(AutoDispatch, BaseCounter):
    """ Calculates a running average of arbitrary values """

    def __init__(self, name, window_size=300.0, events=None):
        super(AverageWindowCounter, self).__init__(name, events=events)
        self.window_size = window_size
        self.values = deque()
        self.times = deque()

    def _clear(self):
        self.values.clear()
        self.times.clear()

    def _get_value(self):
        self._trim_window()
        if not self.values:
            v = None
        else:
            v = sum(self.values, 0.0) / len(self.values)

        return AverageCounterValue(v, len(self.values))

    def _trim_window(self):
        window_limit = self._get_current_time() - self.window_size
        # trim old data
        while self.times and self.times[0] < window_limit:
            self.times.popleft()
            self.values.popleft()

    def _report_event_value(self, param, value):
        self._trim_window()
        self.values.append(value)
        self.times.append(self._get_current_time())

    def _get_current_time(self):
        return time()


class FrequencyCounter(TriggerMixin, AverageWindowCounter):
    """ Counts the frequency of end events in the last five minutes
    """

    def _get_value(self):
        self._trim_window()
        if not self.values or len(self.values) < 1:
            return AccumulativeCounterValue(0.0)
        return AccumulativeCounterValue(sum(self.values, 0.0) / (self._get_current_time() - self.times[0]))


class AverageTimeCounter(TimerMixin, AverageWindowCounter):
    """ Counts the average time between start and end events
    """

    pass


class ValueAccumulator(AutoDispatch, BaseCounter):
    """ Captures all named values it gets and accumulates them. Also allows rethrowing them, prefixed with their name."""

    def __init__(self, name, events=None):
        self.accumulated_values = dict()
        # forces the object not to accumulate values. Used when the object itself is raising events
        self._ignore_values = False

        super(ValueAccumulator, self).__init__(name, events=events)

    def _report_event_value(self, name, value):
        if self._ignore_values:
            return
        cur_value = self.accumulated_values.get(name)
        if cur_value:
            cur_value += value
        else:
            cur_value = value
        self.accumulated_values[name] = cur_value

    def _get_value(self):
        return copy(self.accumulated_values)

    def _clear(self):
        self.accumulated_values.clear()

    def raise_value_events(self, clear=False):
        """ raises accumuated values as value events. """
        with self.lock:
            self._ignore_values = True
            try:
                for k, v in self.accumulated_values.iteritems():
                    THREAD_DISPATCHER.dispatch_event(self.name + "." + k, "value", v)
            finally:
                self._ignore_values = True

            if clear:
                self._clear()


class ThreadTimeCategorizer(BaseListener):
    """ A class to divide the time spent by thread across multiple categories. Categories are mutually exclusive. """

    def __init__(self, name, categories, timer_class=Timer):
        super(ThreadTimeCategorizer, self).__init__()
        self.name = name
        self.category_timers = dict()
        self.timer_stack = list()  # a list of strings of paused timers
        for cat in categories:
            self.category_timers[cat] = timer_class()

    def get_times(self):
        ret = []
        for k, v in self.category_timers.iteritems():
            ret.append((self.name + "." + k, v.get_accumulated_time()))
        return ret

    def report_event(self, name, property, param):
        if property == "start":
            cat_timer = self.category_timers.get(name)
            if not cat_timer:
                return

            if self.timer_stack:
                self.timer_stack[-1].pause()

            cat_timer.start()
            self.timer_stack.append(cat_timer)

        elif property == "end":
            cat_timer = self.category_timers.get(name)  # if all went well it is there...
            if not cat_timer:
                return
            cat_timer.pause()
            self.timer_stack.pop()
            if self.timer_stack:
                self.timer_stack[-1].start()  # continute last paused timer

    def raise_value_events(self, clear=False):
        """ raises category total time as value events. """
        for k, v in self.get_times():
            THREAD_DISPATCHER.dispatch_event(k, "value", v)

        if clear:
            self.category_timers.clear()
