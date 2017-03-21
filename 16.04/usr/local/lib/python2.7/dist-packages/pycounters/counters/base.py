from exceptions import NotImplementedError
from time import time
from pycounters.base import BaseListener, CounterValueBase
from threading import RLock, local as threading_local


class BaseCounter(BaseListener):

    def __init__(self, name, events=None):
        """
           name - name of counter
           events - events this counter should count. can be
                None - defaults to events called the same as counter name
                [event, event, ..] - a list of events to listen to
        """
        self.name = name
        if events is None:
            events = [name]
        super(BaseCounter, self).__init__(events=events)

        self.lock = RLock()

    def report_event(self, name, property, param):
        """ reports an event to this counter """
        with self.lock:
            self._report_event(name, property, param)

    def get_value(self):
        """
         gets the value of this counter
        """
        with self.lock:
            return self._get_value()

    def clear(self, dump=True):
        """ Clears the stored information
        """
        with self.lock:
            self._clear()

    def _report_event(self, name, property, param):
        """ implement this in sub classes """
        raise NotImplementedError("_report_event is not implemented")

    def _get_value(self):
        """ implement this in sub classes """
        raise NotImplementedError("_get_value is not implemented")

    def _clear(self):
        """ implement this in sub classes """
        raise NotImplementedError("_clear is not implemented")


class AutoDispatch(object):
    """ a mixing to wire up events to functions based on the property parameter. Anything without a match will be
        ignored.
        function signature is:
        def _report_event_PROPERTY(name, param)

    """

    def __init__(self, *args, **kwargs):
        super(AutoDispatch, self).__init__(*args, **kwargs)
        dispatch_dict = dict()
        for k in dir(self):
            if k.startswith("_report_event_"):
                # have a a handler, wire it up
                dispatch_dict[k[len("_report_event_"):]] = getattr(self, k)

        self.dispatch_dict = dispatch_dict

    def _report_event(self, name, property, param):
        handler = self.dispatch_dict.get(property)
        if handler:
            handler(name, param)


class Timer(object):
    """ a thread specific timer. """

    def _get_current_time(self):
        return time()

    def start(self):
        """ start timing """
        self.start_time = self._get_current_time()
        if not hasattr(self, "accumulated_time"):
            self.accumulated_time = 0.0

    def stop(self):
        """ stops the timer returning accumulated time so far. Also clears out the accumaulated time. """
        t = self.pause()
        self.accumulated_time = 0.0
        return t

    def pause(self):
        """ pauses the time returning accumulated time so far """
        ct = self._get_current_time()
        delta = ct - self.start_time
        self.accumulated_time += delta

        return self.accumulated_time

    def get_accumulated_time(self):
        if not hasattr(self, "accumulated_time"):
                self.accumulated_time = 0.0
        return self.accumulated_time


class ThreadLocalTimer(threading_local, Timer):
    pass


class TimerMixin(AutoDispatch):

    def __init__(self, *args, **kwargs):
        self.timer = None
        super(TimerMixin, self).__init__(*args, **kwargs)

    def _report_event_start(self, name, param):
        if not self.timer:
            self.timer = ThreadLocalTimer()

        self.timer.start()

    def _report_event_end(self, name, param):
        self._report_event(name, "value", self.timer.stop())


class TriggerMixin(AutoDispatch):
    """ translates end events to 1-valued events. Effectively counting them.
    """

    def _report_event_end(self, name, param):
        self._report_event(name, "value", 1L)


class AccumulativeCounterValue(CounterValueBase):
    """ Counter values that are added upon merges
    """

    def merge_with(self, other_counter_value):
        """ updates this CounterValue with information of another. Used for multiprocess reporting
        """
        if self.value:
            if other_counter_value.value:
                self.value += other_counter_value.value
        else:
            self.value = other_counter_value.value


class AverageCounterValue(CounterValueBase):
    """ Counter values that are averaged upon merges
    """

    @property
    def value(self):
        if not self._values:
            return None
        sum_of_counts = sum([c for v, c in self._values], 0)
        return sum([v * c for v, c in self._values], 0.0) / sum_of_counts

    def __init__(self, value, agg_count):
        """
            value - the average counter to store
            agg_count - the number of elements that was averaged in value. Important for proper merging.
        """
        self._values = [(value, agg_count)] if value is not None else []

    def merge_with(self, other_counter_value):
        """ updates this CounterValue with information of another. Used for multiprocess reporting
        """
        self._values.extend(other_counter_value._values)


class MaxCounterValue(CounterValueBase):
    """ Counter values that are merged by selecting the maximal value. None values are ignored.
        """

    def merge_with(self, other_counter_value):
        """ updates this CounterValue with information of another. Used for multiprocess reporting
        """
        if self.value is None:
            self.value = other_counter_value.value
            return
        if other_counter_value.value is not None and self.value < other_counter_value.value:
            self.value = other_counter_value.value


class MinCounterValue(CounterValueBase):
    """ Counter values that are merged by selecting the minimal value. None values are ignored.
        """

    def merge_with(self, other_counter_value):
        """ updates this CounterValue with information of another. Used for multiprocess reporting
        """
        if self.value is None:
            self.value = other_counter_value.value
            return
        if other_counter_value.value is not None and self.value > other_counter_value.value:
            self.value = other_counter_value.value
