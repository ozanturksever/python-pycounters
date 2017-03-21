from functools import wraps
import base
import counters


def count(name=None, auto_add_counter=counters.EventCounter):
    """
        A shortcut decorator to count the number times a function is called. Uses the :obj:`counters.EventCounter` counter by default.
        If the parameter name is not supplied events are reported under the name of the wrapped function.
    """
    return _reporting_decorator_context_manager(name, auto_add_counter=auto_add_counter)


def value(name, value, auto_add_counter=counters.AverageWindowCounter):
    """
      A shortcut function to report a value of something. Uses the :obj:`counters.AverageWindowCounter` counter by default.
    """
    if auto_add_counter:
        cntr = base.GLOBAL_REGISTRY.get_counter(name, throw=False)
        if not cntr:
            base.GLOBAL_REGISTRY.add_counter(auto_add_counter(name), throw=False)

    base.THREAD_DISPATCHER.dispatch_event(name, "value", value)


def occurrence(name, auto_add_counter=counters.FrequencyCounter):
    """
      A shortcut function reports an occurrence of something. Uses the :obj:`counters.FrequencyCounter` counter by default.
    """
    if auto_add_counter:
        cntr = base.GLOBAL_REGISTRY.get_counter(name, throw=False)
        if not cntr:
            base.GLOBAL_REGISTRY.add_counter(auto_add_counter(name), throw=False)

    base.THREAD_DISPATCHER.dispatch_event(name, "end", None)


def frequency(name=None, auto_add_counter=counters.FrequencyCounter):
    """
        A shortcut decorator to count the frequency in which a function is called. Uses the :obj:`counters.FrequencyCounter` counter by default.
        If the parameter name is not supplied events are reported under the name of the wrapped function.
    """
    return _reporting_decorator_context_manager(name, auto_add_counter=auto_add_counter)


def time(name=None, auto_add_counter=counters.AverageTimeCounter):
    """
        A shortcut decorator to count the average execution time of a function. Uses the :obj:`counters.AverageTimeCounter` counter by default.
        If the parameter name is not supplied events are reported under the name of the wrapped function.
    """
    return _reporting_decorator_context_manager(name, auto_add_counter=auto_add_counter)


class _reporting_decorator_context_manager(object):

    def __init__(self, name, auto_add_counter=None):
        self.name = name
        self.auto_add_counter = auto_add_counter
        if auto_add_counter and name:
            # we have a name, we can register things now. O.w. this must be used as a decorator.
            # name will be registered then and there.
            cntr = base.GLOBAL_REGISTRY.get_counter(name, throw=False)
            if not cntr:
                base.GLOBAL_REGISTRY.add_counter(auto_add_counter(name), throw=True)

    def __call__(self, f):
        event_name = self.name
        if not self.name:
            event_name = f.__name__
            # we don't have stored name... counter needs to be registered.
            if self.auto_add_counter:
                cntr = base.GLOBAL_REGISTRY.get_counter(event_name, throw=False)
                if not cntr:
                    base.GLOBAL_REGISTRY.add_counter(self.auto_add_counter(event_name), throw=True)

        @wraps(f)
        def wrapper(*args, **kwargs):

            base.THREAD_DISPATCHER.dispatch_event(event_name, "start", None)
            try:
                r = f(*args, **kwargs)
            finally:
                ## make sure calls are balanced
                base.THREAD_DISPATCHER.dispatch_event(event_name, "end", None)
            return r

        return wrapper

    def __enter__(self):
        if not self.name:
            raise Exception("PyCounters context manager used without defining a name.")
        base.THREAD_DISPATCHER.dispatch_event(self.name, "start", None)

    def __exit__(self, *args, **kwargs):
        base.THREAD_DISPATCHER.dispatch_event(self.name, "end", None)
