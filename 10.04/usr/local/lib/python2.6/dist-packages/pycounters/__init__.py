"""

PyCounters is a light weight library to monitor performance in production system.
It is meant to be used in scenarios where using a profile is unrealistic due to the overhead it requires.
Use PyCounters to get high level and concise overview of what's going on in your production code.

See #### (read the docs) for more information

"""
import logging
from pycounters.reporters.base import CollectingRole
from shortcuts import _reporting_decorator_context_manager
from . import reporters, base


def report_start(name):
    """ reports an event's start.
        NOTE: you *must*  fire off a corresponding event end with report_end
    """
    base.THREAD_DISPATCHER.dispatch_event(name, "start", None)


def report_end(name):
    """ reports an event's end.
        NOTE: you *must* have fired off a corresponding event end with report_start
    """
    base.THREAD_DISPATCHER.dispatch_event(name, "end", None)


def report_start_end(name=None):
    """
     returns a function decorator and/or context manager which raises start and end events.
     If name is None events name is set to the name of the decorated function. In that case report_start_end
     can not be used as a context manager.
    """
    return _reporting_decorator_context_manager(name)


def report_value(name, value):
    """
     reports a value event to the counters.
    """

    base.THREAD_DISPATCHER.dispatch_event(name, "value", value)


def register_counter(counter, throw_if_exists=True):
    """ Register a counter with PyCounters
    """
    base.GLOBAL_REGISTRY.add_counter(counter, throw=throw_if_exists)


def unregister_counter(counter=None, name=None):
    """ Removes a previously registered counter
    """
    base.GLOBAL_REGISTRY.remove_counter(counter=counter, name=name)


def output_report():
    """
      Manually cause the current values all registered counters to be reported.
    """
    reporters.base.GLOBAL_REPORTING_CONTROLLER.report()


def start_auto_reporting(seconds=300):
    """
    Start reporting in a background thread. Reporting frequency is set by seconds param.
    """
    reporters.base.GLOBAL_REPORTING_CONTROLLER.start_auto_report(seconds=seconds)


def stop_auto_reporting():
    """ Stop auto reporting """
    reporters.base.GLOBAL_REPORTING_CONTROLLER.stop_auto_report()


def register_reporter(reporter=None):
    """
        add a reporter to PyCounters. Registered reporters will output collected metrics
    """
    reporters.base.GLOBAL_REPORTING_CONTROLLER.register_reporter(reporter)


def unregister_reporter(reporter=None):
    """
        remove a reporter from PyCounters.
    """
    reporters.base.GLOBAL_REPORTING_CONTROLLER.unregister_reporter(reporter)


def configure_multi_process_collection(collecting_address=[("", 60907), ("", 60906)], timeout_in_sec=120,
                                       role=CollectingRole.AUTO_ROLE):
    """
        configures PyCounters to collect values from multiple processes

        :param collecting_address: a list of (address,port) tuples address of machines and ports data should be collected on.
            the extra tuples are used as backup in case the first address/port combination is (temporarily)
            unavailable. PyCounters would automatically start using the preferred address/port when it becomes
            available again. This behavior is handy when restarting the program and the old port is not yet
            freed by the OS.
        :param timeout_in_sec: timeout configuration for connections. Default should be good enough for pratically
            everyone.

        :param role: the role of this process. Leave at the default of AUTO_ROLE for pycounters to automatically choose
            a collecting leader.

    """

    reporters.base.GLOBAL_REPORTING_CONTROLLER.configure_multi_process(collecting_address=collecting_address,
        timeout_in_sec=timeout_in_sec, debug_log=logging.getLogger(name="pycounters_multi_proc"), role=role)
