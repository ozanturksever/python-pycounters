from .. import report_start_end
import sys


def _get_existing_wrapped_events(method):
    """ returns the current wrappers around methods
    """

    ret = []
    if hasattr(method, "__get_pycounters_event_names__"):
        ret = method.__get_pycounters_event_names__()
    return ret


def _make_existing_wrapped_events_reporter(event_name, wrapped_method):
    """ creates a function to report event_name when _get_existing_wrapped_events needs it
    """

    def report():
        r = _get_existing_wrapped_events(wrapped_method)
        r.append(event_name)
        return r

    return report


def _create_wrapped_method(event_name, method_to_wrap):
    wrapped = report_start_end(event_name)(method_to_wrap)
    wrapped.__get_pycounters_event_names__ = _make_existing_wrapped_events_reporter(event_name, method_to_wrap)

    return wrapped


def add_start_end_reporting(event_name, klass, method_name):
    """
      wraps method of class klass to generate start and ends events around  method_name.
    """

    current_method = getattr(klass, method_name)
    if not callable(current_method):
        raise Exception("Attribute found is not callable. Looked for '%s' on %s" % (method_name, klass))

    if event_name in _get_existing_wrapped_events(current_method):
        return  # method is already firing this event. Job is done.

    wrapped_method = _create_wrapped_method(event_name, current_method)
    setattr(klass, method_name, wrapped_method)


def execute_patching_scheme(scheme):
    """
       executes a list of patching commands. schem is of the folloowing structure:
         [
           {
             "class":"fully_qualifed_name_of_class_to_patch",
             "method": "name_of_method_of_class",
             "event": "event_name")
            },
           ...
         ]
    """

    for row in scheme:
        class_name = row["class"]
        method_name = row["method"]
        event_name = row["event"]

        mod_name = class_name.rsplit(".", 1)
        if len(mod_name) < 2 or mod_name[0].startswith("."):
            raise Exception("Patching scheme must contain fully qualified absolute class names. Got '%s'." %
                            (class_name, ))
        mod_name, class_name = mod_name

        __import__(mod_name)
        module = sys.modules[mod_name]

        if not hasattr(module, class_name):
            raise Exception("Can't find class '%s' in module '%s" % (class_name, mod_name))

        klass = getattr(module, class_name)
        add_start_end_reporting(event_name, klass, method_name)
