import fcntl
import json
import os
from .base import  BaseReporter


__author__ = 'boaz'


class LogReporter(BaseReporter):
    """ Log based reporter.
    """

    def __init__(self, output_log=None):
        """ output will be logged to output_log
            :param output_log: a python log object to output reports to.
        """
        super(LogReporter, self).__init__()
        self.logger = output_log

    def output_values(self, counter_values):
        logs = sorted(counter_values.iteritems(), cmp=lambda a, b: cmp(a[0], b[0]))

        for k, v in logs:
            if not (k.startswith("__") and k.endswith("__")):   # don't output __node_reports__ etc.
                self.logger.info("%s %s", k, v)


class JSONFileReporter(BaseReporter):
    """
        Reports to a file in a JSON format.

    """

    def __init__(self, output_file=None):
        """
            :param output_file: a file name to which the reports will be written.
        """
        super(JSONFileReporter, self).__init__()
        self.output_file = output_file
        ## try to open the file now, just to see if it is possible and raise an exception if not
        self.output_values({"__initializing__": True})

    def output_values(self, counter_values):
        JSONFileReporter.safe_write(counter_values, self.output_file)

    @staticmethod
    def _lockfile(file):
        try:
            fcntl.flock(file, fcntl.LOCK_EX)
            return True
        except IOError, exc_value:
        #  IOError: [Errno 11] Resource temporarily unavailable
            if exc_value[0] == 11 or exc_value[0] == 35:
                return False
            else:
                raise

    @staticmethod
    def _unlockfile(file):
        fcntl.flock(file, fcntl.LOCK_UN)

    @staticmethod
    def safe_write(value, filename):
        """ safely writes value in a JSON format to file
        """
        fd = os.open(filename, os.O_CREAT | os.O_WRONLY)
        JSONFileReporter._lockfile(fd)
        try:

            file = os.fdopen(fd, "w")
            file.truncate()
            json.dump(value, file)
        finally:
            JSONFileReporter._unlockfile(fd)
            file.close()
        # fd is now close by the with clause

    @staticmethod
    def safe_read(filename):
        """ safely reads a value in a JSON format frome file
        """
        fd = os.open(filename, os.O_RDONLY)
        JSONFileReporter._lockfile(fd)
        try:
            file = os.fdopen(fd, "r")
            return json.load(file)
        finally:
            JSONFileReporter._unlockfile(fd)
            file.close()

        # fd is now close by the with clause
