from SocketServer import BaseRequestHandler, TCPServer
from itertools import repeat
import threading
import multiprocessing
import pickle
import socket
import time
import traceback
import itertools


class _noplogger(object):
    """ a fake logger that does nothing
    """

    def debug(self, *args, **kwargs):
        pass

    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def exception(self, *args, **kwargs):
        pass

    def critical(self, *args, **kwargs):
        pass


class ExplicitRequestClosingTCPServer(TCPServer):
    """ A tcp server that doesn't automatically shutdown incoming requests
    """

    def process_request(self, request, client_address):
        """Call finish_request.

            Different from parent by that that it doesn't shutdown the request
        """
        self.finish_request(request, client_address)


class CollectingNodeProxy(BaseRequestHandler):
    """ a proxy to the CollectingNode. Used by collecting leader to get info from collection Node.
    """

    # Default buffer sizes for rfile, wfile.
    # We default rfile to buffered because otherwise it could be
    # really slow for large data (a getc() call per byte); we make
    # wfile unbuffered because (a) often after a write() we want to
    # read and we need to flush the line; (b) big writes to unbuffered
    # files are typically optimized by stdio even when big reads
    # aren't.
    rbufsize = 0
    wbufsize = 0

    def __init__(self, leader, request, client_address, server, debug_log=None):
        self.leader = leader
        self.debug_log = debug_log if debug_log else _noplogger()
        BaseRequestHandler.__init__(self, request, client_address, server)

    ### BaseRequestHandler functions
    def setup(self):
        self.connection = self.request
        self.rfile = self.connection.makefile('rb', self.rbufsize)
        self.wfile = self.connection.makefile('wb', self.wbufsize)

    def handle(self):
        try:
            self.debug_log.debug("Handling an incoming registration request. Asking for node name.")
            node_id = self.receive()
            if node_id == "ping":
                self.debug_log.info("Got pinged.Acknowelding.")
                self.send("ack")
                self.close()
                return

            self.debug_log.info("Connected to node %s", node_id)
            self.id = node_id
            self.debug_log.debug("Registering node %s with proxy.", node_id)
            self.leader.register_node_proxy(self)
            self.debug_log.debug("Sending ack back to node %s.", node_id)
            self.send("ack")
            self.debug_log.debug("Done handling request from node %s.", node_id)
        except Exception as e:
            st = traceback.format_exc()
            self.debug_log.exception("Got an exception while dealing with an incoming request: %s, st:", e, st)
            self.close()
            raise

    def finish(self):
        pass

    ### Collecting Proxy functions
    def send(self, data):
        pickle.dump(data, self.wfile, pickle.HIGHEST_PROTOCOL)
        self.wfile.flush()

    def receive(self):
        return pickle.load(self.rfile)

    def send_and_receive(self, data):
        self.send(data)
        return self.receive()

    def close(self):
        if not self.wfile.closed:
            self.wfile.flush()
        self.wfile.close()
        self.rfile.close()
        self.connection.close()


def normalize_hosts_and_ports(hosts_and_ports):
    """ normalize the various options of hosts_and_ports int a list of
        host and port tuples
    """
    if isinstance(hosts_and_ports[0], list) or isinstance(hosts_and_ports[0], tuple):
        # already in right format:
        return hosts_and_ports

    # a single tuple:
    return [hosts_and_ports]


class CollectingLeader(object):
    """
        A class which sets up a socket server for collecting reports from CollectingNodes
    """

    def __init__(self, hosts_and_ports=[("", 60709), ("", 60708)], debug_log=None):
        self.hosts_and_ports = normalize_hosts_and_ports(hosts_and_ports)
        self.debug_log = debug_log if debug_log else _noplogger()
        self.lock = threading.RLock()
        self.node_proxies = dict()
        self.tcp_server = None
        self.leading_level = None  # if leading this is set to the sequential number of the chosen host and port

    @property
    def leading(self):
        return self.leading_level is not None

    def try_to_lead(self, throw=False):
        """ Iterates of host_and_ports trying to claim leadership position.
           Returns none on success or the last error message on failure

        """
        for potential_level in range(len(self.hosts_and_ports)):
            try:
                self.tcp_server = ExplicitRequestClosingTCPServer(self.hosts_and_ports[potential_level],
                    self.make_stream_request_handler, bind_and_activate=False)
                #                self.allow_reuse_address = True
                try:
                    self.tcp_server.server_bind()
                    self.tcp_server.server_activate()
                except:
                    self.tcp_server.server_close()
                    raise

                # success!
                self.leading_level = potential_level
                break

            except IOError as e:
                self.debug_log.info("Failed to setup TCP Server %s . Error: %s", self.hosts_and_ports[potential_level],
                    e)
                if potential_level == len(self.hosts_and_ports) - 1:
                    # nothing to do, give up.
                    if throw:
                        raise
                    return str(e)

        self.debug_log.info("Successfully gained leader ship on %s (level: %s). Start responding to nodes",
            self.hosts_and_ports[self.leading_level], self.leading_level)

        def target():
            try:
                self.debug_log.debug('serving thread is running')
                self.tcp_server.serve_forever()
                self.debug_log.debug('serving thread stoppinng')
            except Exception as e:
                self.debug_log.exception("Server had an error: %s", e)

        t = threading.Thread(target=target)
        t.daemon = True
        t.start()

        return None

    def disconnect_nodes(self):
        self._send_termination_msg("quit")

    def reconnect_nodes(self):
        self._send_termination_msg("reconnect")

    def _send_termination_msg(self, msg):
        with self.lock:
            for node in self.node_proxies.itervalues():
                try:
                    node.send(msg)
                except IOError as e:
                    self.debug_log.warning("Get an error when sending to node %s:\nerror:%s, \nmsg:%s", node.id, e, msg)

                try:
                    node.close()
                except:
                    pass

            self.node_proxies.clear()

    def stop_leading(self):
        if self.leading:
            self.tcp_server.shutdown()
            self.tcp_server.server_close()
        with self.lock:
            for node in self.node_proxies.itervalues():
                self.debug_log.debug("Closing proxy for %s", node.id)
                node.close()

            self.node_proxies = {}

    def send_to_all_nodes(self, data):
        with self.lock:
            error_nodes = []
            for node in self.node_proxies.itervalues():
                try:
                    node.send(data)
                except IOError as e:
                    self.debug_log.warning("Get an error when sending to node %s:\nerror:%s, \ndata:%s", node.id, e,
                        data)
                    try:
                        node.close()
                    except:
                        pass
                    error_nodes.append(node.id)

            for err_node in error_nodes:
                self.debug_log.debug("Removing node %s from collection", err_node)
                del self.node_proxies[err_node]

    def collect_from_all_nodes(self):
        """ returns a dictionary with answers from all nodes. Dictionary id is node id.
        """
        ret = {}
        with self.lock:
            error_nodes = []
            for node in self.node_proxies.itervalues():
                try:
                    ret[node.id] = node.send_and_receive("collect")
                except IOError as e:
                    self.debug_log.warning("Get an error when sending to node %s:\nerror:%s", node.id, e)
                    node.close()
                    error_nodes.append(node.id)

            for err_node in error_nodes:
                self.debug_log.debug("Removing node %s from collection", err_node)
                del self.node_proxies[err_node]

        return ret

    def log(self, *args, **kwargs):
        if self.debug_log:
            self.debug_log.debug(*args, **kwargs)

    def register_node_proxy(self, proxy):
        with self.lock:
            self.node_proxies[proxy.id] = proxy

    def make_stream_request_handler(self, request, client_address, server):
        """ Creates a CollectingNodeProxy
        """
        return CollectingNodeProxy(self, request, client_address, server, debug_log=self.debug_log)

_GLOBAL_COUNTER = itertools.count()


class CollectingNode(object):
    def __init__(self, collect_callback, io_error_callback, hosts_and_ports=[("", 60709), ("", 60708)], debug_log=None):
        """ collect_callback will be called to collect values
            io_error_callbakc is called when an io error ocours (the exception is passed as a param).
                NOTE: ** IT IS YOUR RESPONSIBILITY TO RE-Connect.
        """
        self.hosts_and_ports = normalize_hosts_and_ports(hosts_and_ports)
        self.debug_log = debug_log if debug_log else _noplogger()
        self.collect_callback = collect_callback
        self.io_error_callback = io_error_callback
        self.background_thread = None
        self.id = self.gen_id()
        self.wfile = self.rfile = self.socket = None
        self._shutting_down = False

    def gen_id(self):
        id = _GLOBAL_COUNTER.next()
        return socket.getfqdn() + "_" + str(multiprocessing.current_process().ident) + "_" + str(id) + "_" + str(
            time.time())

    def try_connecting_to_leader(self, throw=False, ping_only=False):
        """ tries to find an elected leader on one of the give hosts and ports.
            Returns None on success or the last error message on failure
        """

        self.socket = None

        for host_port_index in range(len(self.hosts_and_ports)):
            cur_host_port = self.hosts_and_ports[host_port_index]
            try:
                self.debug_log.debug("%s: Trying to connect to a lader on %s.", self.id, cur_host_port)
                candidate_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                candidate_socket.settimeout(10)
                candidate_socket.connect(cur_host_port)

                self.socket = candidate_socket
                break  # success!

            except  IOError as e:
                self.debug_log.warning("%s: Failed to find leader on %s . Error: %s", self.id,
                    cur_host_port, e)
                if host_port_index == len(self.hosts_and_ports) - 1:
                    self.socket = None
                    if throw:
                        raise
                    return str(e)

        self.rfile = self.socket.makefile('rb', 1)
        self.wfile = self.socket.makefile('wb', 1)
        if ping_only:
            self.debug_log.debug("Sending a ping to server.")
            self.send("ping")
        else:
            self.debug_log.debug("%s: Sending id to server.", self.id)
            self.send(self.id)
        if self.receive() != "ack":
            self._close_socket()
            raise Exception("Failed to get ack from leader.")

        self.debug_log.debug("%s: Successfully connected to server.", self.id)

        self.socket.settimeout(None)  # needs to be without a time so recieving thread will block.

        if ping_only:
            self._close_socket()
        return None

    def connect_to_leader(self, timeout_in_sec=120):
        """ tries repeatedly to connect to leader
        """
        wait_times = [0.1, 0.2]
        wait_times.extend(repeat(1, int(timeout_in_sec)))
        last_node_attempt_error = None
        for cur_itr in range(len(wait_times)):
            last_node_attempt_error = self.try_connecting_to_leader()
            if not last_node_attempt_error:
                # success!
                self.start_background_receive()
                return

            time.sleep(wait_times[cur_itr])

        # if we got here things are bad..
        raise IOError("Failed to ellect a leader. Tried %s times. Last node attempt error: %s." %
                      (cur_itr, last_node_attempt_error)
        )

    def start_background_receive(self):
        def target():
            try:
                self.debug_log.debug('%s: Cmd exec thread is running', self.id)
                self.execute_commands()
                self.debug_log.debug('%s: Cmd exec thread stoppinng', self.id)
            except Exception as e:
                self.debug_log.exception("Cmd exec had an error (id:%s): %s", self.id, e)

        self.background_thread = threading.Thread(target=target)
        self.background_thread.daemon = True
        self.background_thread.start()

        return

    def send(self, data):
        pickle.dump(data, self.wfile, pickle.HIGHEST_PROTOCOL)
        self.wfile.flush()

    def receive(self):
        return pickle.load(self.rfile)

    def get_command_and_execute(self):
        cmd = self.receive()
        self.debug_log.debug("%s: Got %s", self.id, cmd)
        if cmd == "quit":
            self.close()
            return False
        if cmd == "reconnect":
            self._close_socket()
            self.connect_to_leader()
            return False
        if cmd == "collect":
            self.debug_log.info("'%s': Collecting.", self.id)
            v = self.collect_callback()
            self.send(v)
            self.debug_log.info("'%s': Done collecting.", self.id)
            return True

        if cmd == "wait":
            return True

    def execute_commands(self):
        go = True
        while go and not self._shutting_down:
            try:
                go = self.get_command_and_execute()
            except (IOError, EOFError) as e:
                if not self._shutting_down:
                    self.debug_log.warning("%s: Got an IOError/EOFError %s" % (self.id, e))
                    self._close_socket()

                    self.debug_log.info("%s: Call io_error_callback." % (self.id, ))
                    self.io_error_callback(e)
                go = False

    def _close_socket(self):
        self.wfile.close()
        self.rfile.close()
        try:
            #explicitly shutdown.  socket.close() merely releases
            #the socket and waits for GC to perform the actual close.
            self.socket.shutdown(socket.SHUT_WR)
        except socket.error:
            pass  # some platforms may raise ENOTCONN here
        self.socket.close()
        self.socket = None

    def close(self):
        self.debug_log.info("%s: closing..", self.id)
        self._shutting_down = True
        if self.socket:
            if self.wfile.closed:
                self.wfile.flush()

            self._close_socket()


def elect_leader(collecting_node, collecting_leader, timeout_in_sec=120):
    """ initiates the process of electing a leader between running processes. All processes are assumed to call this
        function whenever in doubt of the current leader. This can be due to network issues, or at startup.
        Protocol:
            - Try to connect to an existing leader.
            - Try to become a leader.
            - Wait (increasingly long, 0.1, 0.2, 0.5, 0.5, 1, 1, 1)
            - If got here scream!

        Input - configured collecting_node and collecting leader.

        returns:
            (Status, last_node_attempt_error, last_leader_attempt_error)
                Status= True if leader (collecting_leader is now answering requests), false if not (collecting_node is
                    connected to ellected leader)

    """
    wait_times = [0.1, 0.2]
    wait_times.extend(repeat(1, int(timeout_in_sec)))
    last_node_attempt_error = None
    last_leader_attempt_error = None
    for cur_itr in range(len(wait_times)):
        last_node_attempt_error = collecting_node.try_connecting_to_leader()
        if not last_node_attempt_error:
            # success!
            collecting_node.start_background_receive()
            return (False, last_node_attempt_error, last_leader_attempt_error)

        last_leader_attempt_error = collecting_leader.try_to_lead()
        if not last_leader_attempt_error:
            # success!
            return (True, last_node_attempt_error, last_leader_attempt_error)

        time.sleep(wait_times[cur_itr])

    # if we got here things are bad..
    raise Exception(
        "Failed to ellect a leader. Tried %s times. Last node attempt error: %s. Last leader attempt error: %s." %
        (cur_itr, last_node_attempt_error, last_leader_attempt_error)
    )
