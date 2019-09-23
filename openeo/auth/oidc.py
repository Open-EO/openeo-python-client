"""
OpenID Connect related functionality and helpers.
"""

import functools
import http.server
import logging
import threading
import time
import warnings
from queue import Queue, Empty
from typing import Tuple


log = logging.getLogger(__name__)


class QueuingRequestHandler(http.server.BaseHTTPRequestHandler):
    """
    Request handler that put results in a threadsafe queue
    """

    def __init__(self, *args, **kwargs):
        self._queue = kwargs.pop('queue', None) or Queue()
        super().__init__(*args, **kwargs)

    def do_GET(self):
        log.info('Request: {p}'.format(p=self.path))
        # TODO: parse path before putting on queue
        self._queue.put(self.path)
        self.send_response(200)
        self.end_headers()

    @classmethod
    def with_queue(cls, queue: Queue):
        """Generate a class (constructor) pre-bound with given queue object"""
        return functools.partial(cls, queue=queue)


class HttpServerThread(threading.Thread):

    def __init__(self, RequestHandlerClass, server_address=('', 0)):
        # Make it a daemon to minimize potential shutdown issues due to `serve_forever`
        super().__init__(daemon=True)
        self._RequestHandlerClass = RequestHandlerClass
        # Server address ('', 0): listen on all ips and let OS pick a free port
        self._server_address = server_address
        self._server = None

    def start(self):
        self._server = http.server.HTTPServer(self._server_address, self._RequestHandlerClass)
        self._log_status("start thread")
        super().start()

    def run(self):
        self._log_status("start serving")
        self._server.serve_forever()
        self._log_status("stop serving")

    def shutdown(self):
        self._log_status("shut down thread")
        self._server.shutdown()

    def server_address_info(self) -> Tuple[int, str, str]:
        """
        Get server address info: (port, host_address, fully_qualified_domain_name)
        """
        if self._server is None:
            raise RuntimeError("Server is not set up yet")
        return self._server.server_port, self._server.server_address[0], self._server.server_name

    def _log_status(self, message):
        port, host, fqdn = self.server_address_info()
        log.info("{c}: {m} (at {h}:{p}, {f})".format(c=self.__class__.__name__, m=message, h=host, p=port, f=fqdn))


def drain_queue(queue: Queue, initial_timeout: float = 10, item_minimum: int = 1, overall_timeout=60):
    """
    Drain the given queue, requiring at least a given number of items (within an initial timeout)
    :param queue: queue to drain
    :param initial_timeout: time in seconds within which a minimum number of items should be fetched
    :param item_minimum: minimum number of items to fetch
    :param overall_timeout: overall timeout to abort when queue doesn't get empty
    :return: generator of items from the queue
    """
    start = time.time()
    count = 0
    while True:
        try:
            yield queue.get(timeout=initial_timeout / 10)
            count += 1
        except Empty:
            pass

        if time.time() > start + initial_timeout and count < item_minimum:
            raise TimeoutError("Only {c} items (<{m}) after initial timeout".format(c=count, m=item_minimum))
        if queue.empty() and count >= item_minimum:
            break
        if time.time() > start + overall_timeout:
            warnings.warn("Queue still not empty after overall timeout: aborting.")
            break

