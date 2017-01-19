import json
import logging

try:
    # Python 2.x
    import urllib2 as HTTPClient
    from urllib2 import HTTPError
except ImportError:
    # Python 3.x
    import urllib.request as HTTPClient
    from urllib.error import HTTPError

class SenderBase(object):
    """The base class for all types of senders for use in conjunction with an implementation of :class:`QueueBase`.

    The queue will notify the sender that it needs to pick up items. The concrete sender implementation will
    listen to these notifications and will pull items from the queue getting at most :func:`send_buffer_size` items.
    It will then call :func:`send` using the list of items pulled from the queue.
    """
    def __init__(self, service_endpoint_uri, send_timeout=None):
        """Initializes a new instance of the class.

        Args:
            service_endpoint_uri (str) the address of the service to send telemetry data to.
        """
        self._service_endpoint_uri = service_endpoint_uri
        self._queue = None
        self._send_buffer_size = 100

    @property
    def service_endpoint_uri(self):
        """The HTTP or HTTPS endpoint that this sender will send data to.

        Args:
            value (str). the service endpoint URI.

        Returns:
            str. the service endpoint URI.
        """
        return self._service_endpoint_uri

    @service_endpoint_uri.setter
    def service_endpoint_uri(self, value):
        """The service endpoint URI where this sender will send data to.

        Args:
            value (str). the service endpoint URI.

        Returns:
            str. the service endpoint URI.
        """
        self._service_endpoint_uri = value

    @property
    def queue(self):
        """The queue that this sender is draining. While :class:`SenderBase` doesn't implement any means of doing
        so, derivations of this class do.

        Args:
            value (:class:`QueueBase`). the queue instance that this sender is draining.

        Returns:
            :class:`QueueBase`. the queue instance that this sender is draining.
        """
        return self._queue

    @queue.setter
    def queue(self, value):
        """The queue that this sender is draining. While :class:`SenderBase` doesn't implement any means of doing
        so, derivations of this class do.

        Args:
            value (:class:`QueueBase`). the queue instance that this sender is draining.

        Returns:
            :class:`QueueBase`. the queue instance that this sender is draining.
        """
        self._queue = value

    @property
    def send_buffer_size(self):
        """The buffer size for a single batch of telemetry. This is the maximum number of items in a single service
        request that this sender is going to send.

        Args:
            value (int). the maximum number of items in a telemetry batch.

        Returns:
            int. the maximum number of items in a telemetry batch.
        """
        return self._send_buffer_size

    @send_buffer_size.setter
    def send_buffer_size(self, value):
        """The buffer size for a single batch of telemetry. This is the maximum number of items in a single service
        request that this sender is going to send.

        Args:
            value (int). the maximum number of items in a telemetry batch.

        Returns:
            int. the maximum number of items in a telemetry batch.
        """
        if value < 1:
            value = 1
        self._send_buffer_size = value

    def send(self, data_to_send):
        """ Immediately sends the data passed in to :func:`service_endpoint_uri`. If the service request fails, the
        passed in items are pushed back to the :func:`queue`.

        Args:
            data_to_send (Array): an array of :class:`contracts.Envelope` objects to send to the service.
        """
        request_payload = json.dumps([ a.write() for a in data_to_send ])
        request = HTTPClient.Request(self._service_endpoint_uri, bytearray(request_payload, 'utf-8'), { 'Accept': 'application/json', 'Content-Type' : 'application/json; charset=utf-8' })
        try:
            logging.debug("Sending request to %s - payload length: %d bytes", self._service_endpoint_uri, len(request_payload))
            response = HTTPClient.urlopen(request)
            status_code = response.getcode()
            logging.debug("Response: %d - %s", status_code, response.read())
            if 200 <= status_code < 300:
                return
            logging.warn("Error status code received from server: %d", status_code)
            logging.info("Response: %s", response.read())
        except HTTPError as e:
            logging.info("HTTP Exception caught: %s", e)
            if e.getcode() == 400:
                return
        except Exception as e:
            logging.info("Unknown Exception caught: %s", e)

        # Add our unsent data back on to the queue
        logging.debug("Putting back %d requests to the send queue", len(data_to_send))
        for data in data_to_send:
            self._queue.put(data)