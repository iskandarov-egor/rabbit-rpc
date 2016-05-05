import json

from rabbit_rpc.exceptions import RabbitRpcException, BadResponseException
from rabbit_rpc.rpc_raw import rpc_request_raw
from threading import Thread


class RabbitRpcRequest:
    def __init__(self):
        self.returned_json = None
        self._in_progress = True
        self.return_fields = None

    def in_progress(self):
        """return whether request is still in progress"""
        return self._in_progress

    def response(self):
        """
        returns response fields specified in return_fields, or response as dict if return_fields in None
        raises:
            ValueError if request is still in progress
            BadResponseException if response is bad or return_fields are not present in response.
            RabbitRpcException if status_code is not 'ok'.

        examples:
            x = request.response() # if return_fields is None
            x, y, z = request.response() # if return_fields is a list of length 3

        """
        if self.in_progress():
            raise ValueError('request in progress')

        status_code = self.returned_json.get('status_code')
        if status_code is None:
            raise RabbitRpcException(message='no status code in rpc response')

        if status_code == 'ok':
            response = self.returned_json.get('response')
            if response is None:
                raise BadResponseException(message='no response field in rpc response')
            if self.return_fields is None:
                return response
            absent_fields = [field for field in self.return_fields if field not in response]
            if len(absent_fields) != 0:
                raise BadResponseException('return fields "%s" not found in rpc response' % str(absent_fields))

            if len(self.return_fields) == 1:
                return response[self.return_fields[0]]
            else:
                return [response[field] for field in self.return_fields]
        else:
            raise RabbitRpcException(status_code, str(self.returned_json.get('message')))


def request(rabbit_host, exchange, routing_key, function, arguments, timeout=None, return_fields=None):
    """
    Call a remote function with arguments and return response or specified fields from response.
    Returns a RabbitRpcRequest object

    :param function: name of function to call
    :param arguments: dict with function arguments
    :param timeout: timeout in seconds
    :param return_fields: None or list of response keys to return
    """
    request_object = RabbitRpcRequest()

    request_object.return_fields = return_fields
    request_body = {
        'function': function,
        'arguments': arguments
    }

    request_object.returned_json = None

    def threaded_request():
        request_object.returned_json = json.loads(rpc_request_raw(
            rabbit_host=rabbit_host,
            exchange=exchange,
            routing_key=routing_key,
            request_body=json.dumps(request_body),
            timeout=timeout
        ))
        request_object._in_progress = False

    thread = Thread(target=threaded_request)
    thread.start()
    return request_object
