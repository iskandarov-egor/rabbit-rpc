import json

from rabbit_rpc.exceptions import RabbitRpcException, BadResponseException
from rabbit_rpc.rabbit_request import rabbit_request
from threading import Thread


class RpcResponse:
    def __init__(self, response):
        self._response = response


    def check(self, timeout=0):
        """
        :param timeout: max time to block. if None, block until response is ready

        returns response as dict, or None if response is not ready

        raises:
            BadResponseException if response is bad
            RabbitRpcException if status_code is not 'ok'.

        """
        response = self._response.check(timeout=timeout)

        if response is None:
            return None

        response = json.loads(response)

        status_code = response.get('status_code')
        if status_code is None:
            raise BadResponseException(message='no status code in rpc response')

        if status_code == 'ok':
            response = response.get('response')
            if response is None:
                raise BadResponseException(message='no response field in rpc response')
            return response
        else:
            raise RabbitRpcException(status_code, str(response.get('message')))


def rpc(rabbit_host, exchange, routing_key, function, arguments):
    """
    Call a remote function with arguments
    Returns an RpcResponse object

    :param function: name of function to call
    :param arguments: dict with function arguments
    """

    request_body = {
        'function': function,
        'arguments': arguments
    }

    response = RpcResponse(rabbit_request(
            rabbit_host=rabbit_host,
            exchange=exchange,
            routing_key=routing_key,
            request_body=json.dumps(request_body)
        ))


    return response
