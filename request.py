import json

from exceptions import RabbitRpcException, BadResponseException
from rpc_raw import rpc_request_raw
from threading import Thread


class RabbitRpcRequest:
    def __init__(self):
        self.returned_json = None
        self._in_progress = False
        self.return_fields = None

    def _threaded_request(self, rabbit_host, target_queue, request_body, queue_declare):
        self.returned_json = json.loads(rpc_request_raw(
            rabbit_host=rabbit_host,
            target_queue=target_queue,
            request_body=request_body,
            queue_declare=queue_declare
        ))
        self._in_progress = False

    def in_progress(self):
        return self._in_progress

    def request(self, rabbit_host, target_queue, function, arguments, return_fields, queue_declare=True):
        self._in_progress = True
        self.return_fields = return_fields
        request = {
            'function': function,
            'arguments': arguments
        }

        self.returned_json = None
        thread = Thread(target=self._threaded_request,
                        args=(rabbit_host, target_queue, json.dumps(request), queue_declare))
        thread.start()

    def response(self):
        if self.in_progress():
            raise ValueError('request in progress')

        status_code = self.returned_json.get('status_code')
        if status_code is None:
            raise RabbitRpcException(message='no status code in rpc response')

        if status_code == 'ok':
            response = self.returned_json.get('response')
            if response is None:
                raise BadResponseException(message='no response field in rpc response')
            absent_fields = [field for field in self.return_fields if field not in response]
            if len(absent_fields) != 0:
                raise BadResponseException('return fields "%s" not found in rpc response' % str(absent_fields))

            if len(self.return_fields) == 1:
                return response[self.return_fields[0]]
            else:
                # Чтобы можно было делать a, b, c = rpc_request(..)
                return [response[field] for field in self.return_fields]
        else:
            raise RabbitRpcException(status_code, str(self.returned_json.get('message')))
