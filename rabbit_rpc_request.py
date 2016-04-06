import json
import uuid

import pika

from exceptions import RabbitRpcException, BadResponseException





def rpc_request(target_queue, function, arguments, return_fields, queue_declare=True):
    """

    example:

    actor, director = rpc_request('movie_info_queue', ['actor', 'director'])

    rpc response must be:
    {
        'status_code': 'ok',
        'response': {
            'title': 'batman',
            'actor': 345435,
            'director': 345345,
            ...
        }
    }

    RpcException if status_code not 'ok'
    RabbitRpcException if response is bad
    """


