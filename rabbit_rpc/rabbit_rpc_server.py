import inspect
import traceback

import pika
import json

from rabbit_rpc.exceptions import RabbitRpcException


class RawRabbitRpcServer:
    def __init__(self, host, callback, exchange, routing_key, queue_name=None):
        self.host = host
        self.exchange = exchange
        self.routing_key = routing_key
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=host, connection_attempts=10, retry_delay=10))
        self._channel = self._connection.channel()
        self._channel.basic_qos(prefetch_count=1)
        self._channel.exchange_declare(exchange=exchange, exchange_type='direct')
        if queue_name is None:
            queue_info = self._channel.queue_declare(exclusive=True)
            queue_name = queue_info.method.queue
        else:
            self._channel.queue_declare(queue=queue_name)
        self._channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)

        def on_request(ch, method, props, body):
            print('rabbit rpc server received message:')
            print(body)
            response = callback(body)

            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=response)
            ch.basic_ack(delivery_tag=method.delivery_tag)

        self._channel.basic_consume(on_request, queue=queue_name)

    def start(self):
        print('starting rpc server. host "%s" exchange "%s" routing_key "%s"' % (self.host, self.exchange, self.routing_key))
        self._channel.start_consuming()

    def close(self):
        self._connection.close()


def get_function_arguments(f):
    return inspect.signature(f).parameters.values()


class RabbitRpcServer:
    def __init__(self, host, exchange, routing_key):
        self.callbacks = {}

        def _callback(body):
            body = json.loads(body.decode('utf-8'))
            function_name = body.get('function')
            if function_name is None:
                return json.dumps({
                    'status_code': 'bad_request',
                    'message': 'function field not found in request'
                })
            callback = self.callbacks.get(function_name)
            if callback is None:
                return json.dumps({
                    'status_code': 'bad_request',
                    'message': 'no such function: "%s"' % str(function_name)
                })
            request_arguments = body.get('arguments')
            if request_arguments is None:
                return json.dumps({
                    'status_code': 'bad_request',
                    'message': 'arguments field not found in request'
                })

            arguments = []

            for param_name, param in inspect.signature(callback).parameters.items():
                has_default = (param.default is not inspect.Parameter.empty)
                is_provided = (param_name in request_arguments.keys())
                if is_provided:
                    arguments.append(request_arguments.get(param_name))
                else:
                    if has_default:
                        arguments.append(param.default)
                    else:
                        return json.dumps({
                            'status_code': 'bad_request',
                            'message': 'argument "%s" not found in request' % param_name
                        })

            try:
                response = callback(*arguments)
                response = json.dumps({
                    'status_code': 'ok',
                    'response': response
                }, ensure_ascii=False).encode('utf8')
            except RabbitRpcException as e:
                response = json.dumps({
                    'status_code': e.status_code,
                    'message': e.message
                })
            except Exception as e:
                tb = traceback.format_exc()
                response = json.dumps({
                    'status_code': 'unknown_error',
                    'message': 'error: "%s", traceback: "%s"' % (str(e), tb)
                })
            return response

        self.raw_server = RawRabbitRpcServer(host=host, callback=_callback, exchange=exchange, routing_key=routing_key)

    def start(self):
        self.raw_server.start()

    def add_callback(self, callback):
        self.callbacks[callback.__name__] = callback

    def close(self):
        self.raw_server.close()
