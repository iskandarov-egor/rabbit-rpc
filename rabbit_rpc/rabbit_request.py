import uuid

import pika
import time

class Response:
    def __init__(self, connection):
        self._response = None
        self._connection = connection

    def check(self, timeout=0):
        """
        :param timeout: max time to block. if None, block until response is ready
        :return: response string, or None if response not ready
        """
        if timeout is None:
            while self._response is None:
                self._connection.process_data_events(time_limit=None)
        else:
            now = time.time()
            start_time = now

            while self._response is None and now - start_time <= timeout:
                self._connection.process_data_events(time_limit=max(0, timeout - (now - start_time)))
                now = time.time()
        return self._response

    def response(self):
        return self._response


def rabbit_request(rabbit_host, exchange, routing_key, request_body):
    """
    :param request_body: string to send
    :return: a Response object
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbit_host, connection_attempts=10, retry_delay=10), )
    channel = connection.channel()
    queue_info = channel.queue_declare(exclusive=True)
    channel.exchange_declare(exchange=exchange, exchange_type='direct')
    callback_queue = queue_info.method.queue
    corr_id = str(uuid.uuid4())
    response = Response(connection)

    def on_response(ch, method, props, body):
        # if self._corr_id == props.correlation_id:
        print('rpc response: ', body)
        response._response = body.decode('utf-8')

    channel.basic_consume(on_response, no_ack=True, queue=callback_queue)

    print('rabbit-rpc sending message to "%s"."%s": ' % (exchange, routing_key))
    print(request_body)

    channel.basic_publish(exchange=exchange,
                          routing_key=routing_key,
                          properties=pika.BasicProperties(
                              reply_to=callback_queue,
                              correlation_id=corr_id,
                          ),
                          body=request_body)
    return response
