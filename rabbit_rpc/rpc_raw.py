import uuid

import pika
import time


def rpc_request_raw(rabbit_host, exchange, routing_key, request_body, timeout=None):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbit_host, connection_attempts=10, retry_delay=10), )
    channel = connection.channel()
    queue_info = channel.queue_declare(exclusive=True)
    channel.exchange_declare(exchange=exchange, exchange_type='direct')
    callback_queue = queue_info.method.queue
    corr_id = str(uuid.uuid4())
    response = None

    def on_response(ch, method, props, body):
        nonlocal response
        # if self._corr_id == props.correlation_id:
        print('rpc response: ', body)
        response = body.decode('utf-8')

    channel.basic_consume(on_response, no_ack=True, queue=callback_queue)

    print('rabbit-rpc sending message to "%s"."%s": ' % (exchange, routing_key))
    print(request_body)

    start_time = time.time()

    channel.basic_publish(exchange=exchange,
                          routing_key=routing_key,
                          properties=pika.BasicProperties(
                              reply_to=callback_queue,
                              correlation_id=corr_id,
                          ),
                          body=request_body)
    while response is None:
        if timeout and time.time() - start_time > timeout:
            raise TimeoutError()
        connection.process_data_events(time_limit=timeout)

    return response
