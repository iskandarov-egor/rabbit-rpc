import uuid

import pika


def rpc_request_raw(rabbit_host, target_queue, request_body, queue_declare=True):
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host=rabbit_host, connection_attempts=10, retry_delay=10), )
    channel = connection.channel()
    result = channel.queue_declare(exclusive=True)
    if queue_declare:
        channel.queue_declare(queue=target_queue)
    callback_queue = result.method.queue
    corr_id = str(uuid.uuid4())
    response = None

    def on_response(ch, method, props, body):
        nonlocal response
        # if self._corr_id == props.correlation_id:
        print('rpc response: ', body)
        response = body.decode('utf-8')

    channel.basic_consume(on_response, no_ack=True, queue=callback_queue)

    channel.basic_publish(exchange='',
                          routing_key=target_queue,
                          properties=pika.BasicProperties(
                              reply_to=callback_queue,
                              correlation_id=corr_id,
                          ),
                          body=request_body)
    while response is None:
        connection.process_data_events(time_limit=0)

    return response