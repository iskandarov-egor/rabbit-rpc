from rabbit_rpc.exceptions import RabbitRpcException, BadResponseException
from rabbit_rpc.request import RabbitRpcRequest


rpc_request = RabbitRpcRequest()

arguments = {
    'movie': 'batman'
}

rpc_request.request(
    rabbit_host='localhost',
    target_queue='test',
    function='movie_info',
    arguments=arguments,
    return_fields=['title', 'actor'],
    queue_declare=True
)

while rpc_request.in_progress():
    print('waiting')
    pass

title, actor = rpc_request.response()
print(title, actor)

rpc_request.request(
    rabbit_host='localhost',
    target_queue='test',
    function='movie_info2',
    arguments=arguments,
    return_fields=['title', 'actor'],
    queue_declare=True
)

while rpc_request.in_progress():
    pass

try:
    title, actor = rpc_request.response()
except RabbitRpcException as e:
    print(e)

rpc_request.request(
    rabbit_host='localhost',
    target_queue='test',
    function='movie_info',
    arguments=arguments,
    return_fields=['title2', 'actor'],
    queue_declare=True
)

while rpc_request.in_progress():
    pass

try:
    title, actor = rpc_request.response()
except BadResponseException as e:
    print(e)