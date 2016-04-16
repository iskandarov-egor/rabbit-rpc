from rabbit_rpc.exceptions import RabbitRpcException, BadResponseException
import rabbit_rpc


arguments = {
    'movie': 'batman'
}

rpc_request = rabbit_rpc.request(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments=arguments,
    return_fields=['title', 'actor']
)

while rpc_request.in_progress():
    # print('waiting')
    pass

title, actor = rpc_request.response()
print(title, actor)

rpc_request = rabbit_rpc.request(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info2',
    arguments=arguments,
    return_fields=['title', 'actor']
)

while rpc_request.in_progress():
    pass

try:
    title, actor = rpc_request.response()
except RabbitRpcException as e:
    print(e)

rpc_request = rabbit_rpc.request(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments=arguments,
    return_fields=['title2', 'actor'],
)

while rpc_request.in_progress():
    pass

try:
    title, actor = rpc_request.response()
except BadResponseException as e:
    print(e)

rpc_request = rabbit_rpc.request(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments=arguments,
    return_fields=None,
)

while rpc_request.in_progress():
    pass

print(rpc_request.response())