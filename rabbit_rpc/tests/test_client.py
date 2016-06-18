from rabbit_rpc.exceptions import RabbitRpcException, BadResponseException
import rabbit_rpc


arguments = {
    'movie': 'batman'
}

response = rabbit_rpc.rpc(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments=arguments
)
while True:
    res = response.check(0)
    if res is not None:
        response = res
        break


title, actor = response['title'], response['actor']
print(title, actor)

response = rabbit_rpc.rpc(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info2',
    arguments=arguments
)

try:
    response = response.check(None)
    title, actor = response['title'], response['actor']
except RabbitRpcException as e:
    print(e)

response = rabbit_rpc.rpc(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments=arguments
)

try:
    response = response.check(None)
    title, actor = response['title'], response['actor']
except BadResponseException as e:
    print(e)

response = rabbit_rpc.rpc(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments=arguments
)

print(response.check(None))

response = rabbit_rpc.rpc(
    rabbit_host='localhost',
    exchange='exc',
    routing_key='key',
    function='movie_info',
    arguments={'key': 'super_key', 'movie': 'movie'}
)
print(response.check(None)['value'])

