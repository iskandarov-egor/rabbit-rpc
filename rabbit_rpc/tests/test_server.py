from rabbit_rpc.exceptions import RabbitRpcException
from rabbit_rpc.rabbit_rpc_server import RabbitRpcServer


def movie_info(movie, detail=True):
    if movie == 'batman':
        response = {
            'title': 'the batman'
        }
        if detail:
            response['actor'] = 'batman'
        return response
    else:
        raise RabbitRpcException(status_code='bad_movie')

server = RabbitRpcServer(host='localhost', exchange='exc', routing_key='key')
server.add_callback(movie_info)
server.start()