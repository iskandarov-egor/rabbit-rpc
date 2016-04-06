class RabbitRpcException(Exception):
    def __init__(self, status_code='unknown_error', message=''):
        self.status_code = status_code
        self.message = message


class BadResponseException(Exception):
    def __init__(self, message=''):
        self.message = message