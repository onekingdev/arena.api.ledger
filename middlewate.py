from werkzeug.wrappers import Request, Response


class Middleware:
    def __init__(self, app):
        self.app = app
        with open('token.txt') as f:
            self.token = f.read()

    def __call__(self, environ, start_response):
        request = Request(environ)
        auth_token = request.headers.get('X-AUTH-TOKEN')

        if auth_token is None:
            res = Response(u'Token is not provided', mimetype='text/plain', status=401)
            return res(environ, start_response)

        if auth_token != self.token:
            res = Response(u'Token mismatch', mimetype='text/plain', status=401)
            return res(environ, start_response)

        return self.app(environ, start_response)
