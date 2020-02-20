from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer
import logging

from protocolhandler import ProtocolHandler
from protocolhandler import Disconnect
from protocolhandler import CommandError
from protocolhandler import Error

logger = logging.getLogger(__name__)

class Server:
    # check /etc/hosts
    def __init__(self, host='localhost', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer((host, port), self.connection_handler, spawn=self._pool)
        self._protocol = ProtocolHandler()
        self._kv = {}
        self._commands = self.get_commands()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MSET': self.mset,
            'MGET': self.mget
        }

    def connection_handler(self, conn, address):
        logger.info('Connection received: %s:%s' % address)
        socket_file = conn.makefile('rwb')
        while True:
            try:
                data = self._protocol.handle_request(socket_file)
            except Disconnect:
                logger.info(f'Client went away: %s:%s' % address)
                break

            try:
                resp = self.get_response(data)
            except CommandError:
                logger.exception('Command Error')
                resp = Error(str(CommandError))

            self._protocol.write_response(socket_file, resp)

    def get_response(self, data):
        if not isinstance(data, list):
            try:
                data = data.split() # split by \r\n
            except:
                raise CommandError('Request must be list or simple string')
        if not data:
            raise CommandError('Missing Command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f'Unrecognized command: {command}')
        else:
            logger.debug(f'Received command: {command}')

        return self._commands[command](*data[1:])

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        if key in self._kv:
            del self._kv[key]
            return 1
        return 0

    def flush(self):
        dictlen = len(self._kv)
        self._kv.clear()
        return dictlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        data = list(zip(items[::2], items[1::2]))
        for key, value in data:
            self._kv[key] = value
        return len(data)

    def run(self):
        self._server.serve_forever()

if __name__ == '__main__':
    from gevent import monkey
    monkey.patch_all()
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    Server().run()