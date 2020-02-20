from io import BytesIO
from collections import namedtuple
from socket import error as socket_error

Error = namedtuple('Error', ('message',))

class CommandError(Exception):
    pass

class Disconnect(Exception):
    pass

class ProtocolHandler:
    def __init__(self):
        # refers to https://redis.io/topics/protocol
        self.handlers = {
            b'+': self.handle_str,
            b'-': self.handle_error,
            b':': self.handle_int,
            b'$': self.handle_binary,
            b'*': self.handle_array,
            b'%': self.handle_dict,
        }
    # deserialize data
    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            raise CommandError('bad request')

    def handle_str(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n').decode('utf8')

    def handle_error(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n').decode('utf8')

    def handle_int(self, socket_file):
        return int(socket_file.readline().rstrip(b'\r\n'))

    def handle_binary(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2]

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        return [self.handle_request(socket_file) for _ in range(num_elements)]

    def handle_dict(self, socket_file):
        num_keys = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_keys * 2)]
        return dict(zip(elements[::2], elements[1::2]))
    # serialize data
    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        socket_file.write(buf.getvalue())
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, str):
            buf.write('+{}\r\n'.format(data).encode('utf8'))
        elif isinstance(data, bytes):
            buf.write(b'$%d\r\n%s\r\n' % (len(data), data))
        elif isinstance(data, int):
            buf.write(b':%d\r\n' % data)
        elif isinstance(data, Error):
            buf.write('-{}\r\n'.format(data.message).encode('utf8'))
        elif isinstance(data, (list, tuple)):
            buf.write(b'*%d\r\n' % len(data))
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(b'%%%d\r\n' % len(data))
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b'$-1\r\n')
        else:
            raise CommandError(f'unrecognized type: {type(data)}')



