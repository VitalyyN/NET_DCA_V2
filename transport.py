"""Bounded newline-delimited JSON transport shared by clients and gateway."""
import json
import socket
import time

MAX_FRAME_BYTES = 4 * 1024 * 1024
REQUEST_WAIT_SECONDS = 9
# Registration includes connection setup and several sequential read-only RPCs.
# Client deadline includes the gateway's wait and response delivery margin.
REGISTRATION_WAIT_SECONDS = 55
REGISTRATION_TIMEOUT_SECONDS = 60
TRANSACTION_CONFIRM_TIMEOUT_SECONDS = 25

class BridgeDisconnected(ConnectionError):
    pass

class BridgeProtocolError(BridgeDisconnected):
    pass

class RemoteError(RuntimeError):
    def __init__(self, response):
        self.response = response
        super().__init__(response.get('lua_error') or response.get('error') or str(response))

class TransactionUncertain(RemoteError):
    """The broker result must be reconciled before sending another transaction."""


def encode_frame(value):
    raw = json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('cp1251') + b'\n'
    if len(raw) > MAX_FRAME_BYTES:
        raise BridgeProtocolError('JSON frame exceeds the configured limit')
    return raw

class JsonLineSocket:
    def __init__(self, sock, max_bytes=MAX_FRAME_BYTES):
        self.sock = sock
        self.buffer = bytearray()
        self.max_bytes = max_bytes

    def read(self, deadline=None):
        while True:
            end = self.buffer.find(b'\n')
            if end >= 0:
                if end + 1 > self.max_bytes:
                    raise BridgeProtocolError('JSON frame exceeds the configured limit')
                raw = bytes(self.buffer[:end]).rstrip(b'\r')
                del self.buffer[:end + 1]
                if not raw:
                    continue
                try:
                    value = json.loads(raw.decode('cp1251'))
                except (UnicodeError, ValueError) as exc:
                    raise BridgeProtocolError('Malformed JSON frame') from exc
                if not isinstance(value, dict) or not isinstance(value.get('cmd'), str):
                    raise BridgeProtocolError('Expected an object containing cmd')
                return value
            if len(self.buffer) >= self.max_bytes:
                raise BridgeProtocolError('JSON frame exceeds the configured limit')
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError('Timed out waiting for a complete JSON response')
            try:
                fragment = self.sock.recv(min(65536, self.max_bytes - len(self.buffer)))
            except socket.timeout:
                if deadline is None:
                    raise
                continue
            except OSError as exc:
                raise BridgeDisconnected(str(exc)) from exc
            if not fragment:
                raise BridgeDisconnected('Peer closed the connection')
            self.buffer.extend(fragment)

    def send(self, value):
        try:
            self.sock.sendall(encode_frame(value))
        except OSError as exc:
            raise BridgeDisconnected(str(exc)) from exc


def close_socket(sock):
    if sock is None:
        return
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    try:
        sock.close()
    except OSError:
        pass
