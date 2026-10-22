"""Local gateway discovery, a single hidden process, and dynamic client ports."""
import argparse
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time

from transport import JsonLineSocket, close_socket

ROOT = Path(__file__).resolve().parent
HOST = '127.0.0.1'
CONTROL_PORT = 34232
PROTOCOL = 2


class FileLock:
    def __init__(self, path, timeout=20):
        self.path, self.timeout, self.handle = Path(path), timeout, None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open('a+b')
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.handle.seek(0)
                if os.name == 'nt':
                    import msvcrt
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                # Initialize only after acquiring the byte lock: another launcher
                # may already hold byte zero even while the new file is empty.
                if self.path.stat().st_size == 0:
                    self.handle.write(b'0')
                    self.handle.flush()
                return self
            except OSError:
                if time.monotonic() >= deadline:
                    self.handle.close()
                    raise TimeoutError('Gateway startup lock is busy')
                time.sleep(0.1)

    def __exit__(self, *args):
        self.handle.seek(0)
        if os.name == 'nt':
            import msvcrt
            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        self.handle.close()


def control_request(command, data=None, port=CONTROL_PORT):
    with socket.create_connection((HOST, port), timeout=1) as sock:
        sock.settimeout(0.5)
        stream = JsonLineSocket(sock)
        stream.send(dict(cmd=command, id=0, data=data))
        response = stream.read(time.monotonic()+3)
    if response.get('error'):
        raise RuntimeError(response['error'])
    result = response['data']
    if result.get('protocol') != PROTOCOL or result.get('project') != str(ROOT):
        raise RuntimeError('Another or outdated gateway occupies the control port')
    return result


def ensure_gateway(requests_port, callbacks_port):
    try:
        control_request('status')
    except (ConnectionError, OSError, TimeoutError):
        with FileLock(ROOT/'runtime'/'gateway.startup.lock'):
            try:
                control_request('status')
            except (ConnectionError, OSError, TimeoutError):
                executable = Path(sys.executable)
                if os.name == 'nt':
                    executable = executable.with_name('pythonw.exe')
                runtime = ROOT/'runtime'
                runtime.mkdir(exist_ok=True)
                kwargs = dict(cwd=str(ROOT), stdin=subprocess.DEVNULL,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                if os.name == 'nt':
                    kwargs['creationflags'] = subprocess.CREATE_NO_WINDOW
                else:
                    kwargs['start_new_session'] = True
                child = subprocess.Popen(
                    [str(executable), '-X', 'utf8', '-B', str(ROOT/'gateway_bootstrap.py')],
                    **kwargs)
                deadline = time.monotonic()+20
                while True:
                    try:
                        control_request('status')
                        break
                    except (ConnectionError, OSError, TimeoutError):
                        if child.poll() is not None or time.monotonic() >= deadline:
                            raise RuntimeError('Gateway failed to start; see runtime/gateway_startup.log and gateway.log')
                        time.sleep(0.1)
    return control_request('ensure_ports', dict(
        requests_port=requests_port, callbacks_port=callbacks_port))


class ControlServer:
    def __init__(self, server, port=CONTROL_PORT):
        self.server = server
        self.shutdown_requested = threading.Event()
        self.stopped = threading.Event()
        self.socket = socket.socket()
        try:
            if hasattr(socket, 'SO_EXCLUSIVEADDRUSE'):
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            self.socket.bind((HOST, port))
            self.socket.listen(16)
            self.socket.settimeout(0.5)
        except Exception:
            close_socket(self.socket)
            raise
        self.thread = threading.Thread(target=self.run, name='GatewayControl', daemon=True)

    def start(self):
        self.thread.start()
        return self

    def run(self):
        while not self.stopped.is_set():
            try:
                sock, _ = self.socket.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            with sock:
                sock.settimeout(0.5)
                stream = JsonLineSocket(sock)
                try:
                    request = stream.read(time.monotonic()+3)
                    result = self.execute(request)
                    stream.send(dict(cmd=request['cmd'], id=request.get('id'), data=result))
                except Exception as exc:
                    try:
                        stream.send(dict(cmd='control_error', id=0, data=None, error=str(exc)))
                    except OSError:
                        pass

    def execute(self, request):
        cmd = request['cmd']
        if cmd == 'ensure_ports':
            data = request['data']
            ports = (data['requests_port'], data['callbacks_port'])
            reserved = {*self.server.engine.ports, self.socket.getsockname()[1]}
            if any(not isinstance(p, int) or not 1 <= p <= 65535 or p in reserved for p in ports):
                raise ValueError('Client ports overlap with the gateway or Lua connector')
            pair = self.server.add_pair(*ports)
            extra = dict(ports=pair)
        elif cmd == 'stop':
            with self.server.lock:
                if any(not s.closed.is_set() for s in self.server.sessions.values()):
                    raise ValueError('Stop the bot processes before stopping the gateway')
                self.shutdown_requested.set()
            extra = {}
        elif cmd == 'status':
            extra = {}
        else:
            raise ValueError('Unknown control command')
        return dict(protocol=PROTOCOL, project=str(ROOT), pid=os.getpid(),
                    upstream_connected=bool(self.server.engine.client and self.server.engine.client.connected),
                    bot_ports=list(self.server.pairs), **extra)

    def close(self):
        self.stopped.set()
        close_socket(self.socket)
        if self.thread.ident:
            self.thread.join(timeout=4)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stop', action='store_true')
    parser.add_argument('--start', action='store_true')
    args = parser.parse_args()
    if args.stop and args.start:
        parser.error('Use --start or --stop')
    result = (ensure_gateway(34130, 34131) if args.start
              else control_request('stop' if args.stop else 'status'))
    if sys.stdout is not None:
        print(result)


if __name__ == '__main__':
    main()

