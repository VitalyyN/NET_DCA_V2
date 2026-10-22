"""Launch copied robots and one hidden gateway without connecting to QUIK."""
import importlib.util
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from concurrent.futures import ThreadPoolExecutor

import gateway_control
from gateway import Engine, GatewayServer
from QuikPy import QuikPy
from run_bot import load_settings, ports_for_bot
from tests.test_v2 import Broker, TEST_ROOT, wait_until

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class LaunchTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(dir=TEST_ROOT)
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        for name in ('gateway.py', 'gateway_control.py', 'transport.py', 'registry.py',
                     'telemetry.py', 'run_bot.py', 'main.py', 'log_policy.py',
                     'gateway_bootstrap.py'):
            shutil.copy2(ROOT/name, self.root/name)
        shutil.copytree(ROOT/'QuikPy', self.root/'QuikPy')
        self.port = free_port()
        path = self.root/'gateway_control.py'
        path.write_text(path.read_text(encoding='utf-8').replace(
            'CONTROL_PORT = 34232', f'CONTROL_PORT = {self.port}'), encoding='utf-8')
        spec = importlib.util.spec_from_file_location('isolated_control', path)
        self.control = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.control)

    def test_copied_settings_get_independent_ids_and_ports(self):
        path = self.root/'settings_4.py'
        shutil.copy2(ROOT/'settings_1.py', path)
        with patch.dict('sys.modules'):
            settings = load_settings(path)
        self.assertEqual(settings.BOT_ID, 'bot_4')
        self.assertEqual((settings.REQUESTS_PORT, settings.CALLBACKS_PORT), (34136, 34137))
        self.assertNotIn(34230, ports_for_bot(51))
        self.assertNotIn(34232, ports_for_bot(52))

    def test_concurrent_start_creates_one_hidden_gateway(self):
        pairs = [(free_port(), free_port()) for _ in range(4)]
        children = []
        original = subprocess.Popen

        def spawn(*args, **kwargs):
            child = original(*args, **kwargs)
            children.append(child)
            if os.name == 'nt':
                self.assertTrue(kwargs['creationflags'] & subprocess.CREATE_NO_WINDOW)
                self.assertTrue(args[0][0].endswith('pythonw.exe'))
            return child

        def cleanup():
            try:
                self.control.control_request('stop')
            except (OSError, RuntimeError, TimeoutError):
                pass
            for child in children:
                try:
                    child.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    child.terminate()
                    child.wait(timeout=5)
        self.addCleanup(cleanup)
        with patch.object(self.control.subprocess, 'Popen', side_effect=spawn):
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(pool.map(lambda pair: self.control.ensure_gateway(*pair), pairs))
        self.assertEqual(len(children), 1)
        self.assertEqual(len({r['pid'] for r in results}), 1)
        status = self.control.control_request('status')
        self.assertEqual(len(status['bot_ports']), 4)
        self.assertFalse(status['upstream_connected'])
        self.assertTrue(children[0].poll() is None)
        # Reuse the live hidden process; no second spawn and no broker connection.
        self.assertEqual(self.control.ensure_gateway(*pairs[0])['pid'], results[0]['pid'])

    @unittest.skipUnless(os.name == 'nt', 'Native batch launcher runs on Windows')
    def test_copied_batch_and_bot_choose_settings_four(self):
        for source, target in (('bot_1.py','bot_4.py'), ('start_bot_1.bat','start_bot_4.bat'),
                               ('settings_1.py','settings_4.py')):
            shutil.copy2(ROOT/source, self.root/target)
        path = self.root/'settings_4.py'
        text = path.read_text(encoding='utf-8')
        text += '\nCLASS="FUTSPREAD"\nSECCODE="SPREAD_4"\nBASE_ASSET_CODE="LEG_4"\nACCOUNT="DEMO"\n'
        path.write_text(text, encoding='utf-8')
        # Live bot_4 may occupy 34136/34137. Keep this copied launcher on
        # isolated ports; numbering itself is covered by the settings test.
        test_ports = (free_port(), free_port())
        runner = self.root/'run_bot.py'
        runner.write_text(runner.read_text(encoding='utf-8').replace(
            'module.REQUESTS_PORT, module.CALLBACKS_PORT = ports_for_bot(number)',
            f'module.REQUESTS_PORT, module.CALLBACKS_PORT = {test_ports!r}'),encoding='utf-8')
        # Retain the native batch logic but use the existing Python environment.
        batch = self.root/'start_bot_4.bat'
        batch.write_text(batch.read_text().replace(
            '%~dp0env\\Scripts\\python.exe', str(Path(sys.base_prefix)/'python.exe')), encoding='ascii')
        engine = Engine(self.root/'test.sqlite3', client_factory=Broker().factory)
        server = GatewayServer(engine).start()
        self.addCleanup(engine.close)
        self.addCleanup(server.close)
        with patch.object(gateway_control, 'ROOT', self.root):
            control = gateway_control.ControlServer(server, port=self.port).start()
            try:
                result = subprocess.run(['cmd.exe', '/c', str(batch), '--check'],
                    input='\n', text=True, encoding='utf-8', capture_output=True,
                    cwd=self.root, timeout=20)
                self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
                self.assertIn('SPREAD_4', result.stdout)
                self.assertEqual(server.pairs, [test_ports])
                self.assertEqual(engine.metrics['events'], 0)
                self.assertFalse((self.root/'runtime'/'bot_1').exists())
            finally:
                control.close()


if __name__ == '__main__':
    unittest.main()

