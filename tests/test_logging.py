"""Log size, age, startup errors, and cleanup boundaries."""
import logging
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from log_policy import RetainedFileHandler, cleanup_logs, MAX_RECORD_BYTES
from tests.test_v2 import TEST_ROOT


class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=TEST_ROOT)
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_size_rotation_limits_bytes_and_archives(self):
        path = self.root/'bot.log'
        handler = RetainedFileHandler(path, max_bytes=256, backups=2)
        self.addCleanup(handler.close)
        for _ in range(30):
            handler.handle(logging.LogRecord('test', logging.INFO, '', 0, 'Я'*60, (), None))
        handler.close()
        files = list(self.root.glob('bot.log*'))
        self.assertLessEqual(len(files), 3)
        self.assertTrue(all(f.stat().st_size <= 256 for f in files))

    def test_small_daily_log_rotates_and_old_archives_expire(self):
        path = self.root/'gateway.log'
        path.write_text('2000-01-01 old record\n', encoding='utf-8')
        old = self.root/'gateway.log.3'
        old.write_text('old archive', encoding='utf-8')
        os.utime(old, (time.time()-20*86400,)*2)
        handler = RetainedFileHandler(path)
        self.addCleanup(handler.close)
        handler.handle(logging.LogRecord('test', logging.INFO, '', 0, 'new record', (), None))
        handler.close()
        self.assertIn('new record', path.read_text(encoding='utf-8'))
        self.assertIn('old record', (self.root/'gateway.log.1').read_text(encoding='utf-8'))
        self.assertFalse(old.exists())

    def test_cleanup_includes_inactive_bots_startup_and_legacy_lua(self):
        names = ('runtime/gateway.log.1', 'runtime/gateway_startup.log.2',
                 'runtime/bot_4/bot.log', 'runtime/bot_4/bot.log.3',
                 'QuikPy/QUIK/lua/logs/NetDCA_lua.log.2',
                 'QuikPy/QUIK/lua/logs/QUIK#_20000101.log',
                 'QuikPy/QUIK/lua/QUIK#_20000101.log')
        for name in names:
            path = self.root/name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('old')
            os.utime(path, (time.time()-20*86400,)*2)
        state = self.root/'runtime/bot_4/state.txt'
        state.write_text('100\nSPREAD\nACCOUNT\nbot_4\nLEG\n')
        os.utime(state, (time.time()-20*86400,)*2)
        unrelated = self.root/'runtime/notes.log'
        unrelated.write_text('keep')
        os.utime(unrelated, (time.time()-20*86400,)*2)
        recent = self.root/'runtime/gateway.log'
        recent.write_text('recent')
        removed = cleanup_logs(self.root)
        self.assertEqual(len(removed), len(names))
        self.assertTrue(all(not (self.root/name).exists() for name in names))
        self.assertTrue(state.exists() and unrelated.exists() and recent.exists())

    def test_huge_record_is_clipped(self):
        path = self.root/'gateway_startup.log'
        handler = RetainedFileHandler(path)
        self.addCleanup(handler.close)
        handler.handle(logging.LogRecord('test', logging.ERROR, '', 0, 'Я'*100000, (), None))
        handler.close()
        self.assertLessEqual(path.stat().st_size, MAX_RECORD_BYTES+1)
        self.assertIn('[truncated]', path.read_text(encoding='utf-8'))

    def test_startup_failure_uses_retained_handler(self):
        import gateway_bootstrap
        with patch.object(gateway_bootstrap, 'ROOT', self.root), \
             patch('gateway.main', side_effect=RuntimeError('startup failure')):
            self.assertEqual(gateway_bootstrap.main(), 1)
        path = self.root/'runtime/gateway_startup.log'
        self.assertIn('startup failure', path.read_text(encoding='utf-8'))


if __name__ == '__main__':
    unittest.main()

