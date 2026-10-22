"""Bounded, age-limited logs; cleanup only touches known NetDCA log names."""
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import re
import time

ROOT = Path(__file__).resolve().parent
MAX_BYTES = 5 * 1024 * 1024
BACKUPS = 5
RETENTION_DAYS = 14
MAX_RECORD_BYTES = 64 * 1024


class RetainedFileHandler(RotatingFileHandler):
    def __init__(self, filename, max_bytes=MAX_BYTES, backups=BACKUPS,
                 retention_days=RETENTION_DAYS):
        self.retention_seconds = retention_days * 86400
        super().__init__(filename, maxBytes=max_bytes, backupCount=backups,
                         encoding='utf-8', errors='replace', delay=True)
        path = Path(filename)
        self.log_day = time.strftime('%Y-%m-%d')
        if path.exists() and path.stat().st_size:
            with path.open('r', encoding='utf-8', errors='replace') as stream:
                day = stream.read(10)
            if re.fullmatch(r'\d{4}-\d{2}-\d{2}', day):
                self.log_day = day
        if path.exists() and path.stat().st_mtime < time.time()-self.retention_seconds:
            self.doRollover()
        self.purge()

    def format(self, record):
        text = super().format(record)
        raw = text.encode('utf-8', errors='replace')
        if len(raw) > MAX_RECORD_BYTES:
            text = raw[:MAX_RECORD_BYTES-32].decode('utf-8', errors='ignore')+' ... [truncated]'
        return text

    def shouldRollover(self, record):
        if self.log_day != time.strftime('%Y-%m-%d'):
            return True
        path = Path(self.baseFilename)
        if path.exists() and path.stat().st_mtime < time.time()-self.retention_seconds:
            return True
        if self.stream is None:
            self.stream = self._open()
        self.stream.seek(0, 2)
        return self.stream.tell()+len((self.format(record)+self.terminator).encode('utf-8')) > self.maxBytes

    def doRollover(self):
        super().doRollover()
        self.log_day = time.strftime('%Y-%m-%d')

    def purge(self):
        path = Path(self.baseFilename)
        cutoff = time.time()-self.retention_seconds
        for item in path.parent.glob(path.name+'.*'):
            suffix = item.name[len(path.name)+1:]
            try:
                if suffix.isdigit() and (int(suffix) > self.backupCount or item.stat().st_mtime < cutoff):
                    item.unlink()
            except OSError:
                pass

    def emit(self, record):
        super().emit(record)
        self.purge()


def make_handler(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return RetainedFileHandler(path)


def cleanup_logs(root=ROOT, now=None):
    """Delete old closed logs; Windows leaves open files for their own handler."""
    root = Path(root).resolve()
    cutoff = (time.time() if now is None else now)-RETENTION_DAYS*86400
    removed = []
    runtime = root/'runtime'
    families = [(runtime, r'(?:gateway|gateway_startup)\.log(?:\.\d+)?')]
    if runtime.exists():
        families.extend((folder, r'bot\.log(?:\.\d+)?')
                        for folder in runtime.glob('bot_[0-9]*') if folder.is_dir())
    lua = root/'QuikPy'/'QUIK'/'lua'
    for folder in (lua, lua/'logs'):
        families.append((folder, r'(?:NetDCA_lua\.log(?:\.\d+)?|QUIK#_\d{8}\.log)'))
    for folder, pattern in families:
        if not folder.is_dir():
            continue
        try:
            entries = list(folder.iterdir())
        except OSError:
            continue
        for item in entries:
            try:
                if not re.fullmatch(pattern, item.name) or not item.is_file():
                    continue
                # Never follow a symlink out of this copy.
                if not item.resolve().is_relative_to(root):
                    continue
                if item.stat().st_mtime < cutoff:
                    item.unlink()
                    removed.append(str(item))
            except OSError:
                pass
    return removed

