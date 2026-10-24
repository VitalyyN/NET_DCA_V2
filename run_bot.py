"""Run independent bot processes with independent Python settings files."""
import argparse
import importlib.util
import contextlib
from decimal import Decimal
import logging
from log_policy import make_handler, cleanup_logs
import math
from pathlib import Path
import runpy
import sys
import re
import time
from lifecycle import WaitStatus

ROOT = Path(__file__).resolve().parent

def load_settings(path):
    path = Path(path).resolve()
    spec = importlib.util.spec_from_file_location('settings', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    numbered = re.fullmatch(r'settings_([1-9]\d*)', path.stem)
    if numbered:
        number = int(numbered.group(1))
        module.BOT_ID = f'bot_{number}'
        module.REQUESTS_PORT, module.CALLBACKS_PORT = ports_for_bot(number)
    if not getattr(module, 'CONFIGURED', False):
        raise ValueError(f'{path.name}: supply the original parameters and set CONFIGURED=True')
    for name in ('BOT_ID', 'CLASS', 'SECCODE', 'BASE_ASSET_CODE', 'ACCOUNT'):
        if not isinstance(getattr(module, name, None), str) or not getattr(module, name):
            raise ValueError(f'{name} must be a nonempty string')
    if not all(c.isalnum() or c in '_-' for c in module.BOT_ID):
        raise ValueError('BOT_ID must contain letters, digits, underscores or hyphens')
    for name in ('LOT_PER_LEVEL', 'LEVELS', 'MAX_LOTS_TOTAL'):
        value = getattr(module, name)
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f'{name} must be a positive integer')
    if not math.isfinite(module.GRID_STEP) or module.GRID_STEP <= 0:
        raise ValueError('GRID_STEP must be positive and finite')
    if module.MAX_LOTS_TOTAL < module.LOT_PER_LEVEL:
        raise ValueError('MAX_LOTS_TOTAL must cover at least one level')
    sys.modules['settings'] = module
    return module


def ports_for_bot(number):
    if not isinstance(number, int) or number < 1:
        raise ValueError('Bot number must be a positive integer')
    port = 34130 + 2 * (number - 1)
    if port >= 34230:
        port += 4  # Skip Lua 34230/34231 and gateway control 34232.
    if port + 1 > 65535:
        raise ValueError('Bot number exceeds the available TCP port range')
    return port, port + 1



def validate_tick(settings, registration):
    tick = Decimal(str(registration['tick']['param_value']))
    if not tick.is_finite() or tick <= 0 or Decimal(str(settings.GRID_STEP)) % tick:
        raise ValueError('GRID_STEP must be a multiple of SEC_PRICE_STEP (price units)')


class ConsoleLog:
    def __init__(self, console, logger):
        self.console, self.logger = console, logger

    def write(self, text):
        count = self.console.write(text)
        if text.strip():
            self.logger.info(text.rstrip())
        return count

    def flush(self):
        self.console.flush()

    def isatty(self):
        return self.console.isatty()


def execute_bot(settings):
    folder = ROOT / 'runtime' / settings.BOT_ID
    folder.mkdir(parents=True, exist_ok=True)
    cleanup_logs(ROOT)
    handler = make_handler(folder/'bot.log')
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger = logging.getLogger('netdca.'+settings.BOT_ID)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        with contextlib.redirect_stdout(ConsoleLog(sys.stdout, logger)), \
             contextlib.redirect_stderr(ConsoleLog(sys.stderr, logger)):
            status = WaitStatus()
            while True:
                try:
                    runpy.run_path(str(ROOT/'main.py'), run_name='__main__')
                    return
                except KeyboardInterrupt:
                    print('Робот остановлен пользователем')
                    return
                except SystemExit as exc:
                    if exc.code in (None, 0):
                        return
                    reason = f'Завершение с кодом {exc.code}'
                except Exception as exc:
                    reason = f'{type(exc).__name__}: {exc}'
                status.show(f'Ожидание после ошибки инициализации: {reason}. '
                            'Повторная загрузка с обязательной сверкой заявок и состояния.')
                try:
                    time.sleep(settings.RECONNECT_DELAY_SEC)
                except KeyboardInterrupt:
                    print('Робот остановлен пользователем')
                    return
    finally:
        logger.removeHandler(handler)
        handler.close()


def wait_for_settings(path):
    status = WaitStatus()
    while True:
        try:
            return load_settings(path)
        except Exception as exc:
            status.show(f'Настройки пока не готовы: {exc}. Исправьте файл; повтор через 20 сек. Выход — Ctrl+C.')
            time.sleep(20)


def launch(default='settings_1.py'):
    parser = argparse.ArgumentParser()
    parser.add_argument('--settings', default=default)
    parser.add_argument('--check', action='store_true', help='Read-only registration and position check')
    args = parser.parse_args()
    path = Path(args.settings)
    path = path if path.is_absolute() else ROOT / path
    try:
        settings = load_settings(path) if args.check else wait_for_settings(path)
    except KeyboardInterrupt:
        print('Робот остановлен пользователем')
        return
    from gateway_control import ensure_gateway
    if args.check:
        ensure_gateway(settings.REQUESTS_PORT, settings.CALLBACKS_PORT)
        from QuikPy import QuikPy
        with QuikPy(requests_port=settings.REQUESTS_PORT, callbacks_port=settings.CALLBACKS_PORT) as qp:
            result = qp.RegisterBot(settings.BOT_ID, settings.CLASS, settings.SECCODE, settings.BASE_ASSET_CODE, settings.ACCOUNT)
            validate_tick(settings, result['data'])
            print(result)
            print(qp.GetFuturesHoldings())
        return
    execute_bot(settings)

if __name__ == '__main__':
    launch()
