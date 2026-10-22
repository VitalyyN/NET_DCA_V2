"""Hidden gateway entry point with bounded startup-error logging."""
import logging
from pathlib import Path
import sys

from log_policy import cleanup_logs, make_handler

ROOT = Path(__file__).resolve().parent


def main():
    cleanup_logs(ROOT)
    logger = logging.getLogger('netdca.startup')
    handler = make_handler(ROOT/'runtime'/'gateway_startup.log')
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    try:
        from gateway import main as run_gateway
        run_gateway()
        return 0
    except Exception:
        logger.exception('Gateway failed')
        return 1
    finally:
        logger.removeHandler(handler)
        handler.close()


if __name__ == '__main__':
    sys.exit(main())

