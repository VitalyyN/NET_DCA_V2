"""Inspect, or explicitly close one old unknown NEW_ORDER while the gateway is stopped.

This tool never sends orders. Closing uncertainty is an operator decision, not a
claim that the broker rejected the request. The original row and a backup survive.
"""
import argparse
from contextlib import closing
from datetime import datetime, timezone, timedelta
import getpass
import hashlib
import json
from pathlib import Path
import sqlite3
import time
import uuid

from gateway_control import FileLock

ROOT = Path(__file__).resolve().parent
MOSCOW = timezone(timedelta(hours=3))


def inspect(db_path, bot, gid):
    with closing(sqlite3.connect(Path(db_path).resolve().as_uri()+'?mode=ro', uri=True)) as db:
        db.row_factory = sqlite3.Row
        row = db.execute('SELECT * FROM transactions WHERE gid=? AND bot=?', (gid, bot)).fetchone()
        if row is None:
            raise ValueError('Transaction not found for this bot; no changes made')
        result = dict(row)
        result['created_moscow'] = datetime.fromtimestamp(row['created'], MOSCOW).isoformat()
        result['active_orders_in_registry'] = db.execute('SELECT COUNT(*) FROM orders WHERE active=1').fetchone()[0]
        return result


def validate(db, bot, gid, now):
    row = db.execute('SELECT * FROM transactions WHERE gid=? AND bot=?', (gid, bot)).fetchone()
    if row is None:
        raise ValueError('Transaction not found for this bot')
    if row['state'] != 'uncertain' or row['action'] != 'NEW_ORDER' or row['order_num']:
        raise ValueError('Only an uncertain NEW_ORDER without an order number can be manually closed')
    if now - row['created'] < 24 * 3600:
        raise ValueError('Transaction must be at least 24 hours old')
    if datetime.fromtimestamp(row['created'], MOSCOW).date() >= datetime.fromtimestamp(now, MOSCOW).date():
        raise ValueError('Same-day or future transactions cannot be closed with this tool')
    if db.execute('SELECT COUNT(*) FROM orders WHERE active=1').fetchone()[0]:
        raise ValueError('Registry still contains active orders; reconcile/cancel them in QUIK first')
    for entry in db.execute('SELECT num,payload FROM orders'):
        order = json.loads(entry['payload'])
        if str(order.get('trans_id')) == str(gid) or str(order.get('trans_id_str')) == str(gid):
            raise ValueError(f"Order {entry['num']} matches this transaction; inspect its actual outcome instead")
    return dict(row)


def close_stale(root, bot, gid, reason, acknowledge=False, now=None):
    if not acknowledge or len(reason.strip()) < 10:
        raise ValueError('Explicit --acknowledge-unknown-outcome and a review reason are required')
    root = Path(root).resolve()
    path = root/'runtime/gateway.sqlite3'
    if not path.is_file():
        raise ValueError('Gateway database not found')
    now = time.time() if now is None else now
    # The running gateway owns this same lock, preventing unsafe concurrent writes.
    with FileLock(root/'runtime/gateway.instance.lock', timeout=0):
        with closing(sqlite3.connect(path.as_uri()+'?mode=rw', uri=True)) as db:
            db.row_factory = sqlite3.Row
            before = validate(db, bot, gid, now)
            folder = root/'runtime/recovery'
            folder.mkdir(parents=True, exist_ok=True)
            backup = folder/f'gateway_before_{gid}_{uuid.uuid4().hex}.sqlite3'
            with closing(sqlite3.connect(backup)) as target:
                db.backup(target)
            backup_hash = hashlib.sha256(backup.read_bytes()).hexdigest()
            try:
                db.execute('BEGIN IMMEDIATE')
                # Recheck after taking SQLite's write lock.
                current = validate(db, bot, gid, now)
                if current != before:
                    raise ValueError('Transaction changed during review; inspect it again')
                db.execute("""CREATE TABLE IF NOT EXISTS transaction_resolutions (
                    gid INTEGER PRIMARY KEY, bot TEXT NOT NULL, resolved REAL NOT NULL,
                    operator TEXT NOT NULL, decision TEXT NOT NULL, reason TEXT NOT NULL,
                    before_json TEXT NOT NULL, backup TEXT NOT NULL, backup_sha256 TEXT NOT NULL)""")
                db.execute('INSERT INTO transaction_resolutions VALUES (?,?,?,?,?,?,?,?,?)',
                    (gid, bot, now, getpass.getuser(), 'closed_unknown', reason.strip(),
                     json.dumps(before, ensure_ascii=False), str(backup), backup_hash))
                changed = db.execute("UPDATE transactions SET state='closed_unknown' WHERE gid=? AND bot=? AND state='uncertain'",
                                     (gid, bot)).rowcount
                if changed != 1:
                    raise ValueError('Expected exactly one transaction; operation aborted')
                db.commit()
            except BaseException:
                db.rollback()
                raise
    return dict(bot=bot, gid=gid, state='closed_unknown', backup=str(backup),
                note='Broker outcome remains unknown. No order was sent or retried. Other bots were not modified.')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bot', required=True)
    parser.add_argument('--gid', type=int, required=True)
    parser.add_argument('--close-stale', action='store_true')
    parser.add_argument('--acknowledge-unknown-outcome', action='store_true')
    parser.add_argument('--reason', default='')
    args = parser.parse_args()
    try:
        if args.close_stale:
            result = close_stale(ROOT, args.bot, args.gid, args.reason, args.acknowledge_unknown_outcome)
        else:
            result = inspect(ROOT/'runtime/gateway.sqlite3', args.bot, args.gid)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except (ValueError, sqlite3.Error, OSError, TimeoutError) as exc:
        parser.exit(1, f'{exc}\nStop all bots and stop_gateway.bat before closing a transaction.\n')


if __name__ == '__main__':
    main()
