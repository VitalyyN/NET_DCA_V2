"""Persistent transaction ownership; broker history remains on disk."""
import json
import sqlite3
import threading
import time

from transport import RemoteError, TransactionUncertain


def matches(data, profile):
    return (data.get('class_code') == profile['class_code']
            and data.get('sec_code') == profile['sec_code']
            and data.get('account', data.get('trdaccid')) == profile['account'])


class Registry:
    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.changed = threading.Condition(self.lock)
        self.sequence = 0
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS transactions (
                gid INTEGER PRIMARY KEY, bot TEXT NOT NULL, client_id TEXT NOT NULL,
                action TEXT NOT NULL, state TEXT NOT NULL, order_num TEXT,
                reply TEXT, created REAL NOT NULL);
            CREATE INDEX IF NOT EXISTS tx_client ON transactions(bot,client_id,gid);
            CREATE INDEX IF NOT EXISTS tx_order ON transactions(order_num);
            CREATE TABLE IF NOT EXISTS orders (
                num TEXT PRIMARY KEY, bot TEXT, payload TEXT NOT NULL,
                active INTEGER NOT NULL, seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS last_trades (bot TEXT PRIMARY KEY, payload TEXT NOT NULL);
        """)
        columns = {row[1] for row in self.db.execute('PRAGMA table_info(transactions)')}
        if 'session' not in columns:
            self.db.execute("ALTER TABLE transactions ADD COLUMN session TEXT NOT NULL DEFAULT ''")
        if 'visibility_checked' not in columns:
            self.db.execute('ALTER TABLE transactions ADD COLUMN visibility_checked INTEGER NOT NULL DEFAULT 0')
            self.db.execute("""UPDATE transactions SET visibility_checked=1
                WHERE action='NEW_ORDER' AND EXISTS
                (SELECT 1 FROM orders o WHERE o.num=transactions.order_num AND o.bot=transactions.bot)""")
        self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS tx_session ON transactions(bot,session,client_id) WHERE session != ''")
        self.db.execute('CREATE INDEX IF NOT EXISTS tx_visibility ON transactions(bot,action,state,visibility_checked)')
        self.sequence = self.db.execute('SELECT COALESCE(MAX(seq),0) FROM orders').fetchone()[0]
        self.db.commit()

    def allocate(self, bot, client_id, action, order_num=None, session=""):
        with self.lock:
            if session and self.db.execute(
                    'SELECT 1 FROM transactions WHERE bot=? AND session=? AND client_id=?',
                    (bot, session, str(client_id))).fetchone():
                raise ValueError('TRANS_ID already submitted in this connection; do not replay')
            last = self.db.execute('SELECT MAX(gid) FROM transactions').fetchone()[0]
            gid = max(last or 0, 1000000000) + 1
            if gid > 2147483647:
                raise RuntimeError('Transaction id range exhausted')
            self.db.execute('INSERT INTO transactions (gid,bot,client_id,action,state,order_num,reply,created,session) VALUES (?,?,?,?,?,?,?,?,?)',
                            (gid, bot, str(client_id), action, 'created',
                             str(order_num) if order_num else None, None, time.time(), session))
            self.db.commit()
            return gid

    def transaction(self, gid):
        with self.lock:
            return self.db.execute('SELECT * FROM transactions WHERE gid=?', (str(gid),)).fetchone()

    def client_transaction(self, bot, client_id, session):
        with self.lock:
            return self.db.execute('SELECT * FROM transactions WHERE bot=? AND client_id=? AND session=? ORDER BY gid DESC LIMIT 1',
                (bot,str(client_id),session)).fetchone()

    def order(self, number):
        with self.lock:
            row = self.db.execute('SELECT payload FROM orders WHERE num=?',(str(number),)).fetchone()
            return json.loads(row['payload']) if row else None

    def missing_orders(self, bot, limit=128):
        """Accepted NEW_ORDER without an observed owned order is not an empty grid."""
        with self.lock:
            return self.db.execute("""SELECT t.* FROM transactions t
                WHERE t.bot=? AND t.action='NEW_ORDER' AND t.state='accepted'
                AND t.visibility_checked=0
                ORDER BY t.gid LIMIT ?""",(bot,limit)).fetchall()

    def unverify(self, number):
        with self.lock:
            self.db.execute("UPDATE transactions SET visibility_checked=0 WHERE order_num=? AND action='NEW_ORDER'",(str(number),))
            self.db.commit()

    def owner(self, num):
        with self.lock:
            row = self.db.execute('SELECT bot FROM orders WHERE num=?', (str(num),)).fetchone()
            if row and row['bot']:
                return row['bot']
            row = self.db.execute("SELECT bot FROM transactions WHERE order_num=? AND state='accepted' ORDER BY gid DESC LIMIT 1", (str(num),)).fetchone()
            return row['bot'] if row else None

    def mark(self, gid, state, reply=None):
        with self.changed:
            current = self.transaction(gid)
            if current and current['state'] not in ('accepted', 'rejected'):
                self.db.execute('UPDATE transactions SET state=?, reply=COALESCE(?,reply) WHERE gid=?',
                                (state, json.dumps(reply) if reply else None, gid))
                self.db.commit()
                self.changed.notify_all()

    def on_reply(self, data):
        gid = data.get('trans_id')
        row = self.transaction(gid)
        if row is None:
            return None
        status = int(data.get('status', -1))
        if status >= 3:
            with self.changed:
                number = data.get('order_num')
                self.db.execute('UPDATE transactions SET state=?, reply=?, order_num=COALESCE(?,order_num) WHERE gid=?',
                                ('accepted' if status == 3 else 'rejected', json.dumps(data),
                                 str(number) if number else None, gid))
                self.db.commit()
                self.changed.notify_all()
        return row

    def _put_order(self, data, profiles, sequence):
        num = str(data['order_num'])
        owner = self.owner(num)
        tx = self.transaction(data.get('trans_id', -1))
        if not owner and tx and tx['action'] == 'NEW_ORDER':
            profile = profiles.get(tx['bot'])
            if profile and matches(data, profile):
                owner = tx['bot']
                self.db.execute('UPDATE transactions SET order_num=? WHERE gid=?', (num, tx['gid']))
                self.mark(tx['gid'], 'accepted',
                          {'status': 3, 'order_num': data['order_num'], 'trans_id': tx['gid']})
        if owner in profiles and not matches(data,profiles[owner]):
            self.db.execute('UPDATE transactions SET visibility_checked=0 WHERE order_num=? AND bot=?', (num,owner))
            owner = None  # A known transaction never authorizes a different account.
        if owner in profiles and matches(data,profiles[owner]):
            self.db.execute("UPDATE transactions SET visibility_checked=1 WHERE order_num=? AND bot=? AND action='NEW_ORDER'",(num,owner))
        active = int(bool(int(data.get('flags', 0)) & 1))
        self.db.execute('INSERT OR REPLACE INTO orders VALUES (?,?,?,?,?)',
                        (num, owner, json.dumps(data), active, sequence))
        return owner

    def on_order(self, data, profiles):
        with self.changed:
            self.sequence += 1
            owner = self._put_order(data, profiles, self.sequence)
            self.db.commit()
            self.changed.notify_all()
            return owner

    def snapshot(self, data, profile, profiles, watermark):
        with self.changed:
            seen = {str(item['order_num']) for item in data}
            stored = self.db.execute('SELECT * FROM orders WHERE active=1').fetchall()
            for row in stored:
                if row['seq'] <= watermark and row['num'] not in seen and matches(json.loads(row['payload']), profile):
                    self.db.execute('UPDATE orders SET active=0 WHERE num=?', (row['num'],))
            for item in data:
                previous = self.db.execute('SELECT seq FROM orders WHERE num=?', (str(item['order_num']),)).fetchone()
                if not previous or previous['seq'] <= watermark:
                    self._put_order(item, profiles, watermark)
            self.db.commit()
            self.changed.notify_all()

    def active(self, profile, owned=True):
        with self.lock:
            result = []
            for row in self.db.execute('SELECT * FROM orders WHERE active=1'):
                data = json.loads(row['payload'])
                belongs = row['bot'] == profile['bot_id']
                if matches(data, profile) and (belongs if owned else not belongs):
                    result.append(data)
            return result

    def unresolved(self, bot):
        with self.lock:
            return self.db.execute("SELECT COUNT(*) FROM transactions WHERE bot=? AND state IN ('created','sent','uncertain')", (bot,)).fetchone()[0]

    def wait(self, bot, client_id, timeout, session=""):
        deadline = time.monotonic() + min(max(float(timeout), 0), 8)
        with self.changed:
            while True:
                row = self.db.execute('SELECT * FROM transactions WHERE bot=? AND client_id=? AND session=? ORDER BY gid DESC LIMIT 1', (bot, str(client_id), session)).fetchone()
                if row is None:
                    raise ValueError('Unknown transaction')
                if row['state'] == 'accepted':
                    return json.loads(row['reply']) if row['reply'] else {'status': 3, 'order_num': row['order_num']}
                if row['state'] == 'rejected':
                    raise RemoteError(dict(cmd='gateway_error', error=str(json.loads(row['reply'])), data=json.loads(row['reply'])))
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.mark(row['gid'], 'uncertain')
                    raise TransactionUncertain(dict(cmd='transaction_uncertain', error='Broker confirmation missing; transaction must not be replayed'))
                self.changed.wait(remaining)

    def on_trade(self, data):
        owner = self.owner(data.get('order_num'))
        if owner is None:
            return None
        with self.lock:
            old = self.db.execute('SELECT payload FROM last_trades WHERE bot=?', (owner,)).fetchone()
            if not old or int(data.get('trade_num', 0)) >= int(json.loads(old['payload']).get('trade_num', 0)):
                self.db.execute('INSERT OR REPLACE INTO last_trades VALUES (?,?)', (owner, json.dumps(data)))
                self.db.commit()
        return owner

    def trades(self, profile):
        with self.lock:
            row = self.db.execute('SELECT payload FROM last_trades WHERE bot=?', (profile['bot_id'],)).fetchone()
            value = json.loads(row['payload']) if row else None
            return [value] if value and matches(value, profile) else []

    def close(self):
        with self.lock:
            self.db.close()
