"""Read-only settings, instrument availability and pending-ledger diagnostics."""
import argparse
import json
from pathlib import Path
import re
import sqlite3
from contextlib import closing

from gateway_control import ensure_gateway
from QuikPy import QuikPy
from run_bot import load_settings

ROOT=Path(__file__).resolve().parent


def instrument_status(qp, settings):
    raw=qp.GetClassesList()['data']
    classes=[x.strip() for x in re.split('[,;]',raw or '') if x.strip()] if isinstance(raw,str) else list(raw or [])
    info=qp.GetSecurityInfo(settings.CLASS,settings.SECCODE)['data']
    codes=[]
    if settings.CLASS in classes:
        raw_codes=qp.GetClassSecurities(settings.CLASS)['data']
        codes=[x.strip() for x in re.split('[,;]',raw_codes or '') if x.strip()]
    return dict(bot_id=settings.BOT_ID, class_code=settings.CLASS, sec_code=settings.SECCODE,
                base_asset_code=settings.BASE_ASSET_CODE, connected=qp.IsConnected()['data'],
                available_classes=classes,class_available=settings.CLASS in classes,
                instrument_found=bool(info),
                similar_instruments=[x for x in codes if settings.BASE_ASSET_CODE[:2].upper() in x.upper()])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--settings',default='settings_1.py')
    args=parser.parse_args()
    path=Path(args.settings)
    settings=load_settings(path if path.is_absolute() else ROOT/path)
    ensure_gateway(34990,34991)
    with QuikPy(requests_port=34990,callbacks_port=34991) as qp:
        result=instrument_status(qp,settings)
    db_path=ROOT/'runtime/gateway.sqlite3'
    if db_path.is_file():
        with closing(sqlite3.connect(db_path.as_uri()+'?mode=ro',uri=True)) as db:
            db.row_factory=sqlite3.Row
            result['pending_transactions']=[dict(row) for row in db.execute(
                "SELECT gid,action,state,order_num,created FROM transactions WHERE bot=? AND state IN ('created','sent','uncertain')",
                (settings.BOT_ID,))]
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
