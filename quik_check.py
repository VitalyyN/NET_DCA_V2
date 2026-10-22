"""Terminal inventory and bot-profile validation; this command never sends transactions."""
import argparse
import json
from pathlib import Path

from QuikPy import QuikPy
from run_bot import load_settings, validate_tick
from gateway_control import ensure_gateway

ROOT = Path(__file__).resolve().parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', action='store_true',
                        help='Show accounts and available FUTSPREAD codes without bot registration')
    parser.add_argument('--class-code', default='FUTSPREAD',
                        help='Instrument class shown by --inventory')
    args = parser.parse_args()
    ensure_gateway(34130, 34131)
    with QuikPy() as qp:
        metadata = {}
        for key in ('VERSION', 'SERVER', 'TRADEDATE', 'SERVERTIME'):
            metadata[key] = qp.GetInfoParam(key)['data']
        metadata['connected'] = qp.IsConnected()['data']
        print(json.dumps({'terminal': metadata}, ensure_ascii=False, indent=2))
        if args.inventory:
            inventory = {
                'terminal': metadata,
                'classes': qp.GetClassesList()['data'],
                'accounts': qp.GetTradeAccounts()['data'],
                'class_code': args.class_code,
                'instruments': qp.GetClassSecurities(args.class_code)['data'],
                'holdings': qp.GetFuturesHoldings()['data']}
            print(json.dumps(inventory, ensure_ascii=False, indent=2))
            runtime = ROOT/'runtime'
            runtime.mkdir(exist_ok=True)
            (runtime/'quik_inventory.json').write_text(
                json.dumps(inventory, ensure_ascii=False, indent=2), encoding='utf-8')
            return
        if metadata['connected'] != 1:
            raise RuntimeError('Connect QUIK to its server first')
    results = []
    files = sorted(ROOT.glob('settings_[0-9]*.py'), key=lambda p: int(p.stem.split('_')[1]))
    for path in files:
        settings = load_settings(path)
        ensure_gateway(settings.REQUESTS_PORT, settings.CALLBACKS_PORT)
        with QuikPy(requests_port=settings.REQUESTS_PORT,
                    callbacks_port=settings.CALLBACKS_PORT) as qp:
            result = qp.RegisterBot(
                settings.BOT_ID, settings.CLASS, settings.SECCODE,
                settings.BASE_ASSET_CODE, settings.ACCOUNT)['data']
            validate_tick(settings, result)
            result['bot_id'] = settings.BOT_ID
            result['position_records'] = qp.GetFuturesHoldings()['data']
            results.append(result)
            print(json.dumps(result, ensure_ascii=False, indent=2))
    runtime = ROOT / 'runtime'
    runtime.mkdir(exist_ok=True)
    (runtime / 'quik_check.json').write_text(
        json.dumps({'terminal': metadata, 'bots': results}, ensure_ascii=False, indent=2),
        encoding='utf-8')


if __name__ == '__main__':
    main()
