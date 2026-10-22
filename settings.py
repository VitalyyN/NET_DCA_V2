from pathlib import Path
from run_bot import load_settings

_config = load_settings(Path(__file__).with_name("settings_1.py"))
globals().update({key: value for key, value in vars(_config).items() if key.isupper()})
