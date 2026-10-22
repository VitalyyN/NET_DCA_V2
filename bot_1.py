from pathlib import Path
from run_bot import launch

if __name__ == "__main__":
    launch(Path(__file__).with_name(Path(__file__).stem.replace("bot_", "settings_", 1) + ".py"))
