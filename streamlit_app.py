import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "flood_dashboard"))

exec(open(Path(__file__).parent / "flood_dashboard" / "app.py").read())
