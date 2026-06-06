"""Point d'entrée Streamlit Cloud : lance flood_dashboard/app.py."""
import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).parent / "flood_dashboard" / "app.py"), run_name="__main__")
