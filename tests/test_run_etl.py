import os
import sys
import types
import json
import importlib.util
from pathlib import Path

# Stub optional dependencies used by run_etl when importing
if "tkinter" not in sys.modules:
    tk = types.ModuleType("tkinter")
    tk.Tk = object
    tk.Label = object
    tk.Entry = object
    tk.Button = object
    tk.Checkbutton = object
    tk.Frame = object
    tk.BooleanVar = object
    tk.StringVar = object
    tk.scrolledtext = types.SimpleNamespace(ScrolledText=object)
    tk.messagebox = types.SimpleNamespace(showerror=lambda *a, **k: None,
                                          showinfo=lambda *a, **k: None)
    tk.filedialog = types.SimpleNamespace(askdirectory=lambda *a, **k: "")
    sys.modules["tkinter"] = tk
    sys.modules["tkinter.messagebox"] = tk.messagebox
    sys.modules["tkinter.scrolledtext"] = tk.scrolledtext
    sys.modules["tkinter.filedialog"] = tk.filedialog

if "pyodbc" not in sys.modules:
    class _DummyError(Exception):
        pass
    sys.modules["pyodbc"] = types.SimpleNamespace(Error=_DummyError, connect=lambda *a, **k: None)


def _import_run_etl_from_repo(tmp_cwd):
    """Import run_etl.py as if executed from a different directory."""
    run_etl_path = Path(__file__).resolve().parents[1] / "run_etl.py"
    spec = importlib.util.spec_from_file_location("run_etl", run_etl_path)
    module = importlib.util.module_from_spec(spec)
    cwd = os.getcwd()
    os.chdir(tmp_cwd)
    try:
        spec.loader.exec_module(module)  # type: ignore
    finally:
        os.chdir(cwd)
    return module


def test_load_config_from_other_directory(tmp_path):
    run_etl = _import_run_etl_from_repo(tmp_path)

    # Ensure CONFIG_FILE is absolute and points to the repo config directory
    assert os.path.isabs(run_etl.CONFIG_FILE)
    assert run_etl.CONFIG_FILE.endswith(os.path.join("config", "values.json"))

    # Write a sample config file at the expected location
    os.makedirs(os.path.dirname(run_etl.CONFIG_FILE), exist_ok=True)
    with open(run_etl.CONFIG_FILE, "w") as f:
        json.dump({"driver": "dummy"}, f)

    config = run_etl.App._load_config(object())
    assert config.get("driver") == "dummy"


def test_save_config_writes_absolute_path(tmp_path):
    run_etl = _import_run_etl_from_repo(tmp_path)

    class DummyVar:
        def __init__(self, value):
            self._v = value
        def get(self):
            return self._v

    dummy_app = types.SimpleNamespace(
        entries={name: types.SimpleNamespace(get=lambda n=name: f"val_{n}")
                 for name in ["driver", "server", "database", "user", "password"]},
        csv_dir_var=DummyVar("/tmp/csv"),
        include_empty_var=DummyVar(True),
    )

    run_etl.App._save_config(dummy_app)
    assert os.path.exists(run_etl.CONFIG_FILE)
    with open(run_etl.CONFIG_FILE) as f:
        data = json.load(f)
    assert data["csv_dir"] == "/tmp/csv"
