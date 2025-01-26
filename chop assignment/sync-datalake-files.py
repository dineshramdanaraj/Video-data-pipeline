import time
from pathlib import Path
import json

from jupytext.cli import jupytext as jupytext_cli
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

SOURCE_DIR = Path("datalake")
DEST_DIR = Path("datalake")

def file_type_dict(file: Path) -> dict:
    if file.suffix == ".ipynb":
        ipynb_file = file
        py_file = ipynb_file.with_suffix(".py")
    elif file.suffix == ".py":
        py_file = file
        ipynb_file = py_file.with_suffix(".ipynb")
    else:
        return {}
    
    return {
        "py_file": py_file,
        "ipynb_file": ipynb_file,
    }

def is_valid_notebook(file_path: Path) -> bool:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json.load(f)
        return True
    except json.JSONDecodeError:
        return False

def sync_py_ipynb_file(file: Path):
    if file.name == "__init__.py":
        return

    file_types = file_type_dict(file)
    if not file_types:
        return

    py_file = file_types["py_file"]
    ipynb_file = file_types["ipynb_file"]

    if ipynb_file.exists() and not is_valid_notebook(ipynb_file):
        print(f"Invalid notebook detected: {ipynb_file}. Skipping sync.")
        return

    if py_file.exists() and not ipynb_file.exists():
        cmd = ["--sync", "--set-formats", "py:percent,ipynb", str(py_file)]
    elif ipynb_file.exists() and not py_file.exists():
        cmd = ["--sync", "--set-formats", "py:percent,ipynb", str(ipynb_file)]
    else:
        cmd = ["--sync", "--set-formats", "py:percent,ipynb", str(file)]

    print(f"Syncing: {cmd}")
    try:
        jupytext_cli(cmd)
    except Exception as e:
        print(f"Error syncing {file}: {str(e)}")

class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return

        file_path = Path(event.src_path)
        if event.event_type in ["created", "modified"]:
            sync_py_ipynb_file(file_path)
        elif event.event_type == "moved":
            sync_py_ipynb_file(Path(event.dest_path))
        elif event.event_type == "deleted":
            file_types = file_type_dict(file_path)
            for file in file_types.values():
                try:
                    file.unlink()
                    print(f"Deleted file: {file}")
                except FileNotFoundError:
                    pass
                except Exception as e:
                    print(f"Failed to delete file {file}: {e}")

if __name__ == "__main__":
    for py_file in SOURCE_DIR.rglob("*.py"):
        sync_py_ipynb_file(py_file)
    for ipynb_file in SOURCE_DIR.rglob("*.ipynb"):
        sync_py_ipynb_file(ipynb_file)

    observer = Observer()
    event_handler = MyEventHandler()
    observer.schedule(event_handler, str(SOURCE_DIR), recursive=True)
    observer.start()

    try:
        print(f"Watching for changes in {SOURCE_DIR}...")
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
