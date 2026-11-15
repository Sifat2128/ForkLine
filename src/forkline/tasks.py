
import time
import subprocess
import sys

from . import schemas


def run_wait(seconds: float) -> None:
    """Wait for the specified number of seconds."""
    time.sleep(seconds)


def run_spawn_child(seconds: float) -> None:
    """Spawn a child process and keep the parent running."""
    child = subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; time.sleep(60)",
        ]
    )

    print(f"Spawned child process with PID {child.pid}", flush=True)

    try:
        time.sleep(seconds)
    finally:
        # Clean up the child if the parent exits normally.
        if child.poll() is None:
            child.terminate()
            child.wait()


TASK_REGISTRY = {
    "wait": {
        "function": run_wait,
        "schema": schemas.WaitTaskInput,
    },
    "spawn_child": {
        "function": run_spawn_child,
        "schema": schemas.WaitTaskInput,
    },
}
