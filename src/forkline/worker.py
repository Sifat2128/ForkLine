from forkline.job_service import (claim_next_job, get_job,update_job_status,
update_worker_heartbeat, mark_worker_offline,recover_jobs_from_offline_workers)
import signal
from threading import Event,Thread
import subprocess 
import sys
import os 
import socket 
import time

RECOVERY_INTERVAL = 10
shutdown_event = Event()

def request_shutdown(signal , frame):
    print("\nShutdown requested. Finishing the current job...")
    shutdown_event.set()

def heartbeat_loop(worker_id : str) -> None:
    while not shutdown_event.is_set():
        try:
            update_worker_heartbeat(worker_id)
        # except Exception as exc:
        #     print(f"Heartbeat update failed: {exc}")
        except Exception:
            import traceback
            traceback.print_exc()
        shutdown_event.wait(timeout = 5)

def run_worker() -> None:
    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    print(f"Worker started: {worker_id}")

    # Recover abandoned jobs when the worker starts.
    try:
        recovered_job_ids = recover_jobs_from_offline_workers()

        if recovered_job_ids:
            print(f"Recovered abandoned jobs: {recovered_job_ids}")
        else:
            print("No abandoned jobs to recover.")

    except Exception as exc:
        print(f"Job recovery failed: {exc}")

    print("Worker waiting for jobs...")

    heartbeat_thread = Thread(
        target=heartbeat_loop,
        args=(worker_id,),
        daemon=True,
    )
    heartbeat_thread.start()

    # Track when periodic recovery should run next.
    last_recovery_time = 0.0

    while not shutdown_event.is_set():

        # Periodically detect offline workers and recover their jobs.
        current_time = time.monotonic()

        if current_time - last_recovery_time >= RECOVERY_INTERVAL:
            try:
                recovered_job_ids = recover_jobs_from_offline_workers()

                if recovered_job_ids:
                    print(
                        f"Recovered abandoned jobs: {recovered_job_ids}"
                    )

            except Exception as exc:
                print(f"Periodic job recovery failed: {exc}")

            last_recovery_time = time.monotonic()

        # Claim the next available job.
        job_id = claim_next_job(worker_id)

        if job_id is None:
            shutdown_event.wait(timeout=1)
            continue

        job = get_job(job_id)

        if job is None:
            print(f"Error: claimed job {job_id} could not be found.")
            continue

        print(f"Worker claimed job {job.id}: {job.task_name}")

        try:
            result = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "forkline.task_runner",
                    job.task_name,
                    job.payload or "{}",
                ],
                start_new_session=True,
            )

            try:
                return_code = result.wait(timeout=10)

            except subprocess.TimeoutExpired:
                print(f"Job {job.id} exceeded its time limit.")

                # Request graceful termination of the process group.
                try:
                    os.killpg(result.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

                # Allow 3 seconds for graceful shutdown.
                try:
                    result.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    pass

                # Force-kill any remaining processes in the group.
                try:
                    os.killpg(result.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

                # Reap the direct child process.
                result.wait()

                update_job_status(job.id, "TIMED_OUT",worker_id)
                print(f"Job {job.id} timed out.")

            else:
                if return_code != 0:
                    raise RuntimeError(
                        f"Task process exited with code {return_code}"
                    )

                update_job_status(job.id, "COMPLETED",worker_id)
                print(f"Job {job.id} completed successfully.")

        except Exception as exc:
            try:
                update_job_status(job.id, "FAILED",worker_id)
            except Exception as status_exc:
                print(
                    f"Could not mark job {job.id} as FAILED: {status_exc}"
                )

            print(f"Job {job.id} failed: {exc}")

    # Graceful shutdown.
    shutdown_event.set()
    heartbeat_thread.join(timeout=2)

    try:
        mark_worker_offline(worker_id)
        print(f"Worker {worker_id} marked OFFLINE.")
    except Exception as exc:
        print(f"Could not mark worker offline: {exc}")

    print("Worker stopped.")


if __name__ == "__main__":
    run_worker()