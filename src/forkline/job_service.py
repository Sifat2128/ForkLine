
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel
from sqlalchemy import exists, select

from forkline.database import SessionLocal
from . import models

def submit_job(task_name: str, task_input: BaseModel) -> int:
    with SessionLocal() as db:
        try:
            job = models.Job(
                task_name=task_name,
                payload=task_input.model_dump_json(),
            )

            db.add(job)
            db.commit()
            db.refresh(job)

            return job.id

        except Exception:
            db.rollback()
            raise


def list_jobs(limit: int = 20) -> list[models.Job]:
    with SessionLocal() as db:
        return (
            db.query(models.Job)
            .order_by(models.Job.id.desc())
            .limit(limit)
            .all()
        )


def get_job(job_id: int) -> models.Job | None:
    with SessionLocal() as db:
        return (
            db.query(models.Job)
            .filter(models.Job.id == job_id)
            .first()
        )


def cancel_queued_job(job_id: int) -> bool:
    with SessionLocal() as db:
        try:
            result = (
                db.query(models.Job)
                .filter(models.Job.id == job_id)
                .update({models.Job.status :"CANCELLED"},synchronize_session = False)
            )
            db.commit()

            return result == 1

        except Exception:
            db.rollback()
            raise


def claim_next_job(worker_id : str) -> int | None:
    with SessionLocal() as db:
        try:
            statement = (
                select(models.Job)
                .where(models.Job.status == "QUEUED")
                .order_by(models.Job.id.asc())
                .with_for_update(skip_locked=True)
                .limit(1)
            )

            job = db.execute(statement).scalar_one_or_none()
            
            if job is None:
                return None

            job.status = "RUNNING"
            job.started_at = datetime.now(timezone.utc)
            job.worker_id = worker_id
            db.commit()

            return job.id

        except Exception:
            db.rollback()
            raise



def update_job_status(job_id: int,status: str,worker_id: str) -> None:

    allowed_statuses = {"COMPLETED", "FAILED", "TIMED_OUT"}

    if status not in allowed_statuses:
        raise ValueError(f"Invalid final status: {status}")

    with SessionLocal() as db:
        try:
            result = (
                db.query(models.Job)
                .filter(models.Job.id == job_id)
                .filter(models.Job.status == "RUNNING")
                .filter(models.Job.worker_id == worker_id)
                .update(
                    {models.Job.status: status},
                    synchronize_session=False,
                )
            )

            if result != 1:
                raise ValueError(
                    f"Cannot update job {job_id}: it is not RUNNING "
                    f"or worker {worker_id} does not own it"
                )

            db.commit()

        except Exception:
            db.rollback()
            raise



def update_worker_heartbeat(worker_id: str) -> None:
    with SessionLocal() as db:
        try:
            worker = db.get(models.Worker, worker_id)

            if worker is None:
                worker = models.Worker(
                    id=worker_id,
                    last_heartbeat=datetime.now(timezone.utc),
                    status="ACTIVE",
                )
                db.add(worker)
            else:
                worker.last_heartbeat = datetime.now(timezone.utc)
                worker.status = "ACTIVE"

            db.commit()

        except Exception:
            db.rollback()
            raise

def mark_worker_offline(worker_id: str) -> None:
    with SessionLocal() as db:
        try:
            worker = db.get(models.Worker, worker_id)

            if worker is not None:
                worker.status = "OFFLINE"
                db.commit()

        except Exception:
            db.rollback()
            raise

def mark_stale_workers_offline(timeout_seconds : int = 15)-> list[str]:
    cutoff_time = datetime.now(timezone.utc)-timedelta(seconds = timeout_seconds)
    with SessionLocal() as db :
        try:
          stale_workers = db.query(models.Worker).filter(models.Worker.status == "ACTIVE",models.Worker.last_heartbeat < cutoff_time).all()
          stale_ids = [worker.id for worker in stale_workers]
          for worker in stale_workers:
              worker.status = "OFFLINE"
          db.commit()
          return stale_ids
        except Exception:
            db.rollback()
            raise 






def recover_jobs_from_offline_workers() -> list[int]:
    """Requeue running jobs assigned to offline workers safely."""
    mark_stale_workers_offline(timeout_seconds=15)

    with SessionLocal() as db:
        try:
            stuck_jobs = (
                db.query(models.Job.id, models.Job.worker_id)
                .join(
                    models.Worker,
                    models.Job.worker_id == models.Worker.id,
                )
                .filter(models.Job.status == "RUNNING")
                .filter(models.Worker.status == "OFFLINE")
                .with_for_update(skip_locked=True)
                .all()
            )

            recovered_job_ids = []

            for job_id, expected_worker_id in stuck_jobs:
                # Recheck job status, ownership, and worker status
                # inside the same UPDATE statement.
                offline_worker_exists = exists().where(
                    models.Worker.id == expected_worker_id,
                    models.Worker.status == "OFFLINE",
                )

                result = (
                    db.query(models.Job)
                    .filter(models.Job.id == job_id)
                    .filter(models.Job.status == "RUNNING")
                    .filter(
                        models.Job.worker_id == expected_worker_id
                    )
                    .filter(offline_worker_exists)
                    .update(
                        {
                            models.Job.status: "QUEUED",
                            models.Job.worker_id: None,
                            models.Job.started_at: None,
                        },
                        synchronize_session=False,
                    )
                )

                if result == 1:
                    recovered_job_ids.append(job_id)

            db.commit()
            return recovered_job_ids

        except Exception:
            db.rollback()
            raise