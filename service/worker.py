import time
from concurrent.futures import ThreadPoolExecutor
from .db import SessionLocal
from .models import Job, JobStatus
from .processing import process_package  # your existing logic refactored to return (verdict, report_path, trivy_json)
from .storage import upload_file
from sqlalchemy.exc import SQLAlchemyError
import os
from datetime import datetime

POOL = ThreadPoolExecutor(max_workers=int(os.getenv("SCAN_WORKERS", "3")))

def submit_job(package: str, version: str, batch_id: str | None = None) -> str:
    db = SessionLocal()
    try:
        job = Job(package=package, version=version, status=JobStatus.submitted, batch_id=batch_id)
        db.add(job)
        db.commit()
        db.refresh(job)
        job_id = job.id
    finally:
        db.close()
    # schedule execution asynchronously
    POOL.submit(_run_job, job_id)
    return job_id

def _run_job(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(Job).get(job_id)
        if not job:
            return
        # update to running
        job.status = JobStatus.running
        job.started_at = datetime.utcnow()
        job.attempts = job.attempts + 1
        db.add(job); db.commit(); db.refresh(job)

        # run the existing processing logic (returns verdict, report_path, details)
        verdict, report_path, details = process_package(job.package, job.version)

        # upload report to minio
        key = f"reports/{job.id}/{os.path.basename(report_path)}"
        report_url = upload_file(str(report_path), key)

        # persist result
        job.result = {"verdict": verdict.value, "details": details}
        job.report_url = report_url
        job.status = JobStatus.done if verdict == verdict.PASS else JobStatus.failed
        job.finished_at = datetime.utcnow()
        db.add(job); db.commit()
    except Exception as e:
        db.rollback()
        job = db.query(Job).get(job_id)
        if job:
            job.status = JobStatus.failed
            job.result = {"error": str(e)}
            job.finished_at = datetime.utcnow()
            db.add(job); db.commit()
    finally:
        db.close()

def recover_and_resubmit_running_jobs():
    db = SessionLocal()
    try:
        # Jobs that were running/submitted at prior run should be requeued
        rows = db.query(Job).filter(Job.status.in_([JobStatus.submitted, JobStatus.running])).all()
        for j in rows:
            # Avoid resubmitting jobs that have many attempts; you can implement backoff
            POOL.submit(_run_job, j.id)
    finally:
        db.close()
