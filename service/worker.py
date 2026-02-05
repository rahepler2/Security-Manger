import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from sqlalchemy.exc import SQLAlchemyError
from .db import SessionLocal
from .models import Job, JobStatus, Batch, BatchStatus
from .storage import upload_file
from .config import SCAN_WORKERS
# Import processing.TrustGateway (after you move your existing code there)
from .processing import TrustGateway

POOL = ThreadPoolExecutor(max_workers=SCAN_WORKERS)
GATEWAY = TrustGateway()

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
    # schedule async execution
    POOL.submit(_run_job, job_id)
    return job_id

def _run_job(job_id: str):
    db = SessionLocal()
    try:
        job = db.query(Job).get(job_id)
        if not job:
            return
        job.status = JobStatus.running
        job.started_at = datetime.utcnow()
        job.attempts = (job.attempts or 0) + 1
        db.add(job); db.commit(); db.refresh(job)

        # Call existing processing logic (returns verdict, report_path)
        verdict, report_path = GATEWAY.process_package(job.package, job.version)

        # Upload report
        key = f"reports/{job.id}/{os.path.basename(report_path)}"
        report_url = upload_file(str(report_path), key) if report_path.exists() else None

        # Persist result summary
        job.result = {"verdict": verdict.value}
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
        rows = db.query(Job).filter(Job.status.in_([JobStatus.submitted, JobStatus.running])).all()
        for j in rows:
            POOL.submit(_run_job, j.id)
    finally:
        db.close()
