import json, os, time, traceback
from pathlib import Path
from sqlalchemy import update
from .config.database import SessionLocal, engine, Base
from .models import AnalysisRun
from .services.run_service import queue
from .services.analysis_service import execute, recoverable_runs
from .services.audit_service import log
from .constants.status import RunStatus

Base.metadata.create_all(engine)


def _requeue_interrupted():
    """Re-enqueue runs interrupted by a previous worker exit.

    Their shard checkpoints are reused: committed batches are not recomputed and a
    half-written shard resumes from its last committed boundary.
    """
    with SessionLocal() as db:
        for run in recoverable_runs(db):
            if run.status == RunStatus.RUNNING:
                run.status = RunStatus.QUEUED
                db.commit()
            queue.rpush("lift:jobs", run.id)


def _write_artifact(run):
    directory = Path(os.environ.get("ARTIFACTS_DIR", "/artifacts"))
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"run-{run.id}.json").write_text(
        json.dumps(run.result, ensure_ascii=False)
    )


def _process(run_id):
    with SessionLocal() as db:
        # Atomic claim: duplicate queue entries must not run the same analysis
        # concurrently.  Only the worker that flips queued -> running proceeds.
        claimed = db.execute(
            update(AnalysisRun)
            .where(AnalysisRun.id == int(run_id), AnalysisRun.status == RunStatus.QUEUED)
            .values(status=RunStatus.RUNNING)
        ).rowcount == 1
        db.commit()
        if not claimed:
            return
        run = db.get(AnalysisRun, int(run_id))
        try:
            execute(db, run)
            _write_artifact(run)
            log(db, "worker", "analysis.completed", run.id)
        except Exception as error:
            run.status = RunStatus.FAILED
            run.error = str(error)
            traceback.print_exc()
        db.commit()


if __name__ == "__main__":
    Path("/artifacts").mkdir(exist_ok=True)
    _requeue_interrupted()
    while True:
        Path("/artifacts/heartbeat").write_text(str(time.time()))
        job = queue.blpop("lift:jobs", timeout=2)
        if not job:
            continue
        _process(job[1])
