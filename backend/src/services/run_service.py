import csv, io
from fastapi import HTTPException
from redis import Redis
from ..config.settings import REDIS_URL
from ..models import AnalysisRun, Experiment
from ..utils.serialize import record
from .analysis_service import live_progress
from .audit_service import log
queue = Redis.from_url(REDIS_URL, decode_responses=True)

# A run in one of these states is still owned by the queue/worker; re-submitting
# the same experiment returns it instead of creating a duplicate analysis.
IN_FLIGHT = ("queued", "running")


def list_runs(db):
    return [record(x) for x in list_run_rows(db)]


def list_run_rows(db):
    return db.query(AnalysisRun).order_by(AnalysisRun.id.desc()).all()


def get_run(db, rid):
    row = db.get(AnalysisRun, rid)
    if not row:
        raise HTTPException(404, "运行记录不存在")
    return row


def enqueue(db, eid, user):
    if not db.get(Experiment, eid):
        raise HTTPException(404, "实验不存在")
    existing = (
        db.query(AnalysisRun)
        .filter(AnalysisRun.experiment_id == eid, AnalysisRun.status.in_(IN_FLIGHT))
        .order_by(AnalysisRun.id.desc())
        .first()
    )
    if existing:
        return record(existing)
    row = AnalysisRun(experiment_id=eid, status="queued")
    db.add(row)
    db.flush()
    log(db, user.email, "analysis.enqueue", row.id)
    db.commit()
    try:
        queue.rpush("lift:jobs", row.id)
    except Exception:
        row.status = "failed"
        row.error = "队列不可用，请稍后重新运行"
        db.commit()
        raise HTTPException(503, row.error)
    return record(row)


def run_output(db, row):
    """Serializable run plus live progress; old completed runs fall back to result."""
    payload = record(row)
    stored = (row.result or {}).get("progress")
    if row.status == "completed":
        payload["progress"] = stored or _legacy_progress(row)
    elif row.status in IN_FLIGHT:
        payload["progress"] = live_progress(db, row)
    else:
        payload["progress"] = stored or {}
    return payload


def _legacy_progress(row):
    """Runs completed before checkpoints existed have no stored progress.

    They were computed over the whole dataset, so report a finished summary
    derived from the sample counts already present in the result.
    """
    result = row.result or {}
    raw = result.get("raw") or {}
    n_total = int(raw.get("n_control", 0)) + int(raw.get("n_treatment", 0))
    return {
        "total_observations": n_total,
        "processed_observations": n_total,
        "shards_total": 0,
        "shards_completed": 0,
        "percent": 1.0 if n_total else 0.0,
        "batch_size": 0,
        "shard_size": 0,
        "finished": True,
    }


def export_run(db, rid):
    row = get_run(db, rid)
    if row.status != "completed":
        raise HTTPException(409, "运行尚未完成")
    out = io.StringIO()
    w = csv.writer(out)
    progress = (row.result or {}).get("progress") or {}
    w.writerow(
        [
            "method",
            "n_control",
            "n_treatment",
            "n_total",
            "progress_percent",
            "processed_observations",
            "difference",
            "p_value",
            "ci_low",
            "ci_high",
        ]
    )
    for method in ("raw", "cuped"):
        result = (row.result or {}).get(method)
        if result:
            n_total = result["n_control"] + result["n_treatment"]
            w.writerow(
                [method]
                + [result[k] for k in ["n_control", "n_treatment"]]
                + [
                    n_total,
                    progress.get("percent", 1.0),
                    progress.get("processed_observations", n_total),
                ]
                + [result[k] for k in ["difference", "p_value", "ci_low", "ci_high"]]
            )
    return out.getvalue()
