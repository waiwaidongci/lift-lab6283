"""Chunked, checkpointed analysis execution.

Observations are never loaded as a whole.  A dataset is partitioned into fixed
shards by observation id, and each shard is accumulated in batches whose boundary
is persisted.  Shard and batch identity depend only on observation id, so results
are independent of batch size and processing order.

Each checkpoint advance is a compare-and-swap on ``analysis_shards.last_id`` under
the row's write lock.  If the same shard is processed twice, concurrently or after
a crash, the loser's local batch is discarded and it resumes from the committed
boundary: every observation is counted exactly once.
"""
from sqlalchemy import func, update
from sqlalchemy.exc import IntegrityError

from ..config.settings import ANALYSIS_BATCH_SIZE, ANALYSIS_SHARD_SIZE
from ..models import AnalysisRun, AnalysisShard, Experiment, Observation
from ..utils.clock import now
from .statistics_service import (
    Accumulator,
    decode_state,
    encode_state,
    finalize,
    merge_states,
)


def _bucket_range(index):
    """Inclusive observation-id range for one fixed shard."""
    return index * ANALYSIS_SHARD_SIZE + 1, (index + 1) * ANALYSIS_SHARD_SIZE


def _shard_bounds(db, dataset_id):
    """First and last shard index for a dataset, or ``(None, None)`` when empty."""
    minimum, maximum = (
        db.query(func.min(Observation.id), func.max(Observation.id))
        .filter(Observation.dataset_id == dataset_id)
        .one()
    )
    if minimum is None:
        return None, None
    return (minimum - 1) // ANALYSIS_SHARD_SIZE, (maximum - 1) // ANALYSIS_SHARD_SIZE


def expected_shard_count(db, dataset_id):
    first, last = _shard_bounds(db, dataset_id)
    return 0 if first is None else last - first + 1


def observation_count(db, dataset_id):
    return int(
        db.query(func.count(Observation.id))
        .filter(Observation.dataset_id == dataset_id)
        .scalar()
        or 0
    )


def ensure_shards(db, run_id, dataset_id):
    """Return this run's fixed shard rows, creating them on first use.

    Once a run has shards their boundaries are authoritative; a restart with a
    different configured shard size must not create overlapping buckets.
    """
    existing = (
        db.query(AnalysisShard)
        .filter(AnalysisShard.run_id == run_id)
        .order_by(AnalysisShard.shard_index)
        .all()
    )
    if existing:
        return existing
    first, last = _shard_bounds(db, dataset_id)
    if first is None:
        return []
    for index in range(first, last + 1):
        # Savepoint: a concurrent worker may insert the same fixed shard.
        try:
            with db.begin_nested():
                db.add(
                    AnalysisShard(
                        run_id=run_id,
                        shard_index=index,
                        lower_id=_bucket_range(index)[0],
                        upper_id=_bucket_range(index)[1],
                        state=encode_state({}),
                        last_id=0,
                        rows=0,
                        completed=False,
                    )
                )
        except IntegrityError:
            pass
    db.commit()
    return (
        db.query(AnalysisShard)
        .filter(AnalysisShard.run_id == run_id)
        .order_by(AnalysisShard.shard_index)
        .all()
    )


def _checkpoint(db, run_id, shard_index, expected_last, accumulator, last_id, rows_added, completed):
    """Persist a batch boundary only if the shard has not moved on.

    Returns True when this process won the compare-and-swap.  A False result means
    another process already counted these observations, so the local batch must be
    discarded rather than merged.
    """
    result = db.execute(
        update(AnalysisShard)
        .where(
            AnalysisShard.run_id == run_id,
            AnalysisShard.shard_index == shard_index,
            AnalysisShard.last_id == expected_last,
            AnalysisShard.completed.is_(False),
        )
        .values(
            last_id=last_id,
            state=encode_state(accumulator.state),
            rows=AnalysisShard.rows + rows_added,
            completed=completed,
            updated_at=now(),
        )
    )
    db.commit()
    return result.rowcount == 1


def process_shard(db, run_id, dataset_id, shard_index):
    """Advance one shard to completion, committing at every batch boundary."""
    while True:
        shard = (
            db.query(AnalysisShard)
            .filter_by(run_id=run_id, shard_index=shard_index)
            .one()
        )
        lo, hi = int(shard.lower_id), int(shard.upper_id)
        snapshot = decode_state(shard.state)
        if shard.completed:
            return
        batch = (
            db.query(
                Observation.id,
                Observation.arm,
                Observation.pre_value,
                Observation.value,
            )
            .filter(
                Observation.dataset_id == dataset_id,
                Observation.id >= lo,
                Observation.id <= hi,
                Observation.id > shard.last_id,
            )
            .order_by(Observation.id)
            .limit(ANALYSIS_BATCH_SIZE)
            .all()
        )
        if not batch:
            # No rows left: close the shard at the boundary we just observed.
            won = _checkpoint(
                db,
                run_id,
                shard_index,
                shard.last_id,
                Accumulator(snapshot),
                shard.last_id,
                0,
                True,
            )
            if won:
                return
            continue

        accumulator = Accumulator(snapshot)
        for observation_id, arm, pre_value, value in batch:
            accumulator.add(arm, pre_value, value)
        finished = len(batch) < ANALYSIS_BATCH_SIZE
        won = _checkpoint(
            db,
            run_id,
            shard_index,
            shard.last_id,
            accumulator,
            batch[-1][0],
            len(batch),
            finished,
        )
        if won and finished:
            return
        # Lost the compare-and-swap: re-read the shard and resume from the
        # boundary the winner committed.  Nothing from our local batch is kept.
        # If we won but the shard is not finished, loop for the next batch.


def _completed_shards(db, run_id):
    return (
        db.query(AnalysisShard)
        .filter(AnalysisShard.run_id == run_id, AnalysisShard.completed.is_(True))
        .order_by(AnalysisShard.shard_index)
        .all()
    )


def execute(db, run):
    """Run (or resume) an analysis to completion and store the exact result."""
    experiment = db.get(Experiment, run.experiment_id)
    if not experiment:
        raise ValueError("实验不存在")
    dataset_id = experiment.dataset_id
    total = observation_count(db, dataset_id)
    shards = ensure_shards(db, run.id, dataset_id)
    for shard in shards:
        process_shard(db, run.id, dataset_id, shard.shard_index)

    completed = _completed_shards(db, run.id)
    if len(completed) != len(shards):
        raise ValueError("分片汇总不完整，拒绝写入结果")
    processed = sum(int(row.rows) for row in completed)
    if processed != total:
        raise ValueError(f"分片汇总样本数 {processed} 与数据集 {total} 不一致，拒绝写入结果")
    # Each completed shard must have scanned its whole bucket; otherwise a
    # half-written shard could slip into the summary even when row totals match.
    for row in completed:
        lo, hi = int(row.lower_id), int(row.upper_id)
        bucket_max = (
            db.query(func.max(Observation.id))
            .filter(
                Observation.dataset_id == dataset_id,
                Observation.id >= lo,
                Observation.id <= hi,
            )
            .scalar()
        )
        if bucket_max is not None and int(row.last_id) < int(bucket_max):
            raise ValueError("分片覆盖不完整，拒绝写入结果")
        if bucket_max is None and int(row.rows) != 0:
            raise ValueError("分片覆盖与数据集不一致，拒绝写入结果")

    merged = merge_states([decode_state(row.state) for row in completed])
    result = finalize(merged.state, experiment.alpha, experiment.cuped)
    result["progress"] = progress_snapshot(
        total=total,
        processed=processed,
        shards_total=len(shards),
        shards_completed=len(completed),
        finished=True,
    )
    run.result = result
    run.status = "completed"
    run.error = ""
    db.commit()
    return result


def progress_snapshot(total, processed, shards_total, shards_completed, finished=False):
    percent = 1.0 if finished else (processed / total if total else 0.0)
    return {
        "total_observations": int(total),
        "processed_observations": int(processed),
        "shards_total": int(shards_total),
        "shards_completed": int(shards_completed),
        "percent": round(percent, 6),
        "batch_size": ANALYSIS_BATCH_SIZE,
        "shard_size": ANALYSIS_SHARD_SIZE,
        "finished": bool(finished),
    }


def live_progress(db, run):
    """Progress for a run that may still be executing; safe for old completed runs."""
    experiment = db.get(Experiment, run.experiment_id)
    dataset_id = experiment.dataset_id if experiment else None
    total = observation_count(db, dataset_id) if dataset_id else 0
    processed, shards_total = (
        db.query(
            func.coalesce(func.sum(AnalysisShard.rows), 0),
            func.count(AnalysisShard.id),
        )
        .filter(AnalysisShard.run_id == run.id)
        .one()
    )
    shards_completed = (
        db.query(func.count(AnalysisShard.id))
        .filter(AnalysisShard.run_id == run.id, AnalysisShard.completed.is_(True))
        .scalar()
        or 0
    )
    if not shards_total and dataset_id:
        shards_total = expected_shard_count(db, dataset_id)
    return progress_snapshot(
        total=total,
        processed=int(processed or 0),
        shards_total=int(shards_total or 0),
        shards_completed=int(shards_completed),
        finished=run.status == "completed",
    )


def recoverable_runs(db):
    """Runs interrupted while queued/running; their checkpoints are reusable."""
    return (
        db.query(AnalysisRun)
        .filter(AnalysisRun.status.in_(("queued", "running")))
        .order_by(AnalysisRun.id)
        .all()
    )
