from sqlalchemy import ForeignKey, JSON, UniqueConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column
from ..config.database import Base
from ..utils.clock import now


class AnalysisShard(Base):
    """Checkpoint of one id-bucket shard of an analysis run.

    A shard is complete exactly once.  The unique constraint plus the worker's
    row lock make repeated or concurrent processing of the same shard count once.
    """

    __tablename__ = "analysis_shards"
    __table_args__ = (
        UniqueConstraint("run_id", "shard_index", name="uq_analysis_shards_run_shard"),
        Index("analysis_shards_run_idx", "run_id"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("analysis_runs.id"))
    shard_index: Mapped[int] = mapped_column()
    # Inclusive observation-id bucket, fixed at shard creation so a restart with a
    # different configured shard size still resumes the same boundaries.
    lower_id: Mapped[int] = mapped_column(default=0)
    upper_id: Mapped[int] = mapped_column(default=0)
    # Last observation id included in this shard, for resuming an interrupted shard.
    last_id: Mapped[int] = mapped_column(default=0)
    # Number of observations folded into this shard so far.
    rows: Mapped[int] = mapped_column(default=0)
    # Exact sufficient statistics, encoded as decimal strings (JSON-safe).
    state: Mapped[dict] = mapped_column(JSON, default=dict)
    completed: Mapped[bool] = mapped_column(default=False)
    updated_at: Mapped[str] = mapped_column(default=now)
