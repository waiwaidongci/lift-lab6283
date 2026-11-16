from pydantic import BaseModel, Field


class ProgressOutput(BaseModel):
    total_observations: int = 0
    processed_observations: int = 0
    shards_total: int = 0
    shards_completed: int = 0
    percent: float = 0.0
    batch_size: int = 0
    shard_size: int = 0
    finished: bool = False


class RunOutput(BaseModel):
    id: int
    experiment_id: int
    status: str
    result: dict
    error: str
    created_at: str
    progress: ProgressOutput = Field(default_factory=ProgressOutput)
