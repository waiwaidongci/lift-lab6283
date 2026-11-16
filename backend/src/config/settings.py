import os
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql+psycopg://lift:lift@db/lift")
REDIS_URL = os.environ.get("REDIS_URL", "redis://redis:6379/0")
JWT_SECRET = os.environ.get("JWT_SECRET", "local-development-change-this-key")
SEED_DEMO = os.environ.get("SEED_DEMO", "true").lower() == "true"
# Rows fetched per checkpointed batch.  Progress is committed at each boundary.
ANALYSIS_BATCH_SIZE = max(1, int(os.environ.get("ANALYSIS_BATCH_SIZE", "2000")))
# Observation ids are partitioned into fixed buckets so a shard's identity never
# depends on batch size or processing order.
ANALYSIS_SHARD_SIZE = max(1, int(os.environ.get("ANALYSIS_SHARD_SIZE", "50000")))
