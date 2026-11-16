"""Regression tests for chunked, checkpointed analysis.

Run from the repository root:

    PYTHONPATH=backend python3 -m unittest backend/tests/test_chunked_analysis.py

Uses a temporary SQLite database and never touches the demo data.
"""
import os
import random
import sys
import tempfile
import unittest

_DB = tempfile.mktemp(suffix=".db")
os.environ.setdefault("DATABASE_URL", f"sqlite:///{_DB}")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6399/0")
os.environ.setdefault("SEED_DEMO", "false")
os.environ.setdefault("ANALYSIS_BATCH_SIZE", "250")
os.environ.setdefault("ANALYSIS_SHARD_SIZE", "1000")
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import sessionmaker  # noqa: E402

import src.models  # noqa: E402,F401  (registers tables)
import src.services.analysis_service as analysis  # noqa: E402
from src.config.database import Base  # noqa: E402
from src.models import (  # noqa: E402
    AnalysisRun,
    AnalysisShard,
    Dataset,
    Experiment,
    Observation,
)
from src.services.statistics_service import (  # noqa: E402
    Accumulator,
    analyze,
    decode_state,
    finalize,
)


class ChunkedAnalysisTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine = create_engine(os.environ["DATABASE_URL"])
        cls.Session = sessionmaker(bind=cls.engine, expire_on_commit=False)
        Base.metadata.create_all(cls.engine)
        with cls.Session() as db:
            db.add_all(
                [
                    Dataset(name="regression", source="t.csv", description="d", checksum="c"),
                ]
            )
            db.flush()
            dataset = db.query(Dataset).filter_by(name="regression").one()
            rng = random.Random(42)
            rows = []
            for i in range(2400):
                arm = "control" if i % 2 == 0 else "treatment"
                x = max(0.1, rng.gauss(36, 12))
                y = max(0, 7 + 0.65 * x + rng.gauss(0, 7) + (2.8 if arm == "treatment" else 0))
                rows.append(
                    dict(
                        dataset_id=dataset.id,
                        subject=f"R-{i:05}",
                        arm=arm,
                        pre_value=round(x, 4),
                        value=round(y, 4),
                    )
                )
            db.bulk_insert_mappings(Observation, rows)
            db.commit()
            cls.dataset_id = dataset.id

    def setUp(self):
        analysis.ANALYSIS_BATCH_SIZE = 250
        analysis.ANALYSIS_SHARD_SIZE = 1000

    def _experiment(self, db, name, cuped=True, alpha=0.05):
        exp = Experiment(
            name=name,
            dataset_id=self.dataset_id,
            hypothesis="regression hypothesis",
            owner="tester",
            alpha=alpha,
            cuped=cuped,
        )
        db.add(exp)
        db.commit()
        return exp

    def _execute(self, db, exp):
        run = AnalysisRun(experiment_id=exp.id, status="running")
        db.add(run)
        db.commit()
        analysis.execute(db, run)
        return run

    def test_matches_single_pass_and_batch_order_is_irrelevant(self):
        with self.Session() as db:
            reference = analyze(
                db.query(Observation).filter_by(dataset_id=self.dataset_id).all(), 0.05, True
            )
            baseline = self._execute(db, self._experiment(db, "baseline"))
            self.assertEqual(baseline.result["raw"], reference["raw"])
            self.assertEqual(baseline.result["cuped"], reference["cuped"])
            for batch in (1, 7, 999, 10000):
                analysis.ANALYSIS_BATCH_SIZE = batch
                run = self._execute(db, self._experiment(db, f"batch-{batch}"))
                self.assertEqual(run.result["raw"], baseline.result["raw"])
                self.assertEqual(run.result["cuped"], baseline.result["cuped"])
            for shard in (1, 333, 5000):
                analysis.ANALYSIS_SHARD_SIZE = shard
                run = self._execute(db, self._experiment(db, f"shard-{shard}"))
                self.assertEqual(run.result["raw"], baseline.result["raw"])
                self.assertEqual(run.result["cuped"], baseline.result["cuped"])

    def test_duplicate_shard_counts_once(self):
        with self.Session() as db:
            run = self._execute(db, self._experiment(db, "duplicate"))
            shard = (
                db.query(AnalysisShard)
                .filter_by(run_id=run.id)
                .order_by(AnalysisShard.shard_index)
                .first()
            )
            before = (shard.rows, shard.last_id, dict(shard.state))
            analysis.process_shard(db, run.id, self.dataset_id, shard.shard_index)
            db.refresh(shard)
            self.assertEqual((shard.rows, shard.last_id, dict(shard.state)), before)
            self.assertEqual(run.result["progress"]["processed_observations"], 2400)

    def test_concurrent_checkpoint_has_one_winner(self):
        with self.Session() as db:
            run = AnalysisRun(
                experiment_id=self._experiment(db, "concurrent").id, status="running"
            )
            db.add(run)
            db.commit()
            analysis.ensure_shards(db, run.id, self.dataset_id)
            shard = (
                db.query(AnalysisShard)
                .filter_by(run_id=run.id)
                .order_by(AnalysisShard.shard_index)
                .first()
            )
            rows = (
                db.query(
                    Observation.id,
                    Observation.arm,
                    Observation.pre_value,
                    Observation.value,
                )
                .filter(
                    Observation.dataset_id == self.dataset_id,
                    Observation.id >= shard.lower_id,
                    Observation.id <= shard.upper_id,
                )
                .order_by(Observation.id)
                .limit(250)
                .all()
            )
            with self.Session() as other:
                acc_a = Accumulator(decode_state(shard.state))
                acc_b = Accumulator(decode_state(shard.state))
                for _, arm, x, y in rows:
                    acc_a.add(arm, x, y)
                    acc_b.add(arm, x, y)
                won_b = analysis._checkpoint(
                    other, run.id, shard.shard_index, 0, acc_b, rows[-1][0], len(rows), False
                )
                won_a = analysis._checkpoint(
                    db, run.id, shard.shard_index, 0, acc_a, rows[-1][0], len(rows), False
                )
                self.assertTrue(won_b)
                self.assertFalse(won_a)
            db.expire_all()
            shard = db.query(AnalysisShard).filter_by(run_id=run.id, shard_index=shard.shard_index).one()
            self.assertEqual(shard.rows, 250)

    def test_resume_from_batch_boundary_after_interruption(self):
        with self.Session() as db:
            reference = self._execute(db, self._experiment(db, "resume-reference"))
            run = AnalysisRun(
                experiment_id=self._experiment(db, "resume").id, status="running"
            )
            db.add(run)
            db.commit()
            analysis.ensure_shards(db, run.id, self.dataset_id)
            shards = (
                db.query(AnalysisShard)
                .filter_by(run_id=run.id)
                .order_by(AnalysisShard.shard_index)
                .all()
            )
            analysis.process_shard(db, run.id, self.dataset_id, shards[0].shard_index)
            second = shards[1]
            batch = (
                db.query(
                    Observation.id,
                    Observation.arm,
                    Observation.pre_value,
                    Observation.value,
                )
                .filter(
                    Observation.dataset_id == self.dataset_id,
                    Observation.id >= second.lower_id,
                    Observation.id <= second.upper_id,
                )
                .order_by(Observation.id)
                .limit(250)
                .all()
            )
            acc = Accumulator()
            for _, arm, x, y in batch:
                acc.add(arm, x, y)
            analysis._checkpoint(
                db, run.id, second.shard_index, 0, acc, batch[-1][0], len(batch), False
            )
            # Simulate a restart: re-run without resetting any checkpoint.
            analysis.execute(db, run)
            self.assertEqual(run.status, "completed")
            self.assertEqual(run.result["raw"], reference.result["raw"])
            self.assertEqual(run.result["cuped"], reference.result["cuped"])
            self.assertEqual(run.result["progress"]["processed_observations"], 2400)

    def test_incomplete_summary_is_rejected_and_retry_is_clean(self):
        with self.Session() as db:
            reference = self._execute(db, self._experiment(db, "retry-reference"))
            run = AnalysisRun(
                experiment_id=self._experiment(db, "retry").id, status="running"
            )
            db.add(run)
            db.commit()
            analysis.ensure_shards(db, run.id, self.dataset_id)
            original = analysis.observation_count
            analysis.observation_count = lambda db_, dataset_id: 2399
            try:
                with self.assertRaises(ValueError):
                    analysis.execute(db, run)
            finally:
                analysis.observation_count = original
            db.rollback()
            run = db.get(AnalysisRun, run.id)
            run.status = "running"
            run.result = {}
            db.commit()
            analysis.execute(db, run)
            self.assertEqual(run.result["raw"], reference.result["raw"])

    def test_huge_common_base_keeps_small_difference(self):
        from fractions import Fraction

        acc = Accumulator()
        rng = random.Random(9)
        treatment, control = [], []
        for i in range(4000):
            arm = "control" if i % 2 == 0 else "treatment"
            base = 1e9
            x = base + rng.gauss(0, 3)
            y = base + rng.gauss(0, 30) + (2.5 if arm == "treatment" else 0)
            acc.add(arm, x, y)
            (treatment if arm == "treatment" else control).append(y)
        result = finalize(acc.state, 0.05, True)
        # Exact reference: sum the float values as Fractions so the huge common
        # base cannot hide the effect through float64 cancellation.
        exact = sum(map(Fraction, treatment)) / len(treatment) - sum(
            map(Fraction, control)
        ) / len(control)
        self.assertEqual(result["raw"]["difference"], float(exact))
        self.assertAlmostEqual(result["raw"]["difference"], 2.5, delta=1.0)
        self.assertTrue(result["significant"])

    def test_rejects_too_few_and_constant_arm(self):
        for rows in (
            [("control", 1.0, 2.0)] * 2 + [("treatment", 1.0, 2.0)] * 2,
            [("control", 1.0, 1.0), ("treatment", 2.0, 2.0)] * 10,
        ):
            acc = Accumulator()
            for arm, x, y in rows:
                acc.add(arm, x, y)
            with self.assertRaises(ValueError):
                finalize(acc.state, 0.05, False)


if __name__ == "__main__":
    unittest.main()
