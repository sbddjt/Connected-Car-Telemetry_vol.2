import json
import sys
import tempfile
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from shared_pipeline_metrics import PipelineMetrics,read_pipeline_metrics


class PipelineMetricsTests(unittest.TestCase):
    def test_missing_and_stale_metrics_are_distinguished_from_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            self.assertIsNone(read_pipeline_metrics(root)["components"]["vehicle-sender"])
            metrics=PipelineMetrics("vehicle-sender",root)
            metrics.publish(0,pending_events=0)
            self.assertFalse(read_pipeline_metrics(root)["components"]["vehicle-sender"]["stale"])
            data=json.loads(metrics.path.read_text())
            data["updated_at"]=(datetime.now(timezone.utc)-timedelta(seconds=30)).isoformat()
            metrics.path.write_text(json.dumps(data))
            self.assertTrue(read_pipeline_metrics(root)["components"]["vehicle-sender"]["stale"])

    def test_parallel_instances_sum_rates_and_pending_without_old_single_file(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            single=PipelineMetrics("vehicle-sender",root)
            single.publish(100,pending_events=999)
            for index,pending in [("0",2),("1",3)]:
                with patch.dict("os.environ",{"SHARED_PIPELINE_METRICS_INSTANCE":index}):
                    metrics=PipelineMetrics("vehicle-sender",root)
                    metrics.publish(10,pending_events=pending)
            result=read_pipeline_metrics(root)["components"]["vehicle-sender"]
            self.assertEqual(result["pending_events"],5)
            self.assertEqual(result["reporting_workers"],2)

    def test_snapshot_failure_does_not_interrupt_processing(self):
        with tempfile.TemporaryDirectory() as directory:
            metrics=PipelineMetrics("vehicle-collector",Path(directory))
            with patch.object(Path,"write_text",side_effect=PermissionError("locked")):
                metrics.publish(10,force=True)
            self.assertEqual(metrics.failures,1)


if __name__=="__main__": unittest.main()
