import unittest
import os
import time
import sqlite3
from unittest.mock import MagicMock
import sys

# Add the src directory to sys.path for testing
sys.path.insert(0, os.path.abspath("/home/pc/dualcore_package_export/src"))

from dualcore.ladybug import LadybugStore
from dualcore.enzyme import EnzymeBridge, EnzymeCLIBridge
from dualcore.reconciliation import ReconciliationEngine, ReconciliationStatus

class TestDualCoreSession(unittest.TestCase):
    def setUp(self):
        self.db_path = "/tmp/test_ladybug.db"
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        
        self.ladybug = LadybugStore(self.db_path)
        self.enzyme = MagicMock(spec=EnzymeBridge)
        self.enzyme.available = True
        self.enzyme.petri.return_value = {"entities": []}
        self.enzyme.catalyze.return_value = {"results": []}
        self.engine = ReconciliationEngine(self.ladybug, self.enzyme)

    def tearDown(self):
        self.ladybug.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)

    def test_full_session_flow(self):
        print("\n--- Starting Full Session Simulation ---")
        results_0 = self.engine.reconcile_prefetch(query="test")
        self.assertEqual(len(results_0), 0)
        self.assertIsNone(self.engine._last_reconciled_ts)

        self.ladybug.store(content="Initial fact 1", importance=5)
        self.ladybug.store(content="Important fact 2", importance=8)
        results_1 = self.engine.reconcile_prefetch(query="fact")
        self.assertEqual(len(results_1), 0)
        self.assertIsNotNone(self.engine._last_reconciled_ts)

        time.sleep(1.1)
        self.ladybug.store(content="New delta fact", importance=5)
        self.ladybug.store(content="High importance tension fact", importance=9)
        time.sleep(1.1)

        self.enzyme.catalyze.return_value = {
            "results": [{
                "content": "However, this is actually different.",
                "similarity": 0.9,
                "file_path": "test.py"
            }]
        }

        results_2 = self.engine.reconcile_prefetch(query="tension")
        found_tension = any("tension fact" in r.claim for r in results_2)
        self.assertTrue(found_tension)
        print("[Step 4] Success: Delta scan correctly isolated new tension.")

    def test_convergence_and_optimization(self):
        print("\n--- Starting Optimization/Convergence Test ---")
        self.ladybug.store(content="Last stable fact", importance=5)
        time.sleep(1.1)
        self.engine.reconcile_prefetch(query="last")
        ts_before = self.engine._last_reconciled_ts
        results = self.engine.reconcile_prefetch(query="anything")
        self.assertEqual(len(results), 0)
        self.assertEqual(self.engine._last_reconciled_ts, ts_before)
        print("[Optimization Test] Success: Engine correctly handled empty delta.")

if __name__ == '__main__':
    unittest.main()
