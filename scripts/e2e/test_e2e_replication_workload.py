#!/usr/bin/env python3

import unittest

import e2e_replication_workload as workload


class WorkloadPlanningTest(unittest.TestCase):
    def test_calculates_exact_write_count_from_qps_duration_and_ratio(self):
        plan = workload.WorkloadPlan(target_qps=10_000, duration_s=60, read_ratio=0.2)

        self.assertEqual(600_000, plan.total_ops)
        self.assertEqual(120_000, plan.total_reads)
        self.assertEqual(480_000, plan.total_writes)

    def test_fixed_width_key_is_16_bytes(self):
        self.assertEqual("key:000000000001", workload.fixed_key(1))
        self.assertEqual("key:000000480000", workload.fixed_key(480_000))
        self.assertEqual(16, len(workload.fixed_key(1).encode("utf-8")))

    def test_partitions_exact_ops_across_clients(self):
        ranges = [workload.op_range_for_client(600_000, 16, i) for i in range(16)]

        self.assertEqual((1, 37_500), ranges[0])
        self.assertEqual((562_501, 600_000), ranges[-1])
        self.assertEqual(600_000, sum(end - start + 1 for start, end in ranges))
        self.assertEqual(list(range(1, 600_001)), [
            op for start, end in ranges for op in range(start, end + 1)
        ])

    def test_write_count_after_op_uses_global_sequence(self):
        plan = workload.WorkloadPlan(target_qps=10_000, duration_s=60, read_ratio=0.2)

        self.assertEqual(0, workload.write_count_after_op(plan, 0))
        self.assertEqual(1, workload.write_count_after_op(plan, 1))
        self.assertEqual(480_000, workload.write_count_after_op(plan, 600_000))


if __name__ == "__main__":
    unittest.main()
