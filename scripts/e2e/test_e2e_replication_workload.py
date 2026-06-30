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

    def test_big_string_key_uses_kerry_prefix_and_fixed_width(self):
        self.assertEqual("kerry-test-big-string-0001", workload.big_string_key(1))
        self.assertEqual("kerry-test-big-string-0005", workload.big_string_key(5))
        self.assertTrue(workload.big_string_key(1).startswith("kerry-test-big-string-"))

        with self.assertRaises(ValueError):
            workload.big_string_key(0)

    def test_big_string_size_cycles_through_threshold_sizes(self):
        # first size must be exactly 256 KiB to hit Velo's big-string + compress branch
        self.assertEqual(256 * 1024, workload.big_string_size(1))
        # all sizes must meet the 256 KiB big-string threshold
        for i in range(1, len(workload.BIG_STRING_SIZES) + 2):
            self.assertGreaterEqual(workload.big_string_size(i), 256 * 1024)
        # cycles when index exceeds the schedule length
        self.assertEqual(workload.big_string_size(1), workload.big_string_size(len(workload.BIG_STRING_SIZES) + 1))

    def test_big_string_value_is_deterministic_and_exact_length(self):
        for index in (1, 2, 3):
            size = workload.big_string_size(index)
            value = workload.big_string_value(index, size)
            self.assertEqual(size, len(value))
            self.assertEqual(value, workload.big_string_value(index, size))

        # arbitrary sizes are honored exactly, including tiny ones
        self.assertEqual(10, len(workload.big_string_value(7, 10)))
        self.assertEqual(1, len(workload.big_string_value(7, 1)))

    def test_big_string_value_is_bytes_for_binary_safe_transport(self):
        value = workload.big_string_value(1, workload.big_string_size(1))
        self.assertIsInstance(value, bytes)


if __name__ == "__main__":
    unittest.main()
