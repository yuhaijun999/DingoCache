"""Restart diagnostic fake-backend tests; no server or hardware loads."""
import copy
import hashlib
from pathlib import Path
import sys
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT/'deploy/validation'))
sys.path.insert(0, str(ROOT/'integration/common/src'))
import prefix_recovery as recovery
from eviction_trace import checksum


class MemoryBackend:
    transport = 'fake-rdma-test'

    def __init__(self):
        self.values = {}
        self.failure = None
        self.calls = 0
        self.closed = False

    def put(self, obj, value):
        self.calls += 1
        if self.failure:
            return self.failure
        self.values[obj['object_id']] = value
        return 'ack'

    def get(self, obj, expected):
        self.calls += 1
        if self.failure:
            if isinstance(self.failure, Exception):
                raise self.failure
            return self.failure, None
        value = self.values.get(obj['object_id'])
        return ('miss', None) if value is None else ('hit', value)

    def close(self):
        self.closed = True


class IdentityBackend(MemoryBackend):
    # This records a keep-first contract, not an eviction-policy oracle.
    def __init__(self):
        super().__init__()
        self.heat = {}
        self.operations = []

    def put(self, obj, value):
        key = (obj['namespace_hex'], obj['key_hex'])
        self.calls += 1
        self.operations.append(('put', key))
        if key not in self.values:
            self.values[key] = value
            self.heat[key] = 0
        return 'ack'

    def get(self, obj, expected):
        key = (obj['namespace_hex'], obj['key_hex'])
        self.calls += 1
        self.operations.append(('get', key))
        if key not in self.values:
            return 'miss', None
        self.heat[key] += 1
        return 'hit', self.values[key]


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.manifest = recovery.generate()
        self.backend = MemoryBackend()

    def run_mode(self, mode, expected=None, expect=None):
        report = recovery.execute(self.manifest, self.backend, mode, expected, expect)
        self.assertEqual(report['summary'], recovery.check(self.manifest, report, expected, expect))
        return report

    def test_manifest_determinism_and_bounds(self):
        self.assertEqual(self.manifest, recovery.generate())
        self.assertEqual(len(self.manifest['objects']), 32)
        self.assertEqual(len(self.manifest['chains']), 4)
        self.assertEqual({obj['length'] for obj in self.manifest['objects']}, {4096, 1048577})
        changed = copy.deepcopy(self.manifest)
        changed['objects'][0]['length'] += 1
        changed['sha256'] = checksum(changed)
        with self.assertRaises(ValueError):
            recovery.validate(changed)

    def test_checkpoint_zero_preserves_original_manifest_and_payloads(self):
        manifest = recovery.generate(0)
        self.assertEqual(manifest, recovery.generate())
        self.assertNotIn('checkpoint', manifest)
        self.assertEqual(manifest['sha256'],
                         '2cf80bf67b291cc789a36a850b979f1c85ef64eddd18ed45461cce310c534a24')
        for index, digest in ((0, 'b482798d5fa5cb80971976c469c5fa8e92ab42b4ff45dd9d115098d44741aca4'),
                              (31, 'a0f2bbc59f9c19f610f8e84ab72077521a96ad7bde8440c1ed35259725d81abf')):
            self.assertEqual(hashlib.sha256(recovery.payload(manifest['objects'][index])).hexdigest(), digest)

    def test_checkpoints_have_disjoint_keys_and_payload_identities(self):
        keys, digests = set(), set()
        for checkpoint in (0, 1, 2, 101):
            manifest = recovery.generate(checkpoint)
            recovery.validate(manifest)
            self.assertEqual(manifest, recovery.generate(checkpoint))
            cohort_keys = {obj['key_hex'] for obj in manifest['objects']}
            self.assertEqual(len(cohort_keys), 32)
            self.assertFalse(keys & cohort_keys)
            keys.update(cohort_keys)
            self.assertEqual({obj['namespace_hex'] for obj in manifest['objects']},
                             {recovery.NAMESPACE.hex()})
            self.assertNotIn(manifest['sha256'], digests)
            digests.add(manifest['sha256'])
            if checkpoint:
                self.assertEqual(manifest['checkpoint'], checkpoint)
                self.assertNotEqual(recovery.payload(manifest['objects'][0]),
                                    recovery.payload(self.manifest['objects'][0]))
        for checkpoint in (-1, 102, True, False, '1', 1.0, None):
            with self.subTest(checkpoint=checkpoint), self.assertRaises(ValueError):
                recovery.generate(checkpoint)
        for checkpoint in (True, '1', 0):
            changed = copy.deepcopy(self.manifest)
            changed['checkpoint'] = checkpoint
            changed['sha256'] = checksum(changed)
            with self.assertRaises(ValueError):
                recovery.validate(changed)

    def test_cross_checkpoint_expected_snapshot_rejected_before_io(self):
        self.backend = IdentityBackend()
        snapshot = self.run_mode('snapshot')
        calls = self.backend.calls
        for expect in ('preserve', 'empty'):
            with self.subTest(expect=expect), self.assertRaises(ValueError):
                recovery.execute(recovery.generate(1), self.backend, 'verify', snapshot, expect)
        self.assertEqual(self.backend.calls, calls)
        with tempfile.TemporaryDirectory() as directory:
            manifest_path = Path(directory)/'manifest.json'
            snapshot_path = Path(directory)/'snapshot.json'
            recovery._write(manifest_path, recovery.generate(1))
            recovery._write(snapshot_path, snapshot)
            args = ['--lib', 'unused', '--members', 'unused', '--output',
                    str(Path(directory)/'output'), '--mode', 'verify',
                    '--manifest', str(manifest_path), '--expected', str(snapshot_path),
                    '--expect', 'preserve']
            with mock.patch('disk_scenarios.load_library') as library, \
                    mock.patch.object(recovery, 'RdmaBackend') as backend:
                self.assertEqual(recovery.main(args), 1)
                library.assert_not_called()
                backend.assert_not_called()
            self.assertFalse((Path(directory)/'output').exists())

    def test_keep_first_duplicate_puts_do_not_reset_heat_but_fresh_cohort_is_unread(self):
        self.backend = IdentityBackend()
        self.run_mode('seed')
        self.run_mode('snapshot')
        old_heat = dict(self.backend.heat)
        self.run_mode('seed')
        self.assertEqual(self.backend.heat, old_heat)
        fresh = recovery.generate(1)
        fresh_keys = {(obj['namespace_hex'], obj['key_hex']) for obj in fresh['objects']}
        self.assertFalse(fresh_keys & old_heat.keys())
        begin = len(self.backend.operations)
        for _ in range(2):
            report = recovery.execute(fresh, self.backend, 'seed')
            recovery.check(fresh, report)
            self.assertTrue(report['passed'])
            self.assertEqual(report['summary']['native']['put_ack'], 32)
            self.assertEqual(report['summary']['native']['get_hit'], 0)
        self.assertEqual([op for op, key in self.backend.operations[begin:]], ['put']*64)
        self.assertTrue(all(self.backend.heat[key] == 0 for key in fresh_keys))
        self.assertTrue(all(self.backend.heat[key] == heat for key, heat in old_heat.items()))
        snapshot = recovery.execute(fresh, self.backend, 'snapshot')
        recovery.check(fresh, snapshot)
        self.assertTrue(snapshot['passed'])
        self.assertEqual(snapshot['objects'], ['hit']*32)
        self.assertEqual(snapshot['summary']['confirmed_prefix_tokens'], 640)

    def test_identity_and_payload_survive_process_boundaries(self):
        code = ("import hashlib,sys;sys.path.insert(0,sys.argv[1]);"
                "import prefix_recovery as p;m=p.generate();"
                "print(m['sha256']);"
                "print(hashlib.sha256(p.payload(m['objects'][31])).hexdigest())")
        result = subprocess.check_output(
            [sys.executable, '-c', code, str(ROOT/'deploy/validation')], text=True)
        import hashlib
        self.assertEqual(result.splitlines(), [self.manifest['sha256'],
                         hashlib.sha256(recovery.payload(self.manifest['objects'][31])).hexdigest()])

    def test_cold_large_class_donor_loss_is_repaired_by_one_refill(self):
        class ColdDonorBackend(MemoryBackend):
            def __init__(self):
                super().__init__()
                self.donated = False

            def put(self, obj, value):
                status = super().put(obj, value)
                if status == 'ack' and obj['length'] == 1048577 and not self.donated:
                    del self.values[0]
                    self.donated = True
                return status

            def evict_large_class_for_pressure(self):
                # Model unrelated pressure reclaiming the large class. Its next
                # binding may donate a small slot even after small-chain reads.
                for index in list(self.values):
                    if self.manifest_objects[index]['length'] == 1048577:
                        del self.values[index]
                self.donated = False

        self.backend = ColdDonorBackend()
        self.backend.manifest_objects = self.manifest['objects']
        seed = self.run_mode('seed')
        self.assertEqual(seed['summary']['native']['put_ack'], 32)
        before = self.run_mode('snapshot')
        self.assertTrue(before['passed'])
        self.assertEqual(before['objects'].count('hit'), 31)
        self.assertEqual(before['objects'][0], 'miss')
        self.assertLess(before['summary']['confirmed_prefix_tokens'], 640)
        self.assertTrue(self.run_mode('refill')['passed'])
        after = self.run_mode('snapshot')
        self.assertEqual(after['objects'], ['hit']*32)
        self.assertEqual(after['summary']['confirmed_prefix_tokens'], 640)

        self.backend.evict_large_class_for_pressure()
        self.assertEqual(len(self.backend.values), 16)
        first_restore = self.run_mode('refill')
        self.assertTrue(first_restore['passed'])
        self.assertEqual(first_restore['summary']['native']['put_ack'], 16)
        # Small chains were validated before the large class was rebound, so
        # the refill report cannot guarantee simultaneous end-of-pass residence.
        self.assertEqual(first_restore['objects'], ['hit']*32)
        partial = self.run_mode('snapshot')
        self.assertEqual(partial['objects'].count('hit'), 31)
        self.assertEqual(partial['objects'][0], 'miss')
        self.assertLess(partial['summary']['confirmed_prefix_tokens'], 640)

        second_restore = self.run_mode('refill')
        self.assertTrue(second_restore['passed'])
        self.assertEqual(second_restore['summary']['native']['put_ack'], 10)
        stabilized = self.run_mode('snapshot')
        self.assertEqual(stabilized['objects'], ['hit']*32)
        self.assertEqual(stabilized['summary']['confirmed_prefix_tokens'], 640)

    def test_seed_snapshot_preserve_empty(self):
        seed = self.run_mode('seed')
        self.assertTrue(seed['passed'])
        self.assertEqual(seed['summary']['native']['put_ack'], 32)
        self.assertEqual(seed['summary']['put_bytes'], 16*(4096+1048577))
        snapshot = self.run_mode('snapshot')
        self.assertEqual(snapshot['summary']['confirmed_prefix_tokens'], 640)
        self.assertTrue(self.run_mode('verify', snapshot, 'preserve')['passed'])
        self.backend.values.clear()
        self.assertTrue(self.run_mode('verify', snapshot, 'empty')['passed'])
        self.assertFalse(self.run_mode('verify', snapshot, 'preserve')['passed'])
        self.backend.values[0] = recovery.payload(self.manifest['objects'][0])
        self.assertFalse(self.run_mode('verify', snapshot, 'empty')['passed'])

    def test_partial_chain_preserves_exact_holes_not_only_prefix(self):
        self.run_mode('seed')
        del self.backend.values[1]
        snapshot = self.run_mode('snapshot')
        self.assertEqual(snapshot['objects'][1], 'miss')
        self.assertEqual(snapshot['objects'][2], 'hit')
        self.assertEqual(snapshot['chains'][0]['confirmed_prefix_tokens'], 16)
        self.assertTrue(self.run_mode('verify', snapshot, 'preserve')['passed'])
        del self.backend.values[2]
        self.assertFalse(self.run_mode('verify', snapshot, 'preserve')['passed'])

    def test_refill_freezes_prefix_and_rereads_every_byte(self):
        self.run_mode('seed')
        del self.backend.values[1]
        report = self.run_mode('refill')
        self.assertTrue(report['passed'])
        self.assertEqual(report['chains'][0]['confirmed_prefix_tokens'], 16)
        self.assertEqual(report['chains'][1]['confirmed_prefix_tokens'], 160)
        self.assertEqual(report['summary']['native']['put_ack'], 9)
        self.assertEqual(report['objects'], ['hit']*32)
        empty = MemoryBackend()
        report = recovery.execute(self.manifest, empty, 'refill')
        self.assertTrue(report['passed'])
        recovery.check(self.manifest, report)
        self.assertEqual(report['summary']['native']['put_ack'], 32)

    def test_errors_unknown_corruption_abort_with_full_denominator(self):
        for failure in ('error', 'unknown', 'invalid', RuntimeError('io')):
            with self.subTest(failure=failure):
                self.backend.failure = failure
                report = self.run_mode('snapshot')
                self.assertFalse(report['passed'])
                self.assertEqual(len(report['events']), 1)
                self.assertEqual(report['summary']['planned_objects'], 32)
                self.assertEqual(report['summary']['target_tokens'], 640)
                self.assertEqual(report['summary']['not_attempted_objects'], 31)
        self.backend.failure = None
        self.backend.values[0] = b'wrong'
        self.assertFalse(self.run_mode('snapshot')['passed'])
        self.backend.failure = 'unknown'
        self.assertFalse(self.run_mode('seed')['passed'])
        self.assertFalse(self.run_mode('refill')['passed'])

    def test_refill_write_and_reread_fail_closed(self):
        with mock.patch.object(self.backend, 'put', return_value='error'):
            report = self.run_mode('refill')
            self.assertFalse(report['passed'])
            self.assertEqual(len(report['events']), 2)
        with mock.patch.object(self.backend, 'put', return_value='ack'):
            report = self.run_mode('refill')
            self.assertFalse(report['passed'])
            self.assertEqual(report['chains'][0]['reread'][0], 'miss')

    def test_checker_rejects_resealed_tamper_and_incomplete_evidence(self):
        self.run_mode('seed')
        original = self.run_mode('snapshot')
        for mutation in ('total', 'payload', 'missing', 'extra', 'identity'):
            report = copy.deepcopy(original)
            if mutation == 'total':
                report['summary']['target_tokens'] = 1
            elif mutation == 'payload':
                report['events'][0]['observed_sha256'] = '0'*64
            elif mutation == 'missing':
                report['events'].pop()
            elif mutation == 'extra':
                report['events'].append(report['events'][0])
            else:
                report['events'][0]['object_id'] = 1
            report['sha256'] = checksum(report)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                recovery.check(self.manifest, report)

    def test_success_events_cannot_carry_errors(self):
        for mode in ('seed', 'snapshot'):
            report = self.run_mode(mode)
            report['events'][0]['error'] = 'contradictory failure'
            report['sha256'] = checksum(report)
            with self.subTest(mode=mode), self.assertRaises(ValueError):
                recovery.check(self.manifest, report)
        self.backend.values.clear()
        report = self.run_mode('snapshot')
        report['events'][0]['error'] = 'failed miss'
        report['sha256'] = checksum(report)
        with self.assertRaises(ValueError):
            recovery.check(self.manifest, report)

    def test_verify_rejects_cross_transport_snapshot_before_io(self):
        snapshot = self.run_mode('snapshot')
        self.backend.transport = 'rdma'
        calls = self.backend.calls
        with self.assertRaises(ValueError):
            recovery.execute(self.manifest, self.backend, 'verify', snapshot, 'empty')
        self.assertEqual(self.backend.calls, calls)

    def test_cli_close_failure_cannot_return_success(self):
        with tempfile.TemporaryDirectory() as directory:
            args = ['--lib', 'unused', '--members', 'unused', '--output',
                    str(Path(directory)/'run'), '--mode', 'seed']
            with mock.patch('disk_scenarios.load_library', return_value=object()), \
                    mock.patch.object(recovery, 'RdmaBackend', return_value=self.backend), \
                    mock.patch.object(self.backend, 'close', side_effect=RuntimeError('close failed')):
                with self.assertRaisesRegex(RuntimeError, 'close failed'):
                    recovery.main(args)
                self.assertTrue((Path(directory)/'run/report.json').exists())

    def test_cli_invalid_verify_does_no_native_io(self):
        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory)/'manifest.json'
            recovery._write(manifest, self.manifest)
            args = ['--lib', 'unused', '--members', 'unused', '--output',
                    str(Path(directory)/'run'), '--mode', 'verify',
                    '--manifest', str(manifest)]
            with mock.patch('disk_scenarios.load_library') as library, \
                    mock.patch.object(recovery, 'RdmaBackend') as backend:
                self.assertEqual(recovery.main(args), 1)
                library.assert_not_called()
                backend.assert_not_called()
                self.assertFalse((Path(directory)/'run').exists())

    def test_expectations_transport_and_cli_validation(self):
        self.backend.transport = 'tcp'
        with self.assertRaises(ValueError):
            recovery.execute(self.manifest, self.backend, 'seed')
        self.backend.transport = 'fake-rdma-test'
        with self.assertRaises(ValueError):
            recovery.execute(self.manifest, self.backend, 'verify')
        with self.assertRaises(ValueError):
            recovery.execute(self.manifest, self.backend, 'seed', expect='empty')
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)/'run'
            args = ['--lib', 'unused', '--members', 'unused', '--output', str(output), '--mode', 'seed']
            with mock.patch('disk_scenarios.load_library', return_value=object()), mock.patch.object(recovery, 'RdmaBackend', return_value=self.backend):
                self.assertEqual(recovery.main(args), 0)
                self.assertTrue(self.backend.closed)
                self.assertTrue((output/'manifest.json').exists())
                self.assertTrue((output/'report.json').exists())
                self.assertEqual(recovery.main(args), 1)


if __name__ == '__main__':
    unittest.main()
