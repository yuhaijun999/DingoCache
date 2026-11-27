"""Negative startup contracts against real owned temporary Python processes."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'deploy/validation'))
import boundary_startup as startup

# Independent fake executable: no imports of the validator/case generator.
FAKE = r'''
import os
from pathlib import Path
import signal
import sys
import time
root = Path(os.environ['FAKE_ROOT'])
mode = os.environ.get('FAKE_MODE', '')
if mode == 'timeout':
    time.sleep(30)
if mode == 'ignore-term':
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    time.sleep(30)
if mode == 'mutate-meta':
    (root / 'slots.tbl').write_bytes(b'changed metadata')
if mode == 'create-root':
    root.mkdir()
if mode == 'payload-only':
    (root / 'payload').write_bytes(b'CHANGED!')
if mode == 'flood':
    sys.stdout.write('x' * (2 << 20))
    sys.stdout.flush()
    sys.exit(1)
name = None
for key in ('DFKV_SLAB_LIFE_MAX_READS', 'DFKV_SLAB_BUCKET_SEC',
            'DFKV_SLAB_BUCKET_COUNT', 'DFKV_SLAB_KEEP_SCORE_SEC',
            'DFKV_SLAB_EVICT_LOW_PCT'):
    if key in os.environ:
        if name is not None:
            print('unexpected stale numeric override')
            sys.exit(9)
        name = key
raw = os.environ[name]
if name == 'DFKV_SLAB_LIFE_MAX_READS' and raw in ('4', '63'):
    name, raw = 'DFKV_SLAB_BUCKET_COUNT', '24'
if os.environ.get('DFKV_SLAB_EVICTION_POLICY') != 'lifetime':
    sys.exit(8)
if os.environ.get('UNRELATED_ENV') != 'preserved':
    sys.exit(7)
if mode == 'wrong-raw':
    raw = 'not-the-supplied-value'
prefix = 'NOTERROR' if mode == 'no-error' else '[ERROR]'
print(prefix + ' ' + name + "='" + raw + "' rejected", flush=True)
sys.exit(0 if mode == 'wrong-exit' else 1)
'''


class BoundaryStartupTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.base = Path(self.directory.name)
        self.root = self.base / 'cache'
        self.root.mkdir()
        for name in startup.METADATA:
            (self.root / name).write_bytes(b'metadata:' + name.encode())
        (self.root / 'payload').write_bytes(b'ORIGINAL')
        self.script = self.base / 'fake.py'
        self.script.write_text(FAKE)
        self.env = dict(os.environ, FAKE_ROOT=str(self.root), UNRELATED_ENV='preserved')
        self.command = [sys.executable, str(self.script)]
        self.output = self.base / 'result'

    def run_fake(self, mode='', *, roots=None, timeout=2):
        return startup.run(self.command, dict(self.env, FAKE_MODE=mode),
                           roots if roots is not None else [self.root],
                           self.output, timeout=timeout)

    def test_frozen_cases_cover_ranges_coupling_and_raw_spelling(self):
        rows = startup.cases()
        self.assertEqual(len(rows), 12)
        self.assertEqual(len({row['case_id'] for row in rows}), 12)
        by_name = {row['case_id']: row for row in rows}
        for name in ('reads-four-default-buckets', 'reads-63-default-buckets'):
            self.assertEqual(by_name[name]['expected_name'], 'DFKV_SLAB_BUCKET_COUNT')
            self.assertEqual(by_name[name]['expected_raw'], '24')
        self.assertEqual(by_name['zero-prefixed-range']['expected_raw'], '00064')
        self.assertEqual(by_name['unsigned-overflow']['expected_raw'], '18446744073709551616')

    def test_real_rejections_all_pass_and_preserve_inventory_and_environment(self):
        before = startup.inventory([self.root])
        # These stale values must be cleared, not accidentally change the case.
        self.env.update({name: '999999999' for name in startup.PARAMETERS})
        original_env = dict(self.env)
        report = self.run_fake()
        self.assertTrue(report['passed'])
        self.assertEqual([row['state'] for row in report['cases']], ['passed'] * 12)
        self.assertTrue(all(row['process']['exit'] == 1 and row['process']['reaped']
                            and row['metadata_unchanged'] for row in report['cases']))
        self.assertTrue(all(not row['process']['cleanup'] for row in report['cases']))
        self.assertEqual(startup.inventory([self.root]), before)
        self.assertEqual(self.env, original_env)
        self.assertEqual(json.loads((self.output / 'report.json').read_text()), report)
        self.assertTrue((self.output / '11-after.json').is_file())
        for row in report['cases']:
            with self.assertRaises(FileNotFoundError):
                Path('/proc', str(row['process']['pid']), 'stat').read_text()

    def test_inventory_hashes_all_four_metadata_files_not_payload_bytes(self):
        report = startup.inventory([self.root])
        records = {entry['name']: entry for entry in report['roots'][0]['entries']}
        for name in startup.METADATA:
            self.assertEqual(records[name]['sha256'],
                             hashlib.sha256((self.root / name).read_bytes()).hexdigest())
        self.assertNotIn('sha256', records['payload'])
        self.assertIn('not-payload', report['scope'])
        # Same-sized payload replacement is intentionally outside this proof.
        report = self.run_fake('payload-only')
        self.assertTrue(report['passed'])
        self.assertEqual((self.root / 'payload').read_bytes(), b'CHANGED!')
        self.assertIn('not-payload-preservation', report['scope'])

    def test_nonexistent_roots_remain_absent_without_creation(self):
        missing = self.base / 'missing' / 'nested-cache'
        self.env['FAKE_ROOT'] = str(missing)
        report = self.run_fake(roots=[self.root, missing])
        self.assertTrue(report['passed'])
        self.assertFalse(missing.exists())
        self.assertFalse(missing.parent.exists())
        self.assertEqual(startup.inventory([missing])['roots'][0],
                         dict(path=str(missing), exists=False, entries=[]))

    def test_wrong_exit_missing_error_word_and_wrong_raw_fail_fast(self):
        for mode in ('wrong-exit', 'no-error', 'wrong-raw'):
            self.output = self.base / mode
            report = self.run_fake(mode)
            self.assertFalse(report['passed'])
            self.assertEqual(report['cases'][0]['state'], 'failed')
            self.assertEqual([row['state'] for row in report['cases'][1:]], ['not_attempted'] * 11)
            self.assertTrue(report['cases'][0]['process']['reaped'])
            self.assertEqual(len(report['errors']), 1)

    def test_metadata_mutation_or_new_root_cannot_pass(self):
        report = self.run_fake('mutate-meta')
        self.assertFalse(report['passed'])
        self.assertFalse(report['cases'][0]['metadata_unchanged'])
        self.assertTrue(report['cases'][0]['diagnostic_matches'])
        missing = self.base / 'new-cache'
        self.output = self.base / 'create-result'
        self.env['FAKE_ROOT'] = str(missing)
        report = self.run_fake('create-root', roots=[missing])
        self.assertFalse(report['passed'])
        self.assertTrue(missing.is_dir())  # Preserve the failure, never delete evidence.
        self.assertFalse(report['cases'][0]['metadata_unchanged'])

    def test_timeout_and_ignored_term_are_reaped_and_not_rejection_success(self):
        for mode in ('timeout', 'ignore-term'):
            self.output = self.base / mode
            report = self.run_fake(mode, timeout=.1)
            first = report['cases'][0]
            self.assertFalse(report['passed'])
            self.assertTrue(first['process']['timed_out'])
            self.assertTrue(first['process']['reaped'])
            self.assertIn('terminate', first['process']['cleanup'])
            if mode == 'ignore-term':
                self.assertIn('kill', first['process']['cleanup'])
                self.assertEqual(first['process']['exit'], -9)

    def test_spawn_failure_and_log_bound_are_retained_as_failed(self):
        self.command = [str(self.base / 'not-an-executable')]
        report = self.run_fake()
        first = report['cases'][0]
        self.assertFalse(report['passed'])
        self.assertIsNone(first['process']['pid'])
        self.assertFalse(first['process']['reaped'])
        self.assertIn('FileNotFoundError', first['process']['error'])
        self.command = [sys.executable, str(self.script)]
        self.output = self.base / 'flood-result'
        report = self.run_fake('flood')
        self.assertFalse(report['passed'])
        self.assertTrue(report['cases'][0]['process']['log_limit'])
        self.assertTrue(report['cases'][0]['process']['reaped'])

    def test_existing_output_and_overlapping_paths_rejected_without_spawn(self):
        self.output.mkdir()
        with mock.patch.object(startup.subprocess, 'Popen') as spawn:
            with self.assertRaises(ValueError):
                self.run_fake()
            spawn.assert_not_called()
        self.output = self.root / 'results'
        with self.assertRaisesRegex(ValueError, 'must not overlap'):
            self.run_fake()
        self.assertFalse(self.output.exists())
        self.output = self.base / 'new-output'
        with self.assertRaisesRegex(ValueError, 'must not overlap'):
            self.run_fake(roots=[self.root, self.root / 'child'])
        self.assertFalse(self.output.exists())

    def test_symlink_roots_ancestors_entries_and_fifo_fail_closed(self):
        linked = self.base / 'linked'
        linked.symlink_to(self.root, target_is_directory=True)
        for path in (linked, linked / 'missing'):
            with self.subTest(path=path), self.assertRaisesRegex(ValueError, 'symlink'):
                startup.inventory([path])
        nested = self.root / 'subdir'
        nested.mkdir()
        (nested / 'linked-meta').symlink_to(self.root / 'slab_meta')
        with self.assertRaisesRegex(ValueError, 'symlink/special'):
            startup.inventory([self.root])
        (nested / 'linked-meta').unlink()
        os.mkfifo(nested / 'fifo')
        with self.assertRaisesRegex(ValueError, 'symlink/special'):
            startup.inventory([self.root])

    def test_inventory_entry_depth_and_hashing_bounds(self):
        with mock.patch.object(startup, 'MAX_ENTRIES', 1):
            with self.assertRaisesRegex(ValueError, 'entry bound'):
                startup.inventory([self.root])
        with mock.patch.object(startup, 'MAX_METADATA_BYTES', 1):
            with self.assertRaisesRegex(ValueError, 'hashing bound'):
                startup.inventory([self.root])
        path = self.root
        for index in range(9):
            path = path / str(index)
            path.mkdir()
        with self.assertRaisesRegex(ValueError, 'depth bound'):
            startup.inventory([self.root])

    def test_metadata_changing_during_hash_is_not_a_stable_snapshot(self):
        original_read = os.read
        changed = False

        def mutate(fd, count):
            nonlocal changed
            if not changed:
                changed = True
                target = self.root / 'slab_initializing'
                target.write_bytes(b'x' * target.stat().st_size)
            return original_read(fd, count)

        with mock.patch.object(startup.os, 'read', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'changed during hashing'):
                startup.inventory([self.root])

    def test_invalid_inputs_create_no_output(self):
        invalid = [dict(command='server'), dict(command=[]), dict(env={'BAD': 1}),
                   dict(env={'A=B': 'x'}), dict(timeout=True), dict(timeout=0),
                   dict(timeout=float('inf')), dict(directories=[]),
                   dict(directories=[self.root, self.root])]
        for changes in invalid:
            arguments = dict(command=self.command, env=self.env, directories=[self.root], output=self.output)
            arguments.update(changes)
            with self.subTest(changes=changes), mock.patch.object(startup.subprocess, 'Popen') as spawn:
                with self.assertRaises(ValueError):
                    startup.run(**arguments)
                spawn.assert_not_called()
                self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
