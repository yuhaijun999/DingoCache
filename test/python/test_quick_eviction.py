"""Hardware-free checks for the bounded eviction supervisor, not RDMA proof."""

import contextlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    'quick_eviction', ROOT / 'deploy/validation/quick_eviction.py')
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def good_client():
    return {'status': 'passed', 'wrong_bytes': 0, 'transport_mode': ['rdma', 'rdma'],
            'hits': 12000, 'misses': 904, 'hot_hits': 128, 'hot_misses': 7424, 'hot_reads': 7552}


def good_metrics():
    return '\n'.join((
        'dfkv_storage_healthy 1', 'dfkv_slab_healthy 1', 'dfkv_errors_total 0',
        'dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_inflight 0',
        'dfkv_evictions_total 904', 'dfkv_uring_reads_total 12128',
        'dfkv_slab_watermark_evictions_total 900', 'dfkv_slab_reclaimed_total 4',
        'dfkv_slab_extent_steals_total 0',
        'dfkv_slab_lifetime_read_success_total 12128',
        'dfkv_slab_lifetime_read_applied_total 12128',
        'dfkv_uring_completions_total 12128',
        'dfkv_disk_objects{disk="a"} 1', 'dfkv_disk_objects{disk="b"} 1',
        'dfkv_disk_objects{disk="c"} 1',
        'dfkv_rdma_pull_requests_dispatched_total 20456',
        'dfkv_rdma_pull_requests_terminal_total{path="async",result="success_nonempty"} 12128',
        'dfkv_rdma_pull_requests_terminal_total{path="async",result="not_found"} 8328',
        'dfkv_rdma_pull_requests_terminal_total{path="sync",result="io_error"} 0')) + '\n'


def sustained_client():
    return {**good_client(), 'hits': 14712, 'misses': 904,
            'hot_hits': 12928, 'hot_misses': 0, 'hot_reads': 12928,
            'pressure_schedule_origin_sec': 3.0, 'pressure_schedule_elapsed_sec': 199.02,
            'steps': [{'step': index, 'start_sec': index + .001, 'end_sec': index + .01,
                       'cold_written': 32, 'hot_hits': 64, 'hot_misses': 0}
                      for index in range(200)]}


def sustained_metrics():
    return good_metrics().replace('12128', '27640').replace('20456', '28544').replace('8328', '904')


def delayed_client(delay=90, all_miss=False):
    client = sustained_client()
    client.update(first_reuse_sec=delay, filler_warmup_reads=18304,
                  hot_cohort_write_completed_sec=[1.0, 2.0])
    for row in client['steps']:
        active = row['start_sec'] >= delay
        row.update(hot_probe_started_sec=row['start_sec'] + .001 if active else None,
                   hot_hits=64 if active and not all_miss else 0,
                   hot_misses=64 if active and all_miss else 0)
    client['hot_hits'] = sum(row['hot_hits'] for row in client['steps'])
    client['hot_misses'] = sum(row['hot_misses'] for row in client['steps'])
    client['hot_reads'] = client['hot_hits'] + client['hot_misses']
    at = 3 + delay + .002
    client['first_reuse'] = [
        {'namespace_hex': ns.hex(), 'probe_started_sec': at,
         'write_to_probe_sec': at - written,
         'hits': 0 if all_miss else 32, 'misses': 32 if all_miss else 0}
        for ns, written in zip((b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe'), (1, 2))]
    return client


def delayed_metrics(client):
    hits = client['hits'] + client['hot_hits'] + 18304
    misses = client['misses'] + client['hot_misses']
    return good_metrics().replace('12128', str(hits)).replace('20456', str(hits + misses)).replace('8328', str(misses))


def mixed_client(reuse_miss=False, hot_miss=False):
    client = sustained_client()
    client.update(filler_warmup_reads=18112, reuse_hits=0 if reuse_miss else 96,
                  reuse_misses=96 if reuse_miss else 0, reuse_reads=96, reuse_cohorts=[])
    if hot_miss:
        client.update(hot_hits=128, hot_misses=12800)
    for row in client['steps']:
        row.update(reuse_probes=[], hot_hits=0 if hot_miss else 64,
                   hot_misses=64 if hot_miss else 0)
    for index, delay in enumerate((45, 90, 185)):
        client['steps'][delay]['reuse_probes'] = [
            {'delay_sec': delay, 'probe_started_sec': delay + .003,
             'hits': 0 if reuse_miss else 32, 'misses': 32 if reuse_miss else 0}]
        client['reuse_cohorts'].append({
            'delay_sec': delay, 'start_index': 32 + index * 16, 'count_per_namespace': 16,
            'write_completed_sec': [1.0, 2.0], 'first_reuse': [
                {'namespace_hex': ns.hex(), 'probe_started_sec': 3 + delay + .004 + i * .001,
                 'write_to_probe_sec': 3 + delay + .004 + i * .001 - written,
                 'hits': 0 if reuse_miss else 16, 'misses': 16 if reuse_miss else 0}
                for i, (ns, written) in enumerate(zip(
                    (b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe'), (1, 2)))]})
    return client


def mixed_metrics(client):
    hits = client['hits'] + client['hot_hits'] + 18112 + client['reuse_hits']
    misses = client['misses'] + client['hot_misses'] + client['reuse_misses']
    return good_metrics().replace('12128', str(hits)).replace('20456', str(hits + misses)).replace('8328', str(misses))


def arrivals_client(reuse_miss=False, hot_miss=False):
    client = sustained_client()
    client.update(filler_warmup_reads=18304, reuse_hits=0 if reuse_miss else 384,
                  reuse_misses=384 if reuse_miss else 0, reuse_reads=384, reuse_cohorts=[])
    if hot_miss:
        client.update(hot_hits=128, hot_misses=12800)
    for row in client['steps']:
        row.update(reuse_probes=[], hot_hits=0 if hot_miss else 64,
                   hot_misses=64 if hot_miss else 0)
    for index in range(12):
        delay = (45, 90, 185)[index % 3]
        first = index + delay
        writes = [3 + index + .002, 3 + index + .003]
        client['steps'][first]['reuse_probes'] = [
            {'admission_step': index, 'delay_sec': delay, 'probe_started_sec': first + .004,
             'hits': 0 if reuse_miss else 32, 'misses': 32 if reuse_miss else 0}]
        client['reuse_cohorts'].append({
            'admission_step': index, 'delay_sec': delay, 'start_index': 4608 + index * 16,
            'count_per_namespace': 16, 'write_completed_sec': writes,
            'due_sec': index + .003 + delay, 'first_reuse': [
                {'namespace_hex': ns.hex(), 'probe_started_sec': 3 + first + .005 + i * .001,
                 'write_to_probe_sec': 3 + first + .005 + i * .001 - writes[i],
                 'hits': 0 if reuse_miss else 16, 'misses': 16 if reuse_miss else 0}
                for i, ns in enumerate((b'disk-validation\x00A\xff', b'disk-validation\x00B\xfe'))]})
    return client


def arrivals_metrics(client):
    hits = client['hits'] + client['hot_hits'] + 18304 + client['reuse_hits']
    misses = client['misses'] + client['hot_misses'] + client['reuse_misses']
    return good_metrics().replace('12128', str(hits)).replace('20456', str(hits + misses)).replace('8328', str(misses))


def multisize_client(warm_miss=False, hot_miss=False):
    rows = [{'payload_bytes': length, 'slot_size': slot, 'put_attempts': 6400, 'put_acks': 6400,
             'admission_hits': 32, 'admission_misses': 0,
             'warmup_hits': 0 if warm_miss else 64, 'warmup_misses': 64 if warm_miss else 0,
             'hot_hits': 0 if hot_miss else 11776, 'hot_misses': 11776 if hot_miss else 0,
             'scan_hits': 5000, 'scan_misses': 1400}
            for length, slot in ((4096, 1048576), (1048577, 2097152))]
    return {**good_client(), 'hits': 10000, 'misses': 2800,
            'admission_hits': 64, 'admission_misses': 0,
            'warmup_hits': sum(row['warmup_hits'] for row in rows),
            'warmup_misses': sum(row['warmup_misses'] for row in rows),
            'hot_hits': sum(row['hot_hits'] for row in rows),
            'hot_misses': sum(row['hot_misses'] for row in rows), 'hot_reads': 23552,
            'multisize': rows}


def multisize_metrics(client):
    hits = client['hits'] + client['hot_hits'] + client['warmup_hits'] + client['admission_hits']
    misses = client['misses'] + client['hot_misses'] + client['warmup_misses'] + client['admission_misses']
    text = good_metrics().replace('12128', str(hits)).replace('20456', '36544').replace('8328', str(misses))
    return text + '\n'.join(('dfkv_cache_put_total 12800', 'dfkv_bytes_written_total 6737107200',
        'dfkv_slab_classes 6',
        'dfkv_slab_class_put_total{slot_size="1048576"} 6400',
        'dfkv_slab_class_put_total{slot_size="2097152"} 6400',
        'dfkv_slab_class_put_total{slot_size="4194304"} 0')) + '\n'


def multisize_watermark_metrics(client):
    text = multisize_metrics(client)
    for metric, value in (('capacity_bytes', 4294967296), ('watermark_high_bytes', 2147483600),
                          ('watermark_low_bytes', 1717986880)):
        for index, disk in enumerate(('a', 'b', 'c')):
            text += 'dfkv_slab_disk_' + metric + '{disk="' + disk + '",disk_index="' + str(index) + '"} ' + str(value) + '\n'
    return text


def refill_client(mode='partial', verify_miss=False):
    client = arrivals_client()
    stats = {'first_hits': 0, 'first_misses': 0, 'put_attempts': 0, 'put_acks': 0,
             'attempted_payload_bytes': 0, 'acked_payload_bytes': 0,
             'verify_hits': 0, 'verify_misses': 0, 'objects': [],
             'baseline_payload_bytes': 63963136}
    client['refill'] = stats
    for step in client['steps']:
        for event in step['reuse_probes']:
            cohort = client['reuse_cohorts'][event['admission_step']]
            for ns_index, outcome in enumerate(cohort['first_reuse']):
                hits = 0
                for offset in range(16):
                    hit = int(mode == 'hit' or (mode == 'partial' and offset % 2))
                    at = outcome['probe_started_sec'] + offset * .00001
                    row = {'namespace_hex': outcome['namespace_hex'],
                           'index': cohort['start_index'] + offset,
                           'first_get_started_sec': at, 'first_hit': hit,
                           'put_attempted': not hit, 'put_acked': not hit,
                           'verify_hit': None}
                    if not hit:
                        row.update(put_ack_sec=at + .000001,
                                   verify_started_sec=at + .000002,
                                   verify_hit=int(not verify_miss))
                    stats['objects'].append(row)
                    hits += hit
                outcome.update(hits=hits, misses=16 - hits)
            event.update(hits=sum(o['hits'] for o in cohort['first_reuse']),
                         misses=sum(o['misses'] for o in cohort['first_reuse']))
    client['reuse_hits'] = sum(row['first_hit'] for row in stats['objects'])
    client['reuse_misses'] = 384 - client['reuse_hits']
    misses = client['reuse_misses']
    stats.update(first_hits=client['reuse_hits'], first_misses=misses,
                 put_attempts=misses, put_acks=misses,
                 attempted_payload_bytes=misses * 4096, acked_payload_bytes=misses * 4096,
                 verify_hits=0 if verify_miss else misses, verify_misses=misses if verify_miss else 0,
                 logical_payload_write_amplification=1 + misses / 15616)
    return client


def refill_metrics(client):
    hits = client['hits'] + client['hot_hits'] + 18304 + client['reuse_hits'] + client['refill']['verify_hits']
    misses = client['misses'] + client['hot_misses'] + client['reuse_misses'] + client['refill']['verify_misses']
    text = good_metrics().replace('12128', str(hits)).replace('20456', str(hits + misses)).replace('8328', str(misses))
    return text + ('dfkv_cache_put_total ' + str(15616 + client['refill']['put_acks']) + '\n' +
                   'dfkv_bytes_written_total ' + str(63963136 + client['refill']['acked_payload_bytes']) + '\n')


class QuickEvictionTest(unittest.TestCase):
    def test_multisize_watermark_is_explicit_and_multisize_only(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            self.assertFalse(runner.parse_args(argv).multisize_watermark)
            for policy in ('legacy', 'lifetime'):
                args = runner.parse_args(argv + ['--scenario', 'multisize-pressure',
                                                 '--policy', policy, '--multisize-watermark'])
                self.assertTrue(args.multisize_watermark)
            for scenario in ('quick-pressure', 'sustained-pressure', 'mixed-pressure', 'arrivals-pressure'):
                with self.subTest(scenario=scenario), contextlib.redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit):
                    runner.parse_args(argv + ['--scenario', scenario, '--multisize-watermark'])

    def test_multisize_watermark_requires_actual_activity_without_weakening_old_mode(self):
        client = multisize_client(True, True)
        text = multisize_watermark_metrics(client)
        for policy in ('legacy', 'lifetime'):
            result = runner.check_result(text, client, policy, 'multisize-pressure',
                                         multisize_watermark=True)
            self.assertEqual(result['watermark_evictions'], 900)
            zero = text.replace('dfkv_slab_watermark_evictions_total 900',
                                'dfkv_slab_watermark_evictions_total 0')
            self.assertEqual(runner.check_result(zero, client, policy, 'multisize-pressure')
                             ['watermark_evictions'], 0)
            for invalid in (zero, text.replace('dfkv_slab_watermark_evictions_total 900', ''),
                            text.replace('dfkv_slab_watermark_evictions_total 900',
                                         'dfkv_slab_watermark_evictions_total nan')):
                with self.subTest(policy=policy, invalid=invalid), self.assertRaises(RuntimeError):
                    runner.check_result(invalid, client, policy, 'multisize-pressure',
                                        multisize_watermark=True)
        for invalid in (None, 0, 1, 'true'):
            with self.subTest(value=invalid), self.assertRaises(RuntimeError):
                runner.check_result(text, client, scenario='multisize-pressure',
                                    multisize_watermark=invalid)
        with self.assertRaises(RuntimeError):
            runner.check_result(good_metrics(), good_client(), multisize_watermark=True)

    def test_multisize_watermark_rejects_missing_duplicate_or_wrong_effective_thresholds(self):
        client = multisize_client()
        text = multisize_watermark_metrics(client)
        runner.check_multisize_watermarks(text)
        mutations = [
            ('dfkv_slab_disk_capacity_bytes{disk="a",disk_index="0"} 4294967296', ''),
            ('dfkv_slab_disk_watermark_high_bytes{disk="b",disk_index="1"} 2147483600', ''),
            ('dfkv_slab_disk_watermark_low_bytes{disk="c",disk_index="2"} 1717986880', ''),
            ('disk="a",disk_index="0"', 'disk="other",disk_index="0"'),
            ('disk="c",disk_index="2"', 'disk="c",disk_index="1"'),
            ('dfkv_slab_disk_watermark_high_bytes{disk="a",disk_index="0"}',
             'dfkv_slab_disk_watermark_high_bytes{disk="other",disk_index="0"}'),
            ('4294967296', '8589934592'), ('2147483600', '2147483648'),
            ('1717986880', '1717986918'), ('1717986880', 'nan')]
        for before, after in mutations:
            with self.subTest(before=before, after=after), self.assertRaises(RuntimeError):
                runner.check_result(text.replace(before, after), client, scenario='multisize-pressure',
                                    multisize_watermark=True)
        duplicate = text + 'dfkv_slab_disk_watermark_low_bytes{disk="a",disk_index="0"} 1717986880\n'
        with self.assertRaises(RuntimeError):
            runner.check_multisize_watermarks(duplicate)

    def test_multisize_watermark_rejects_wrong_effective_threshold_before_any_client_writes(self):
        with tempfile.TemporaryDirectory() as temp:
            args = runner.parse_args(self.arguments(Path(temp)) +
                                     ['--data-name', 'fixed', '--scenario', 'multisize-pressure',
                                      '--multisize-watermark'])
            text = multisize_watermark_metrics(multisize_client()).replace('2147483600', '3951369824')
            for label, root in zip(('a', 'b', 'c'), args.data_root):
                text = text.replace('disk="' + label + '"', 'disk=' + json.dumps(str(root / 'fixed')))
            server = mock.Mock()
            server.poll.return_value = None
            server.wait.return_value = 0
            server.returncode = 0
            response = mock.MagicMock()
            response.__enter__.return_value.status = 200
            response.__enter__.return_value.read.return_value = text.encode()
            with mock.patch.object(runner, 'check_ports'), \
                    mock.patch.object(runner.subprocess, 'Popen', return_value=server), \
                    mock.patch.object(runner.subprocess, 'run') as client_run, \
                    mock.patch.object(runner.urllib.request, 'urlopen', return_value=response):
                result = runner.run(args)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('watermark', result['error'])
            client_run.assert_not_called()
            server.terminate.assert_called_once()

    def test_multisize_watermark_override_and_environment_are_frozen_even_on_start_failure(self):
        for enabled in (False, True):
            for policy in ('legacy', 'lifetime'):
                with self.subTest(enabled=enabled, policy=policy), tempfile.TemporaryDirectory() as temp:
                    argv = self.arguments(Path(temp)) + ['--scenario', 'multisize-pressure', '--policy', policy]
                    args = runner.parse_args(argv + (['--multisize-watermark'] if enabled else []))
                    with mock.patch.object(runner, 'check_ports'), \
                            mock.patch.dict(runner.os.environ, {'DFKV_SLAB_EVICT_LOW_PCT': '98',
                                                               'DFKV_SLAB_EVICT_HIGH_PCT': '99'}), \
                            mock.patch.object(runner.subprocess, 'Popen', side_effect=OSError('spawn failed')) as popen:
                        result = runner.run(args)
                    override = {'low': 40, 'high': 50} if enabled else None
                    env = popen.call_args.kwargs['env']
                    self.assertEqual(env.get('DFKV_SLAB_EVICT_LOW_PCT'), '40' if enabled else None)
                    self.assertEqual(env.get('DFKV_SLAB_EVICT_HIGH_PCT'), '50' if enabled else None)
                    self.assertEqual(result['status'], 'failed')
                    self.assertEqual(result['multisize_watermark'], enabled)
                    self.assertEqual(result['watermark_pct_override'], override)
                    case = json.loads((args.output / 'case.json').read_text())
                    self.assertEqual(case['multisize_watermark'], enabled)
                    self.assertEqual(case['watermark_pct_override'], override)
                    saved = json.loads((args.output / 'result.json').read_text())
                    self.assertEqual(saved['watermark_pct_override'], override)

    def test_multisize_watermark_run_passes_only_with_real_counter_and_stops_server(self):
        for policy in ('legacy', 'lifetime'):
            for victims in (0, 900):
                with self.subTest(policy=policy, victims=victims), tempfile.TemporaryDirectory() as temp:
                    args = runner.parse_args(self.arguments(Path(temp)) +
                                             ['--data-name', 'fixed', '--scenario', 'multisize-pressure',
                                              '--policy', policy, '--multisize-watermark'])
                    client = multisize_client(True, True)
                    text = multisize_watermark_metrics(client).replace('dfkv_slab_watermark_evictions_total 900',
                                                             'dfkv_slab_watermark_evictions_total ' + str(victims))
                    for label, root in zip(('a', 'b', 'c'), args.data_root):
                        text = text.replace('disk="' + label + '"', 'disk=' + json.dumps(str(root / 'fixed')))
                    server = mock.Mock()
                    server.poll.return_value = None
                    server.wait.return_value = 0
                    server.returncode = 0
                    response = mock.MagicMock()
                    response.__enter__.return_value.status = 200
                    response.__enter__.return_value.read.return_value = text.encode()

                    def complete_client(command, **kwargs):
                        trace = args.output / 'trace'
                        trace.mkdir()
                        (trace / 'result.json').write_text(json.dumps(client))
                        return subprocess.CompletedProcess(command, 0)

                    with mock.patch.object(runner, 'check_ports'), \
                            mock.patch.object(runner.subprocess, 'Popen', return_value=server), \
                            mock.patch.object(runner.subprocess, 'run', side_effect=complete_client), \
                            mock.patch.object(runner.urllib.request, 'urlopen', return_value=response):
                        result = runner.run(args)
                    self.assertEqual(result['status'], 'passed' if victims else 'failed')
                    self.assertEqual(result['server_exit'], 0)
                    self.assertTrue(result['multisize_watermark'])
                    self.assertEqual(result['watermark_pct_override'], {'low': 40, 'high': 50})
                    if victims:
                        self.assertEqual(result['watermark_evictions'], victims)
                    else:
                        self.assertIn('watermark', result['error'])
                    server.terminate.assert_called_once()

    def test_multisize_warm_and_hot_losses_remain_separate_valid_observations(self):
        for warm_miss, hot_miss in ((False, False), (True, False), (True, True)):
            with self.subTest(warm_miss=warm_miss, hot_miss=hot_miss):
                client = multisize_client(warm_miss, hot_miss)
                text = multisize_metrics(client).replace('dfkv_slab_watermark_evictions_total 900',
                                                         'dfkv_slab_watermark_evictions_total 0')
                text = text.replace('dfkv_slab_extent_steals_total 0', 'dfkv_slab_extent_steals_total 3')
                result = runner.check_result(text, client, scenario='multisize-pressure')
                self.assertEqual(result['multisize_gets'], 36544)
                self.assertEqual(result['multisize_puts'], 12800)
                self.assertEqual(result['watermark_evictions'], 0)
                self.assertEqual(result['extent_steals'], 3)

    def test_multisize_rejects_forged_layout_counts_and_aggregate_types(self):
        mutations = [('payload_bytes', 4097), ('slot_size', 2097152), ('put_attempts', 6399),
                     ('put_acks', 6399), ('put_acks', True), ('admission_hits', 31), ('admission_misses', True), ('warmup_hits', 63),
                     ('warmup_misses', -1), ('hot_hits', 11775), ('hot_misses', 1),
                     ('scan_hits', 4999), ('scan_misses', 1401), ('scan_hits', 5000.0)]
        for field, value in mutations:
            with self.subTest(field=field, value=value), self.assertRaises(RuntimeError):
                client = multisize_client()
                client['multisize'][0][field] = value
                runner.check_result(multisize_metrics(client), client, scenario='multisize-pressure')
        for field, value in (('admission_hits', 63), ('admission_misses', True), ('warmup_hits', 127), ('warmup_misses', True),
                             ('hot_reads', 23551), ('hits', 9999)):
            with self.subTest(field=field, value=value), self.assertRaises(RuntimeError):
                client = multisize_client()
                client[field] = value
                runner.check_result(multisize_metrics(client), client, scenario='multisize-pressure')
        for rows in (None, [], multisize_client()['multisize'][:1],
                     multisize_client()['multisize'][::-1]):
            with self.subTest(rows=rows), self.assertRaises(RuntimeError):
                client = multisize_client()
                client['multisize'] = rows
                runner.check_result(multisize_metrics(client), client, scenario='multisize-pressure')
        client = multisize_client(True, True)
        client['multisize'][0].update(admission_hits=0, admission_misses=32, scan_hits=0, scan_misses=6400)
        client.update(admission_hits=32, admission_misses=32)
        client.update(hits=5000, misses=7800)
        with self.assertRaisesRegex(RuntimeError, 'successful read'):
            runner.check_result(multisize_metrics(client), client, scenario='multisize-pressure')

    def test_multisize_requires_actual_class_and_full_get_put_payload_metrics(self):
        client = multisize_client(True, True)
        changes = [('dfkv_cache_put_total 12800', 'dfkv_cache_put_total 12799'),
                   ('dfkv_bytes_written_total 6737107200', 'dfkv_bytes_written_total 6737107199'),
                   ('dfkv_slab_classes 6', 'dfkv_slab_classes 1'),
                   ('dfkv_slab_class_put_total{slot_size="1048576"} 6400', ''),
                   ('dfkv_slab_class_put_total{slot_size="2097152"} 6400',
                    'dfkv_slab_class_put_total{slot_size="2097152"} 6399'),
                   ('dfkv_slab_class_put_total{slot_size="4194304"} 0',
                    'dfkv_slab_class_put_total{slot_size="4194304"} 1'),
                   ('dfkv_slab_class_put_total{slot_size="4194304"} 0',
                    'dfkv_slab_class_put_total{slot_size="1048576"} 0'),
                   ('dfkv_slab_class_put_total{slot_size="4194304"} 0',
                    'dfkv_slab_class_put_total{other="4194304"} 0'),
                   ('dfkv_rdma_pull_requests_dispatched_total 36544',
                    'dfkv_rdma_pull_requests_dispatched_total 36352'),
                   ('result="not_found"} 26480', 'result="not_found"} 26352'),
                   ('dfkv_slab_lifetime_read_success_total 10064',
                    'dfkv_slab_lifetime_read_success_total 10000'),
                   ('dfkv_errors_total 0', 'dfkv_errors_total 1')]
        for before, after in changes:
            with self.subTest(before=before), self.assertRaises(RuntimeError):
                text = multisize_metrics(client)
                self.assertIn(before, text)
                runner.check_result(text.replace(before, after), client, scenario='multisize-pressure')
        for scenario, client in (('quick-pressure', multisize_client()),
                                 ('multisize-pressure', good_client())):
            with self.subTest(scenario=scenario), self.assertRaises(RuntimeError):
                runner.check_result(good_metrics(), client, scenario=scenario)

    def test_multisize_options_reject_reuse_refill_and_quiet_delay(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp)) + ['--scenario', 'multisize-pressure']
            self.assertEqual(runner.parse_args(argv).scenario, 'multisize-pressure')
            for extra in (['--first-reuse-sec', '45'], ['--refill-on-miss'],
                          ['--pressure-delay-sec', '1']):
                with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + extra)

    def test_refill_counts_and_verification_misses_are_separate_valid_observations(self):
        for mode in ('hit', 'partial', 'miss'):
            for verify_miss in (False, True):
                client = refill_client(mode, verify_miss)
                with self.subTest(mode=mode, verify_miss=verify_miss):
                    checked = runner.check_result(refill_metrics(client), client,
                                                  scenario='arrivals-pressure', refill_on_miss=True)
                    self.assertEqual(checked['refill_put_acks'], client['reuse_misses'])
                    self.assertEqual(checked['refill_verify_hits'], client['refill']['verify_hits'])
                    self.assertEqual(checked['refill_verify_misses'], client['refill']['verify_misses'])
                    self.assertEqual(checked['reuse_reads'], 384)
                    self.assertEqual(sum(checked['terminal_results'].values()),
                                     47232 + client['reuse_misses'])

    def test_refill_requires_explicit_matching_flag_and_scenario(self):
        for flag, client, scenario in ((False, refill_client(), 'arrivals-pressure'),
                                       (True, arrivals_client(), 'arrivals-pressure'),
                                       (True, good_client(), 'quick-pressure')):
            with self.subTest(flag=flag, scenario=scenario), self.assertRaisesRegex(RuntimeError, 'refill'):
                runner.check_result(good_metrics(), client, scenario=scenario, refill_on_miss=flag)

    def test_refill_rejects_identity_timing_flags_outcomes_and_forged_aggregates(self):
        changes = ('missing', 'duplicate', 'order', 'namespace', 'index_bool', 'first_bool',
                   'pending', 'early', 'late', 'nan', 'timestamp_bool', 'backwards',
                   'ack_early', 'verify_early', 'verify_late', 'flag_int', 'no_ack',
                   'no_attempt', 'verify_bool', 'verify_pending', 'hit_refilled',
                   'hit_timestamp', 'first_sum', 'attempts', 'acks', 'bytes', 'attempt_bytes',
                   'verify_sum', 'baseline', 'ratio', 'ratio_nan', 'ratio_bool', 'count_bool')
        for change in changes:
            client = refill_client()
            stats = client['refill']
            row = stats['objects'][0]
            if change == 'missing':
                stats['objects'].pop()
            elif change == 'duplicate':
                stats['objects'][1] = dict(row)
            elif change == 'order':
                stats['objects'].reverse()
            elif change == 'namespace':
                row['namespace_hex'] = 'bad'
            elif change == 'index_bool':
                row['index'] = True
            elif change == 'first_bool':
                row['first_hit'] = False
            elif change == 'pending':
                row['first_hit'] = None
            elif change == 'early':
                row['first_get_started_sec'] -= .01
            elif change == 'late':
                row['first_get_started_sec'] += 1
            elif change == 'nan':
                row['first_get_started_sec'] = float('nan')
            elif change == 'timestamp_bool':
                row['put_ack_sec'] = True
            elif change == 'backwards':
                stats['objects'][2]['first_get_started_sec'] = row['first_get_started_sec']
            elif change == 'ack_early':
                row['put_ack_sec'] = row['first_get_started_sec'] - .001
            elif change == 'verify_early':
                row['verify_started_sec'] = row['first_get_started_sec']
            elif change == 'verify_late':
                row['verify_started_sec'] += 1
            elif change == 'flag_int':
                row['put_attempted'] = 1
            elif change == 'no_ack':
                row['put_acked'] = False
            elif change == 'no_attempt':
                row['put_attempted'] = False
            elif change == 'verify_bool':
                row['verify_hit'] = True
            elif change == 'verify_pending':
                row['verify_hit'] = None
            elif change == 'hit_refilled':
                stats['objects'][1]['put_attempted'] = True
            elif change == 'hit_timestamp':
                stats['objects'][1]['put_ack_sec'] = row['put_ack_sec']
            elif change == 'first_sum':
                stats['first_hits'] += 1
            elif change == 'attempts':
                stats['put_attempts'] += 1
            elif change == 'acks':
                stats['put_acks'] -= 1
            elif change == 'bytes':
                stats['acked_payload_bytes'] += 1
            elif change == 'attempt_bytes':
                stats['attempted_payload_bytes'] += 1
            elif change == 'verify_sum':
                stats['verify_hits'] -= 1
            elif change == 'baseline':
                stats['baseline_payload_bytes'] -= 4096
            elif change == 'ratio':
                stats['logical_payload_write_amplification'] += .01
            elif change == 'ratio_nan':
                stats['logical_payload_write_amplification'] = float('nan')
            elif change == 'ratio_bool':
                stats['logical_payload_write_amplification'] = True
            elif change == 'count_bool':
                stats['put_acks'] = True
            with self.subTest(change=change), self.assertRaises(RuntimeError):
                runner.check_result(refill_metrics(client), client, scenario='arrivals-pressure', refill_on_miss=True)

    def test_refill_terminal_and_heat_denominators_include_verification_gets(self):
        for verify_miss in (False, True):
            client = refill_client('miss', verify_miss)
            text = refill_metrics(client)
            good = runner.check_result(text, client, scenario='arrivals-pressure', refill_on_miss=True)
            for metric in ('dfkv_rdma_pull_requests_dispatched_total',
                           'dfkv_rdma_pull_requests_terminal_total', 'dfkv_slab_lifetime_read_success_total',
                           'dfkv_cache_put_total', 'dfkv_bytes_written_total'):
                lines = text.splitlines()
                for index, line in enumerate(lines):
                    if line.startswith(metric) and not line.endswith(' 0'):
                        name, value = line.rsplit(' ', 1)
                        lines[index] = name + ' ' + str(float(value) - 1)
                        break
                with self.subTest(verify_miss=verify_miss, metric=metric), self.assertRaises(RuntimeError):
                    runner.check_result('\n'.join(lines), client, scenario='arrivals-pressure', refill_on_miss=True)
            self.assertEqual(good['refill_put_acks'], 384)

    def test_refill_cli_is_opt_in_and_arrivals_only(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            self.assertFalse(runner.parse_args(argv).refill_on_miss)
            self.assertTrue(runner.parse_args(argv + ['--scenario', 'arrivals-pressure',
                                                      '--refill-on-miss']).refill_on_miss)
            for scenario in ('quick-pressure', 'sustained-pressure', 'mixed-pressure'):
                with self.subTest(scenario=scenario), contextlib.redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit):
                    runner.parse_args(argv + ['--scenario', scenario, '--refill-on-miss'])

    def test_arrivals_cohort_counts_and_all_misses_remain_valid_observations(self):
        for reuse_miss in (False, True):
            for hot_miss in (False, True):
                client = arrivals_client(reuse_miss, hot_miss)
                checked = runner.check_result(arrivals_metrics(client), client, scenario='arrivals-pressure')
                self.assertEqual(checked['reuse_reads'], 384)
                self.assertEqual(checked['reuse_hits'], 0 if reuse_miss else 384)
                self.assertEqual(checked['reuse_misses'], 384 if reuse_miss else 0)
                self.assertEqual(checked['sustained_steps'], 200)

    def test_arrivals_checker_rejects_identity_ack_age_and_event_forgery(self):
        for mutation in ('cohort_missing', 'cohort_order', 'identity', 'identity_bool', 'overlap',
                         'size', 'delay', 'ack_prefill', 'ack_future', 'ack_order', 'due',
                         'due_nan', 'ack_bool', 'early', 'duplicate', 'missing', 'late',
                         'event_identity', 'event_delay', 'event_count', 'age', 'namespace',
                         'outcome', 'reads', 'hits', 'filler', 'partial', 'outcome_time',
                         'outcome_early', 'cohort_id_float', 'start_bool', 'missing_probes',
                         'ack_deadline_early', 'filler_float'):
            client = arrivals_client()
            cohort = client['reuse_cohorts'][0]
            event = client['steps'][45]['reuse_probes'][0]
            if mutation == 'cohort_missing':
                client['reuse_cohorts'].pop()
            elif mutation == 'cohort_order':
                client['reuse_cohorts'].reverse()
            elif mutation == 'identity':
                cohort['admission_step'] = 1
            elif mutation == 'identity_bool':
                cohort['admission_step'] = False
            elif mutation == 'cohort_id_float':
                cohort['admission_step'] = 0.0
            elif mutation == 'start_bool':
                cohort['start_index'] = True
            elif mutation == 'overlap':
                client['reuse_cohorts'][1]['start_index'] = 4608
            elif mutation == 'size':
                cohort['count_per_namespace'] = 32
            elif mutation == 'delay':
                cohort['delay_sec'] = 90
            elif mutation == 'ack_prefill':
                cohort['write_completed_sec'] = [1., 2.]
            elif mutation == 'ack_future':
                cohort['write_completed_sec'][1] = 4.003
            elif mutation == 'ack_order':
                cohort['write_completed_sec'].reverse()
            elif mutation == 'due':
                cohort['due_sec'] -= .001
            elif mutation == 'due_nan':
                cohort['due_sec'] = float('nan')
            elif mutation == 'ack_bool':
                cohort['write_completed_sec'][0] = True
            elif mutation == 'early':
                event['probe_started_sec'] = 44.9
            elif mutation == 'ack_deadline_early':
                event['probe_started_sec'] = 45.002
            elif mutation == 'filler_float':
                client['filler_warmup_reads'] = 18304.0
            elif mutation == 'duplicate':
                client['steps'][46]['reuse_probes'] = [event.copy()]
            elif mutation == 'missing':
                client['steps'][45]['reuse_probes'] = []
            elif mutation == 'late':
                client['steps'][45]['reuse_probes'] = []
                client['steps'][47]['reuse_probes'] = [{**event, 'probe_started_sec': 47.004}]
            elif mutation == 'event_identity':
                event['admission_step'] = 3
            elif mutation == 'event_delay':
                event['delay_sec'] = 90
            elif mutation == 'event_count':
                event['hits'] = 31
            elif mutation == 'age':
                cohort['first_reuse'][0]['write_to_probe_sec'] += 1
            elif mutation == 'namespace':
                cohort['first_reuse'][0]['namespace_hex'] = 'wrong'
            elif mutation == 'outcome':
                cohort['first_reuse'][0].update(hits=15, misses=1)
            elif mutation == 'reads':
                client['reuse_reads'] = 383
            elif mutation == 'hits':
                client['reuse_hits'] = 383
            elif mutation == 'filler':
                client['filler_warmup_reads'] = 18303
            elif mutation == 'partial':
                cohort['first_reuse'].pop()
            elif mutation == 'outcome_time':
                cohort['first_reuse'][1]['probe_started_sec'] = 50
            elif mutation == 'outcome_early':
                cohort['first_reuse'][0]['probe_started_sec'] = 48.001
                cohort['first_reuse'][0]['write_to_probe_sec'] = 44.999
            else:
                client['steps'][0].pop('reuse_probes')
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                runner.check_arrivals(client)

    def test_arrivals_skipped_decision_can_precede_a_row_end_crossing_the_ack_deadline(self):
        client = arrivals_client()
        event = client['steps'][45]['reuse_probes'].pop()
        # The decision can occur before 45.003 even though row assembly ends at
        # 45.01. The next step is valid; a full extra skipped step is not.
        client['steps'][46]['reuse_probes'] = [{**event, 'probe_started_sec': 46.004}]
        for outcome in client['reuse_cohorts'][0]['first_reuse']:
            outcome['probe_started_sec'] += 1
            outcome['write_to_probe_sec'] += 1
        self.assertEqual(runner.check_arrivals(client)['reuse_reads'], 384)

    def test_arrivals_due_groups_keep_admission_and_namespace_chronology(self):
        client = arrivals_client()
        client['steps'][45]['end_sec'] = 49
        for row in client['steps']:
            row['reuse_probes'] = []
            if row['step'] >= 46:
                row['start_sec'] += 4
                row['end_sec'] += 4
        client['pressure_schedule_elapsed_sec'] += 4
        # A long GET makes groups 0 and 3 due in one row. The driver scans
        # cohorts in admission order, with both namespace GETs before the next.
        for cohort in client['reuse_cohorts']:
            due = cohort['due_sec']
            row = next(row for row in client['steps'] if row['end_sec'] >= due)
            at = max(row['start_sec'] + .003, due + .001)
            row['reuse_probes'].append({'admission_step': cohort['admission_step'],
                'delay_sec': cohort['delay_sec'], 'probe_started_sec': at, 'hits': 32, 'misses': 0})
            for index, outcome in enumerate(cohort['first_reuse']):
                outcome['probe_started_sec'] = 3 + at + .001 * (index + 1)
                outcome['write_to_probe_sec'] = (outcome['probe_started_sec'] -
                                                cohort['write_completed_sec'][index])
        self.assertEqual([p['admission_step'] for p in client['steps'][45]['reuse_probes']], [0, 3])
        self.assertEqual(runner.check_arrivals(client)['reuse_reads'], 384)
        for mutation in ('event_order', 'namespace_order', 'next_group_before_prior_namespace'):
            forged = json.loads(json.dumps(client))
            if mutation == 'event_order':
                forged['steps'][45]['reuse_probes'].reverse()
            elif mutation == 'namespace_order':
                forged['reuse_cohorts'][0]['first_reuse'].reverse()
            else:
                cohort = forged['reuse_cohorts'][0]
                outcome = cohort['first_reuse'][1]
                outcome['probe_started_sec'] = 3 + 48.006
                outcome['write_to_probe_sec'] = outcome['probe_started_sec'] - cohort['write_completed_sec'][1]
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                runner.check_arrivals(forged)

    def test_arrivals_terminals_and_heat_use_the_full_47232_get_denominator(self):
        for all_miss in (False, True):
            client = arrivals_client(all_miss)
            text = arrivals_metrics(client)
            hits = client['hits'] + client['hot_hits'] + 18304 + client['reuse_hits']
            misses = client['misses'] + client['reuse_misses']
            for before, after in (
                    ('dispatched_total 47232', 'dispatched_total 46752'),
                    (f'result="success_nonempty"}} {hits}', f'result="success_nonempty"}} {hits - 1}'),
                    (f'result="not_found"}} {misses}', f'result="not_found"}} {misses - 1}'),
                    (f'dfkv_slab_lifetime_read_success_total {hits}',
                     f'dfkv_slab_lifetime_read_success_total {hits - 1}')):
                self.assertIn(before, text)
                with self.subTest(all_miss=all_miss, changed=before), self.assertRaises(RuntimeError):
                    runner.check_result(text.replace(before, after), client, scenario='arrivals-pressure')

    def test_arrivals_options_reject_other_delay_modes(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp)) + ['--scenario', 'arrivals-pressure']
            self.assertEqual(runner.parse_args(argv).scenario, 'arrivals-pressure')
            for extra in (['--first-reuse-sec', '45'], ['--pressure-delay-sec', '1']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + extra)

    def test_mixed_cohorts_and_all_misses_are_distinct_valid_observations(self):
        for pending_miss in (False, True):
            for hot_miss in (False, True):
                client = mixed_client(pending_miss, hot_miss)
                checked = runner.check_result(mixed_metrics(client), client, scenario='mixed-pressure')
                self.assertEqual(checked['reuse_reads'], 96)
                self.assertEqual(checked['reuse_misses'], 96 if pending_miss else 0)
                self.assertEqual(client['hot_reads'], 12928)

    def test_mixed_checker_rejects_early_duplicate_missing_or_forged_cohorts(self):
        for mutation in ('early', 'duplicate', 'missing', 'late', 'age', 'namespace',
                         'overlap', 'outcome', 'reuse_denominator', 'filler', 'unknown', 'event_count'):
            client = mixed_client()
            if mutation == 'early':
                client['steps'][45]['reuse_probes'][0]['probe_started_sec'] = 44.9
            elif mutation == 'duplicate':
                client['steps'][46]['reuse_probes'] = client['steps'][45]['reuse_probes']
            elif mutation in ('missing', 'late'):
                event = client['steps'][45]['reuse_probes'].pop()
                if mutation == 'late':
                    client['steps'][46]['reuse_probes'] = [{**event, 'probe_started_sec': 46.003}]
            elif mutation == 'age':
                client['reuse_cohorts'][0]['first_reuse'][0]['write_to_probe_sec'] += 1
            elif mutation == 'namespace':
                client['reuse_cohorts'][0]['first_reuse'][0]['namespace_hex'] = 'wrong'
            elif mutation == 'overlap':
                client['reuse_cohorts'][1]['start_index'] = 32
            elif mutation == 'outcome':
                client['reuse_cohorts'][0]['first_reuse'][0].update(hits=15, misses=1)
            elif mutation == 'reuse_denominator':
                client['reuse_reads'] = 95
            elif mutation == 'filler':
                client['filler_warmup_reads'] = 18111
            elif mutation == 'unknown':
                client['steps'][45]['reuse_probes'][0]['delay_sec'] = 46
            else:
                client['steps'][45]['reuse_probes'][0]['hits'] = 31
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                runner.check_mixed(client)

    def test_mixed_terminals_and_heat_include_first_reuse_outcomes(self):
        for misses in (False, True):
            client = mixed_client(misses)
            text = mixed_metrics(client)
            total_success = client['hits'] + client['hot_hits'] + 18112 + client['reuse_hits']
            total_missing = client['misses'] + client['hot_misses'] + client['reuse_misses']
            changes = [
                ('dispatched_total 46752', 'dispatched_total 46656'),
                (f'result="success_nonempty"}} {total_success}',
                 f'result="success_nonempty"}} {total_success - 1}'),
                (f'result="not_found"}} {total_missing}', f'result="not_found"}} {total_missing - 1}'),
                (f'dfkv_slab_lifetime_read_success_total {total_success}',
                 f'dfkv_slab_lifetime_read_success_total {total_success - 1}'),
            ]
            for before, after in changes:
                self.assertIn(before, text)
                with self.subTest(misses=misses, changed=before), self.assertRaises(RuntimeError):
                    runner.check_result(text.replace(before, after), client, scenario='mixed-pressure')

    def test_mixed_options_reject_external_delay_overrides(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp)) + ['--scenario', 'mixed-pressure']
            self.assertEqual(runner.parse_args(argv).scenario, 'mixed-pressure')
            for extra in (['--first-reuse-sec', '45'], ['--pressure-delay-sec', '1']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + extra)

    def test_delayed_first_reuse_counts_and_all_miss_are_valid_observations(self):
        for delay in (45, 90, 185):
            for all_miss in (False, True):
                client = delayed_client(delay, all_miss)
                checked = runner.check_result(delayed_metrics(client), client,
                    scenario='sustained-pressure', first_reuse_sec=delay)
                self.assertEqual(checked['sustained_steps'], 200)
                self.assertEqual(client['hot_reads'], (200 - delay) * 64)

    def test_skipped_step_can_finish_after_delay_without_an_early_probe(self):
        client = delayed_client()
        client['steps'][89]['end_sec'] = 90.01
        for row in client['steps'][90:]:
            row['start_sec'] += .1
            row['end_sec'] += .1
            row['hot_probe_started_sec'] += .1
        client['pressure_schedule_elapsed_sec'] += .1
        for probe in client['first_reuse']:
            probe['probe_started_sec'] += .1
            probe['write_to_probe_sec'] += .1
        self.assertEqual(runner.check_steps(client, 90)['sustained_steps'], 200)

    def test_delayed_first_reuse_rejects_early_reads_skips_and_forged_ages(self):
        for mutation in ('early', 'skip', 'age', 'cohort', 'warmup', 'first_hits', 'missing_probe'):
            client = delayed_client()
            if mutation == 'early':
                client['steps'][89].update(hot_hits=64, hot_probe_started_sec=89.002)
            elif mutation == 'skip':
                client['steps'][91].update(hot_hits=0, hot_probe_started_sec=None)
            elif mutation == 'age':
                client['first_reuse'][0]['write_to_probe_sec'] += 1
            elif mutation == 'cohort':
                client['first_reuse'][0]['namespace_hex'] = 'wrong'
            elif mutation == 'warmup':
                client['filler_warmup_reads'] -= 1
            elif mutation == 'first_hits':
                client['first_reuse'][0].update(hits=31, misses=1)
            else:
                del client['steps'][90]['hot_probe_started_sec']
            with self.subTest(mutation=mutation), self.assertRaises(RuntimeError):
                runner.check_steps(client, 90)

    def test_first_reuse_options_and_actual_client_command(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            for delay in (0, 45, 90, 185):
                args = runner.parse_args(argv + ['--scenario', 'sustained-pressure',
                                                '--first-reuse-sec', str(delay)])
                self.assertEqual(args.first_reuse_sec, delay)
            for extra in (['--first-reuse-sec', '90'], ['--first-reuse-sec', '1'],
                          ['--first-reuse-sec', '-1'], ['--first-reuse-sec', '90.0']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + extra)
            args = runner.parse_args(argv + ['--scenario', 'sustained-pressure', '--first-reuse-sec', '90'])
            with mock.patch.object(runner, 'check_ports'), mock.patch.object(
                    runner.subprocess, 'Popen', side_effect=OSError('stop before startup')):
                self.assertEqual(runner.run(args)['status'], 'failed')
            case = json.loads((args.output / 'case.json').read_text())
            self.assertEqual(case['first_reuse_sec'], 90)
            command = case['client_command']
            self.assertEqual(command[command.index('--first-reuse-sec') + 1], '90')

    def test_metric_presence_finiteness_and_label_sums(self):
        self.assertEqual(runner.total('counter{disk="a"} 2\ncounter{disk="b"} 3', 'counter'), 5)
        for text in ('', '# counter 0', 'other 0', 'counter NaN', 'counter inf',
                     'counter -1', 'counter invalid'):
            with self.subTest(text=text), self.assertRaises((RuntimeError, ValueError)):
                runner.total(text, 'counter')

    def test_real_activity_and_read_denominators_required(self):
        checked = runner.check_result(good_metrics(), good_client())
        self.assertEqual(checked['evictions'], 904)
        changes = (
            ('dfkv_evictions_total 904', 'dfkv_evictions_total 0'),
            ('dfkv_slab_lifetime_read_success_total 12128', 'dfkv_slab_lifetime_read_success_total 12127'),
            ('dfkv_slab_lifetime_read_applied_total 12128', 'dfkv_slab_lifetime_read_applied_total 12129'),
            ('dfkv_storage_healthy 1', 'dfkv_storage_healthy 0'),
            ('dfkv_slab_healthy 1', 'dfkv_slab_healthy 0'),
            ('dfkv_errors_total 0', 'dfkv_errors_total 1'),
            ('dfkv_uring_init_fallbacks_total 0', 'dfkv_uring_init_fallbacks_total 1'),
            ('dfkv_uring_inflight 0', 'dfkv_uring_inflight 1'),
            ('dfkv_uring_reads_total 12128', 'dfkv_uring_reads_total 0'),
            ('dfkv_uring_completions_total 12128', 'dfkv_uring_completions_total 0'),
            ('dfkv_rdma_pull_requests_dispatched_total 20456',
             'dfkv_rdma_pull_requests_dispatched_total 20457'),
            ('result="io_error"} 0', 'result="io_error"} 1'),
            ('result="not_found"} 8328', 'result="not_found"} 8327'),
            ('result="success_nonempty"} 12128', 'result="success_nonempty"} 12127'),
            ('dfkv_disk_objects{disk="c"} 1', 'dfkv_disk_objects{disk="c"} 0'),
            ('dfkv_disk_objects{disk="c"} 1', 'dfkv_disk_objects{disk="a"} 1'),
        )
        for before, after in changes:
            with self.subTest(after=after), self.assertRaises(RuntimeError):
                runner.check_result(good_metrics().replace(before, after), good_client())
        with self.assertRaisesRegex(RuntimeError, 'missing metric'):
            runner.check_result(good_metrics().replace('dfkv_errors_total 0\n', ''), good_client())

    def test_pressure_can_lose_original_resident_before_heat_notification(self):
        metrics = good_metrics().replace('dfkv_slab_lifetime_read_applied_total 12128',
                                         'dfkv_slab_lifetime_read_applied_total 12127')
        result = runner.check_result(metrics, good_client())
        self.assertEqual(result['read_success'], 12128)
        self.assertEqual(result['read_applied'], 12127)

    def test_legacy_does_not_require_lifetime_heat_but_paths_must_exist(self):
        metrics = '\n'.join(line for line in good_metrics().splitlines()
                            if not line.startswith('dfkv_slab_lifetime_read_'))
        self.assertEqual(runner.check_result(metrics, good_client(), 'legacy')['watermark_evictions'], 900)
        with self.assertRaisesRegex(RuntimeError, 'missing metric'):
            runner.check_result(metrics.replace('dfkv_slab_extent_steals_total 0', ''),
                                good_client(), 'legacy')

    def test_client_failure_wrong_bytes_transport_and_counts_rejected(self):
        for field, value in (('status', 'failed'), ('wrong_bytes', 1),
                             ('transport_mode', ['tcp', 'tcp']), ('hits', 11999),
                             ('hot_reads', 129), ('hot_hits', -1), ('misses', True)):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                runner.check_result(good_metrics(), {**good_client(), field: value})

    def test_fixed_hot_probe_count_required_even_if_counters_reconcile(self):
        client = {**good_client(), 'hot_misses': 7423, 'hot_reads': 7551}
        metrics = good_metrics().replace('dispatched_total 20456', 'dispatched_total 20455')
        metrics = metrics.replace('result="not_found"} 8328', 'result="not_found"} 8327')
        with self.assertRaisesRegex(RuntimeError, 'read denominator'):
            runner.check_result(metrics, client)

    def test_sustained_trace_requires_real_duration_and_exact_cohorts(self):
        result = runner.check_result(sustained_metrics(), sustained_client(),
                                     scenario='sustained-pressure')
        self.assertEqual(result['sustained_steps'], 200)
        self.assertEqual(result['schedule_elapsed_sec'], 199.02)
        self.assertAlmostEqual(result['max_step_gap_sec'], 1)
        self.assertAlmostEqual(result['max_step_lateness_sec'], .001)
        self.assertAlmostEqual(result['max_step_duration_sec'], .009)
        with self.assertRaisesRegex(RuntimeError, 'unknown scenario'):
            runner.check_result(good_metrics(), good_client(), scenario='unknown')
        with self.assertRaisesRegex(RuntimeError, 'read denominator'):
            runner.check_result(good_metrics(), good_client(), scenario='sustained-pressure')

    def test_sustained_trace_rejects_missing_steps_bad_clocks_and_operations(self):
        invalid_rows = (
            ('step', 1), ('step', True), ('cold_written', 31), ('cold_written', True),
            ('hot_hits', 63), ('hot_misses', -1), ('start_sec', float('nan')),
            ('start_sec', -.1), ('start_sec', True), ('start_sec', 6),
            ('end_sec', float('inf')), ('end_sec', 0),
        )
        for field, value in invalid_rows:
            with self.subTest(field=field, value=value), self.assertRaises(RuntimeError):
                client = sustained_client()
                client['steps'][0][field] = value
                runner.check_steps(client)
        for steps in (None, [], sustained_client()['steps'][:-1]):
            with self.subTest(steps_count=None if steps is None else len(steps)), self.assertRaises(RuntimeError):
                runner.check_steps({**sustained_client(), 'steps': steps})
        for field, value in (('pressure_schedule_elapsed_sec', 198),
                             ('pressure_schedule_elapsed_sec', float('nan')),
                             ('pressure_schedule_origin_sec', -1)):
            with self.subTest(field=field), self.assertRaises(RuntimeError):
                runner.check_steps({**sustained_client(), field: value})
        for index, start in ((1, .5), (1, 6.1), (199, 198.99)):
            with self.subTest(index=index, start=start), self.assertRaises(RuntimeError):
                client = sustained_client()
                client['steps'][index]['start_sec'] = start
                client['steps'][index]['end_sec'] = start + .01
                runner.check_steps(client)
        with self.assertRaisesRegex(RuntimeError, 'hot cohort'):
            runner.check_steps({**sustained_client(), 'hot_hits': 12927, 'hot_misses': 1})

    def test_sustained_pressure_options_reject_delay_and_unknown_scenario(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            args = runner.parse_args(argv + ['--scenario', 'sustained-pressure'])
            self.assertEqual(args.scenario, 'sustained-pressure')
            for invalid in (['--scenario', 'sustained-pressure', '--pressure-delay-sec', '1'],
                            ['--scenario', 'unknown']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + invalid)

    def arguments(self, root):
        build = root / 'build'
        build.mkdir()
        for name in ('dfkv_server', 'libdfkv.so'):
            (build / name).write_text('fake-only')
        roots = [root / ('disk' + str(i)) for i in range(3)]
        for path in roots:
            path.mkdir()
        argv = ['--build', str(build), '--output', str(root / 'output')]
        for path in roots:
            argv += ['--data-root', str(path)]
        return argv

    def test_arguments_require_new_output_three_distinct_existing_roots(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            argv = self.arguments(root)
            self.assertEqual(runner.parse_args(argv).policy, 'lifetime')
            for invalid in (argv[:-2], argv + ['--data-root', str(root / 'disk0')],
                            argv[:-1] + [str(root / 'absent')],
                            argv + ['--policy', 'unknown']):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(invalid)
            (root / 'output').mkdir()
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                runner.parse_args(argv)

    def test_delay_and_data_name_arguments_are_bounded(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            args = runner.parse_args(argv + ['--pressure-delay-sec', '185',
                                             '--data-name', 'age-study.1_early'])
            self.assertEqual(args.pressure_delay_sec, 185)
            self.assertEqual(args.data_name, 'age-study.1_early')
            for value in ('0', '300'):
                self.assertEqual(runner.parse_args(argv + ['--pressure-delay-sec', value]).pressure_delay_sec,
                                 int(value))
            invalid = [('--pressure-delay-sec', value) for value in ('-1', '301', '1.5')]
            invalid += [('--data-name', value) for value in
                        ('', '.', '..', '../outside', 'a/b', 'a,b', 'a b', '中文', 'a' * 81)]
            for flag, value in invalid:
                with self.subTest(flag=flag, value=value), \
                        contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    runner.parse_args(argv + [flag, value])

    def test_existing_fixed_data_name_is_not_reused_or_removed(self):
        with tempfile.TemporaryDirectory() as temp:
            args = runner.parse_args(self.arguments(Path(temp)) + ['--data-name', 'fixed'])
            existing = args.data_root[0] / 'fixed'
            existing.mkdir()
            sentinel = existing / 'keep'
            sentinel.write_text('existing data')
            with mock.patch.object(runner, 'check_ports'), \
                    mock.patch.object(runner.subprocess, 'Popen') as popen:
                result = runner.run(args)
            self.assertEqual(result['status'], 'failed')
            popen.assert_not_called()
            self.assertEqual(sentinel.read_text(), 'existing data')
            self.assertEqual(list(args.data_root[1].iterdir()), [])
            self.assertEqual(list(args.data_root[2].iterdir()), [])

    def test_delay_and_metrics_are_passed_to_client_with_extended_timeout(self):
        for scenario, delay, reuse, count, timeout, client, metrics, refill in (
                ('quick-pressure', 185, 0, 6452, 305, good_client(), good_metrics(), False),
                ('sustained-pressure', 0, 0, 7808, 320, sustained_client(), sustained_metrics(), False),
                ('sustained-pressure', 0, 90, 7808, 320, delayed_client(), delayed_metrics(delayed_client()), False),
                ('mixed-pressure', 0, 0, 7808, 320, mixed_client(), mixed_metrics(mixed_client()), False),
                ('arrivals-pressure', 0, 0, 7808, 320, arrivals_client(), arrivals_metrics(arrivals_client()), False),
                ('arrivals-pressure', 0, 0, 7808, 320, refill_client(), refill_metrics(refill_client()), True),
                ('multisize-pressure', 0, 0, 6400, 320, multisize_client(), multisize_metrics(multisize_client()), False)):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temp:
                args = runner.parse_args(self.arguments(Path(temp)) +
                                         ['--data-name', 'fixed', '--scenario', scenario,
                                          '--pressure-delay-sec', str(delay), '--first-reuse-sec', str(reuse)] +
                                         (['--refill-on-miss'] if refill else []))
                server = mock.Mock()
                server.poll.return_value = None
                server.wait.return_value = 0
                server.returncode = 0
                response = mock.MagicMock()
                response.__enter__.return_value.status = 200
                text = metrics
                for label, root in zip(('a', 'b', 'c'), args.data_root):
                    text = text.replace('disk="' + label + '"', 'disk=' + json.dumps(str(root / 'fixed')))
                response.__enter__.return_value.read.return_value = text.encode()

                def complete_client(command, **kwargs):
                    trace = args.output / 'trace'
                    trace.mkdir()
                    (trace / 'result.json').write_text(json.dumps(client))
                    return subprocess.CompletedProcess(command, 0)

                with mock.patch.object(runner, 'check_ports'), \
                        mock.patch.object(runner.subprocess, 'Popen', return_value=server), \
                        mock.patch.object(runner.subprocess, 'run', side_effect=complete_client) as client_run, \
                        mock.patch.object(runner.urllib.request, 'urlopen', return_value=response):
                    result = runner.run(args)
                self.assertEqual(result['status'], 'passed')
                command = client_run.call_args.args[0]
                self.assertEqual(command[command.index('--scenario') + 1], scenario)
                self.assertEqual(command[command.index('--count') + 1], str(count))
                self.assertEqual(command[command.index('--pressure-delay-sec') + 1], str(delay))
                self.assertEqual(command[command.index('--metrics-url') + 1], runner.METRICS_URL)
                self.assertEqual(client_run.call_args.kwargs['timeout'], timeout)
                case = json.loads((args.output / 'case.json').read_text())
                self.assertEqual(case['pressure_delay_sec'], delay)
                self.assertEqual(case['first_reuse_sec'], reuse)
                self.assertEqual(command[command.index('--first-reuse-sec') + 1], str(reuse))
                self.assertEqual(case['scenario'], scenario)
                self.assertEqual(case['refill_on_miss'], refill)
                self.assertEqual('--refill-on-miss' in command, refill)
                self.assertEqual(case['client_command'], command)
                self.assertEqual(case['client_timeout_sec'], timeout)
                self.assertEqual(case['data_dirs'], [str(root / 'fixed') for root in args.data_root])
                server.terminate.assert_called_once()

    def test_protect_presets_are_explicit_and_lifetime_only(self):
        with tempfile.TemporaryDirectory() as temp:
            argv = self.arguments(Path(temp))
            self.assertIsNone(runner.parse_args(argv).protect_sec)
            for value in (30, 60, 180):
                self.assertEqual(runner.parse_args(argv + ['--protect-sec', str(value)]).protect_sec, value)
            for extra in (['--protect-sec', '0'], ['--protect-sec', '-1'],
                          ['--protect-sec', '31'], ['--protect-sec', '1.5'],
                          ['--policy', 'legacy', '--protect-sec', '30']):
                with self.subTest(extra=extra), contextlib.redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit):
                    runner.parse_args(argv + extra)

    def test_only_explicit_protect_preset_reaches_server_environment(self):
        for value in (None, 30, 60, 180):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as temp:
                argv = self.arguments(Path(temp))
                args = runner.parse_args(argv + ([] if value is None else ['--protect-sec', str(value)]))
                with mock.patch.object(runner, 'check_ports'), \
                        mock.patch.dict(runner.os.environ, {'DFKV_SLAB_PROTECT_SEC': '999',
                                                           'DFKV_SLAB_PROTECT_BOOST_SEC': '1'}), \
                        mock.patch.object(runner.subprocess, 'Popen', side_effect=OSError('stop before startup')) as popen:
                    result = runner.run(args)
                env = popen.call_args.kwargs['env']
                self.assertEqual(env.get('DFKV_SLAB_PROTECT_SEC'), None if value is None else str(value))
                self.assertNotIn('DFKV_SLAB_PROTECT_BOOST_SEC', env)
                case = json.loads((args.output / 'case.json').read_text())
                self.assertEqual(case['protect_sec_override'], value)
                self.assertEqual(result['status'], 'failed')

    def test_start_failure_records_result_and_clears_inherited_disk_settings(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            args = runner.parse_args(self.arguments(root))
            with mock.patch.object(runner, 'check_ports'), \
                 mock.patch.dict(runner.os.environ, {'DFKV_SLAB_LIFE_MAX_READS': '63'}), \
                 mock.patch.object(runner.subprocess, 'Popen', side_effect=OSError('spawn failed')) as popen:
                result = runner.run(args)
            self.assertEqual(result['status'], 'failed')
            self.assertIn('spawn failed', result['error'])
            self.assertNotIn('DFKV_SLAB_LIFE_MAX_READS', popen.call_args.kwargs['env'])
            self.assertEqual(popen.call_args.kwargs['env']['DFKV_RAM_TIER'], '0')
            saved = json.loads((args.output / 'result.json').read_text())
            self.assertEqual(saved['status'], 'failed')
            self.assertTrue((args.output / 'case.json').is_file())
            for path in args.data_root:
                self.assertEqual(len(list(path.iterdir())), 1)

    def test_teardown_timeout_and_nonzero_exit_are_failures(self):
        server = mock.Mock()
        server.poll.return_value = None
        server.wait.return_value = 0
        runner.stop_server(server)
        server.terminate.assert_called_once()
        server.wait.assert_called_once_with(timeout=15)
        server.poll.return_value = 2
        server.wait.return_value = 2
        with self.assertRaisesRegex(RuntimeError, 'exited 2'):
            runner.stop_server(server)
        server.wait.side_effect = [subprocess.TimeoutExpired('server', 15), -9]
        with self.assertRaisesRegex(RuntimeError, 'SIGKILL'):
            runner.stop_server(server)
        server.kill.assert_called_once()

    def test_busy_port_closes_all_probe_sockets(self):
        first, busy = mock.Mock(), mock.Mock()
        busy.bind.side_effect = OSError('address in use')
        with mock.patch.object(runner.socket, 'socket', side_effect=[first, busy]):
            with self.assertRaises(OSError):
                runner.check_ports()
        first.close.assert_called_once()
        busy.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
