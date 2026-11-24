"""Offline deterministic diagnostics; no service, native library or transport proof."""

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/validation'))
import long_residence as residence


class Clock:
    def __init__(self):
        self.now = 100.0

    def __call__(self):
        return self.now

    def sleep(self, duration):
        self.now += duration


class Client:
    """Controlled memory adapter, not an RDMA emulator."""
    transport = 'rdma'

    def __init__(self):
        self.store = {}
        self.miss_first = set()
        self.first_seen = set()
        self.fail = False
        self.closed = False
        self.scalar = []
        self.batch = []

    def put(self, keys, values):
        if self.fail:
            raise RuntimeError('controlled PUT failure')
        self.store.update(zip(keys, values))

    def read_outcomes(self, keys, values):
        self.batch.append(list(keys))
        return [{'outcome': 'get_hit', 'status': 0} for key in keys]

    def read_outcome(self, key, value):
        self.scalar.append(key)
        if key in self.miss_first and key not in self.first_seen:
            self.first_seen.add(key)
            return 'get_miss'
        if self.store.get(key) != value:
            raise RuntimeError('controlled wrong bytes')
        return 'get_hit'

    def close(self):
        self.closed = True


def config():
    value = residence.profile('accelerated')
    value.update(protect_sec=2, delays_sec=[1, 3, 4], steps=6,
                 prefill=10, cohort_size=2, arrival_batch=2, length=4, preage_sec=2)
    return value


class LongResidenceTest(unittest.TestCase):
    def run_case(self, clients=None):
        clients = clients or [Client(), Client()]
        clock = Clock()
        report = residence.run(clients, config(), clock=clock, sleep=clock.sleep,
                               eviction_counter=lambda: max(0, sum(len(client.store) for client in clients) - 20))
        return report, clients

    def hot_case(self, *, fail_put=False, bad_verify=False, slow_refill=False, hot_refill=True):
        frozen = config()
        if hot_refill:
            frozen['hot_refill'] = True
        clock = Clock()
        clients = [Client(), Client()]
        client = clients[0]
        original_batch, original_put, original_scalar = client.read_outcomes, client.put, client.read_outcome
        hot_key = residence.disk.key_for(frozen['seed'], 0)
        hot_batches = 0
        refill_pending = False
        def batch(keys, values):
            nonlocal hot_batches, refill_pending
            outcomes = original_batch(keys, values)
            if len(keys) == frozen['cohort_size'] and keys[0] == hot_key:
                hot_batches += 1
                # Two preage reads and ready precede measurement. Lose the same
                # key at measurement steps 0 and 2; later reads must be separate.
                if hot_batches in (4, 6):
                    outcomes[0] = {'outcome': 'get_miss', 'status': 1}
                    refill_pending = True
            return outcomes
        def put(keys, values):
            if refill_pending and keys == [hot_key]:
                if fail_put:
                    raise RuntimeError('hot refill PUT failed')
                if slow_refill:
                    clock.now += 6
            return original_put(keys, values)
        def scalar(key, value):
            if bad_verify and refill_pending and key == hot_key:
                return 'get_miss'
            return original_scalar(key, value)
        client.read_outcomes, client.put, client.read_outcome = batch, put, scalar
        report = residence.run(clients, frozen, clock=clock, sleep=clock.sleep,
            eviction_counter=lambda: max(0, sum(len(c.store) for c in clients) - 20))
        return frozen, report

    def test_hot_refill_is_opt_in_and_accounts_repeat_losses_and_later_reads(self):
        frozen, report = self.hot_case()
        self.assertTrue(report['passed'], report['errors'])
        counts = residence.check_report(report, frozen)
        self.assertEqual(report['hot_refill'], {
            'distinct_lost_keys': 1, 'repeat_misses': 1, 'refill_puts': 2,
            'refill_payload_bytes': 8, 'verify_gets': 2, 'verify_hits': 2,
            'subsequent_hits': 4, 'subsequent_misses': 1})
        self.assertEqual(counts, {'put_acks': 46, 'put_bytes': 184,
                                 'get_hits': 68, 'get_misses': 2, 'get_attempts': 70})
        self.assertEqual(report['refill_puts'], 0)  # Delayed-cohort refills only.
        legacy, _ = self.run_case()
        self.assertNotIn('hot_refill', legacy)
        self.assertNotIn('hot_refill', residence.profile('default'))

    def test_no_refill_reports_distinct_losses_without_put_or_verify_and_accepts_old_reports(self):
        frozen, report = self.hot_case(hot_refill=False)
        self.assertTrue(report['passed'])
        counts = residence.check_report(report, frozen)
        self.assertEqual(report['hot_read_summary'], {'distinct_lost_keys': 1, 'repeat_misses': 1})
        self.assertNotIn('hot_refill', report)
        self.assertEqual(counts, {'put_acks': 44, 'put_bytes': 176,
                                 'get_hits': 66, 'get_misses': 2, 'get_attempts': 68})
        old = copy.deepcopy(report)
        old.pop('hot_read_summary')
        for row in old['rows']:
            row.pop('hot_outcomes', None)
        self.assertEqual(residence.check_report(old, frozen), counts)
        for remove in ('summary', 'row'):
            partial = copy.deepcopy(report)
            if remove == 'summary':
                partial.pop('hot_read_summary')
            else:
                next(row for row in partial['rows'] if row['kind'] == 'hot').pop('hot_outcomes')
            with self.assertRaises(ValueError):
                residence.check_report(partial, frozen)

    def test_hot_and_delayed_refill_totals_are_separate_but_full_metrics_include_both(self):
        frozen = config()
        frozen['hot_refill'] = True
        clock = Clock()
        clients = [Client(), Client()]
        hot_key = residence.disk.key_for(frozen['seed'], 0)
        delayed_key = residence.disk.key_for(frozen['seed'], 4)
        for client in clients:
            client.miss_first.add(delayed_key)
            original = client.read_outcomes
            batches = [0]
            def controlled(keys, values, original=original, batches=batches):
                outcomes = original(keys, values)
                if len(keys) == frozen['cohort_size'] and keys[0] == hot_key:
                    batches[0] += 1
                    if batches[0] == 4:
                        outcomes[0] = {'outcome': 'get_miss', 'status': 1}
                return outcomes
            client.read_outcomes = controlled
        report = residence.run(clients, frozen, clock=clock, sleep=clock.sleep,
            eviction_counter=lambda: max(0, sum(len(c.store) for c in clients) - 20))
        counts = residence.check_report(report, frozen)
        self.assertEqual(report['hot_read_summary'], {'distinct_lost_keys': 2, 'repeat_misses': 0})
        self.assertEqual(report['hot_refill']['refill_puts'], 2)
        self.assertEqual(report['refill_puts'], 2)
        self.assertEqual(counts, {'put_acks': 48, 'put_bytes': 192,
                                 'get_hits': 68, 'get_misses': 4, 'get_attempts': 72})

    def test_hot_refill_failed_put_verify_and_stall_cannot_pass(self):
        for option in ('fail_put', 'bad_verify', 'slow_refill'):
            frozen, report = self.hot_case(**{option: True})
            with self.subTest(option=option):
                self.assertFalse(report['passed'])
                with self.assertRaises(ValueError):
                    residence.check_report(report, frozen)
                if option == 'fail_put':
                    self.assertEqual(report['unresolved_operation']['kind'], 'hot_refill')
                    self.assertEqual(report['hot_refill']['refill_puts'], 0)
                elif option == 'bad_verify':
                    self.assertEqual(report['hot_refill']['verify_gets'], 1)
                    self.assertEqual(report['hot_refill']['verify_hits'], 0)
                else:
                    self.assertIn('scheduling window', report['errors'][0])

    def test_hot_refill_checker_rejects_forged_key_counts_phase_and_mode(self):
        frozen, report = self.hot_case()
        hot = next(i for i, row in enumerate(report['rows']) if row['kind'] == 'hot')
        mutations = [lambda r: r['hot_refill'].update(distinct_lost_keys=2),
                     lambda r: r['hot_refill'].update(subsequent_hits=5),
                     lambda r: r['hot_refill'].update(refill_payload_bytes=0),
                     lambda r: r['rows'][hot]['hot_outcomes'].pop(),
                     lambda r: r['rows'][hot]['hot_outcomes'][0].update(index=1),
                     lambda r: r['rows'][hot]['hot_outcomes'][0].update(refill_ack=False),
                     lambda r: r['rows'][hot]['hot_outcomes'][0].update(verify_outcome='get_miss'),
                     lambda r: r['rows'][hot]['hot_outcomes'][0].update(finished_sec=999),
                     lambda r: r['rows'][hot]['hot_outcomes'][0].update(ack_sec=999),
                     lambda r: r['config'].pop('hot_refill')]
        for mutate in mutations:
            forged = copy.deepcopy(report)
            mutate(forged)
            with self.assertRaises(ValueError):
                residence.check_report(forged, frozen)
        for value in (0, 1, 'yes', None):
            invalid = dict(frozen, hot_refill=value)
            with self.assertRaises(ValueError):
                residence.validate(invalid)

    def test_profiles_are_independent_and_default_not_scaled(self):
        accelerated = residence.profile('accelerated')
        accelerated['delays_sec'][0] = 99
        self.assertEqual(residence.profile('accelerated')['delays_sec'], [3, 13, 24])
        self.assertEqual(residence.profile('default')['steps'], 1800)
        self.assertEqual(residence.profile('default')['protect_sec'], 180)
        self.assertEqual(residence.profile('default')['delays_sec'], [45, 185, 600])

    def test_default_window_contract_and_full_planned_denominators(self):
        frozen = residence.profile('default')
        self.assertEqual(frozen, {
            'name': 'default', 'protect_sec': 180, 'delays_sec': [45, 185, 600],
            'steps': 1800, 'preage_sec': 240, 'interval_sec': 1,
            'prefill': 6144, 'cohort_size': 32, 'arrival_batch': 64,
            'length': 4096, 'seed': 0xD1A60051})
        planned = residence.expected_counts(frozen)
        self.assertEqual(planned, {
            'initial_puts': 12288, 'arrival_puts': 230400, 'warm_gets': 256,
            'preage_gets': 15360, 'ready_gets': 64, 'hot_gets': 115200,
            'first_gets': 192, 'cooled_gets': 64})
        self.assertEqual((planned['initial_puts'] + planned['arrival_puts']) *
                         frozen['length'], 994050048)
        self.assertEqual(sum(count for name, count in planned.items()
                             if name.endswith('_gets')), 131136)
        # Aggregate turnover is a workload descriptor, not a resident guarantee.
        nominal_turnover = planned['initial_puts'] / (2 * frozen['arrival_batch'])
        self.assertEqual(nominal_turnover, 96)
        self.assertLess(frozen['delays_sec'][0], nominal_turnover)
        self.assertTrue(all(delay > nominal_turnover
                            for delay in frozen['delays_sec'][1:]))

    def test_full_default_schedule_reaches_all_ack_relative_reuse_windows(self):
        class ScheduleClient(Client):
            """No storage or byte emulation; only schedule outcomes are controlled."""
            def put(self, keys, values):
                pass

            def read_outcomes(self, keys, values):
                return [{'outcome': 'get_hit', 'status': 0} for key in keys]

            def read_outcome(self, key, value):
                self.scalar.append(key)
                return 'get_hit'

        frozen = residence.profile('default')
        clock = Clock()
        clients = [ScheduleClient(), ScheduleClient()]
        pressure_start = clock.now + frozen['preage_sec']
        # Avoid allocating the nominal 994MB dataset in a timing-only unit test.
        # Native status and byte correctness remain separate validation gates.
        with mock.patch.object(residence.disk, 'payload', new=lambda *args: b'fixture'):
            report = residence.run(clients, frozen, clock=clock, sleep=clock.sleep,
                eviction_counter=lambda: max(0, int(clock.now - pressure_start) * 128))
        counts = residence.check_report(report, frozen)
        self.assertTrue(report['passed'])
        self.assertEqual(report['preage_finished_sec'] - report['preage_started_sec'], 240)
        self.assertEqual(report['measurement_elapsed_sec'], 1800)
        self.assertEqual(counts['get_attempts'], 131136)
        self.assertEqual(counts['put_acks'], 242688)
        for delay in (45, 185, 600):
            first = [row for row in report['rows']
                     if row['kind'] == 'first' and row['target_age_sec'] == delay]
            self.assertEqual(len(first), 64)
            self.assertEqual({row['age_sec'] for row in first}, {delay})
            self.assertEqual({row['lateness_sec'] for row in first}, {0})
            self.assertTrue(all(row['eviction_delta_before_first'] > 0 for row in first))

    def test_frozen_first_reuse_lateness_cannot_be_relaxed_in_report(self):
        # The small fake-clock fixture exercises the shared oracle without
        # allocating the full default dataset or sleeping for 30 minutes.
        report, _ = self.run_case()
        self.assertEqual(report['max_lateness_sec'], 2)
        report['max_lateness_sec'] = 3
        with self.assertRaises(ValueError):
            residence.check_report(report, config())

    def test_complete_denominators_and_ack_ages(self):
        report, clients = self.run_case()
        counts = residence.check_report(report, config())
        self.assertTrue(report['passed'])
        self.assertEqual(counts, {'put_acks': 44, 'put_bytes': 176,
                                 'get_hits': 68, 'get_misses': 0, 'get_attempts': 68})
        first = [row for row in report['rows'] if row['kind'] == 'first']
        self.assertEqual(len(first), 12)
        self.assertEqual({row['age_sec'] for row in first}, {1, 3, 4})
        self.assertEqual(report['measurement_elapsed_sec'], 6)
        self.assertFalse(any(client.closed for client in clients))
        # The workload borrows clients; its owner must close them after return.

    def test_delayed_cohorts_never_read_in_warmup(self):
        report, clients = self.run_case()
        for client in clients:
            warmed = set(client.batch[0] + client.batch[1])
            delayed = {residence.disk.key_for(config()['seed'], index) for index in range(4, 10)}
            self.assertFalse(warmed & delayed)
            self.assertEqual(set(client.scalar), delayed)

    def test_residents_are_preaged_while_only_persistent_hot_is_refreshed(self):
        report, _ = self.run_case()
        self.assertEqual(report['preage_finished_sec'] - report['preage_started_sec'], 2)
        rows = [row for row in report['rows'] if row['kind'] == 'preage']
        self.assertEqual(len(rows), 4)
        self.assertTrue(all(row['start_index'] == 0 and row['count'] == 2 for row in rows))
        delayed_puts = [row for row in report['rows'] if row['kind'] == 'initial_puts'
                        and 4 <= row['start_index'] < 10]
        self.assertTrue(all(row['ack_sec'] >= report['preage_finished_sec'] for row in delayed_puts))
        for mutate in (lambda r: r['preage_steps'].pop(),
                       lambda r: r['preage_steps'][0].update(finished_sec=8),
                       lambda r: next(row for row in r['rows'] if row['kind'] == 'preage').update(start_index=2)):
            forged = copy.deepcopy(report)
            mutate(forged)
            with self.assertRaises(ValueError):
                residence.check_report(forged, config())

    def test_miss_refill_does_not_change_first_result(self):
        clients = [Client(), Client()]
        key = residence.disk.key_for(config()['seed'], 4)
        clients[0].miss_first.add(key)
        report, _ = self.run_case(clients)
        counts = residence.check_report(report, config())
        row = next(row for row in report['rows'] if row['kind'] == 'first'
                   and row['namespace_hex'] == residence.disk.NAMESPACES[0].hex()
                   and row['index'] == 4)
        self.assertEqual(row['outcome'], 'get_miss')
        self.assertTrue(row['refill_ack'])
        self.assertEqual(row['verify_outcome'], 'get_hit')
        self.assertEqual(report['refill_payload_bytes'], 4)
        self.assertEqual(counts['put_acks'], 45)
        self.assertEqual(counts['get_misses'], 1)
        self.assertEqual(counts['get_attempts'], 69)

    def test_no_transport_fallback_or_writes(self):
        clients = [Client(), Client()]
        clients[1].transport = 'tcp(rdma-not-requested)'
        report, _ = self.run_case(clients)
        self.assertFalse(report['passed'])
        self.assertEqual(sum(report['actual'].values()), 0)
        self.assertFalse(clients[0].store)

    def test_put_error_leaves_partial_denominators_not_false_success(self):
        clients = [Client(), Client()]
        clients[0].fail = True
        report, _ = self.run_case(clients)
        self.assertFalse(report['passed'])
        self.assertEqual(report['actual']['initial_puts'], 0)
        self.assertEqual(report['unresolved_operation']['terminal'], 'unknown-if-native-call-fails')
        self.assertEqual(report['unresolved_operation']['count'], 2)
        with self.assertRaises(ValueError):
            residence.check_report(report, config())

    def test_unresolved_status_cannot_be_miss(self):
        clients = [Client(), Client()]
        clients[0].read_outcome = lambda *args: 'get_unresolved'
        report, _ = self.run_case(clients)
        self.assertFalse(report['passed'])
        self.assertEqual(report['actual']['first_gets'], 0)
        self.assertEqual(report['refill_puts'], 0)

    def test_batch_status_error_invalidates_initial_state(self):
        clients = [Client(), Client()]
        clients[0].read_outcomes = lambda keys, values: [{'outcome': 'client_error', 'status': 4}] * len(keys)
        report, _ = self.run_case(clients)
        self.assertFalse(report['passed'])
        self.assertEqual(report['actual']['warm_gets'], 0)

    def test_miss_during_initial_warmup_is_failure(self):
        clients = [Client(), Client()]
        clients[0].read_outcomes = lambda keys, values: [{'outcome': 'get_miss', 'status': 1}] * len(keys)
        report, _ = self.run_case(clients)
        self.assertFalse(report['passed'])
        self.assertIn('warmup miss', report['errors'][0])

    def test_refill_failure_is_not_hidden(self):
        clients = [Client(), Client()]
        clients[0].read_outcome = lambda *args: 'get_miss'
        report, _ = self.run_case(clients)
        self.assertFalse(report['passed'])
        self.assertEqual(report['actual']['first_gets'], 1)
        self.assertEqual(report['refill_puts'], 1)
        self.assertEqual(report['verify_hits'], 0)

    def test_invalid_configs_fail_before_io(self):
        for name, value in [('steps', True), ('steps', 4), ('delays_sec', [1, 3, 3]),
                            ('prefill', 9), ('protect_sec', 3), ('length', 0)]:
            bad = config()
            bad[name] = value
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                residence.run([Client(), Client()], bad)

    def test_checker_rejects_forged_denominators_ages_keys_and_refill(self):
        report, _ = self.run_case()
        mutations = [lambda r: r['actual'].update(first_gets=11),
                     lambda r: r.update(refill_payload_bytes=4),
                     lambda r: r.update(measurement_elapsed_sec=5),
                     lambda r: r.update(elapsed_sec=float('nan')),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'first').update(
                         ack_sec=1, started_sec=2, age_sec=1, lateness_sec=0),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'first').update(
                         eviction_delta_before_first=0),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'hot').update(
                         namespace_hex=residence.disk.NAMESPACES[1].hex()),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'warm').update(
                         finished_sec=1),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'cooled').update(
                         started_sec=0),
                     lambda r: r['rows'].append(copy.deepcopy(r['rows'][0])),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'first').update(age_sec=0),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'first').update(key_hex='00'),
                     lambda r: next(row for row in r['rows'] if row['kind'] == 'first').update(refill_ack=True)]
        for mutate in mutations:
            forged = copy.deepcopy(report)
            mutate(forged)
            with self.assertRaises(ValueError):
                residence.check_report(forged, config())

    def test_no_eviction_before_first_is_not_pressure_validation(self):
        clock = Clock()
        report = residence.run([Client(), Client()], config(), clock=clock,
                               sleep=clock.sleep, eviction_counter=lambda: 0)
        self.assertFalse(report['passed'])
        self.assertEqual(report['actual']['first_gets'], 0)
        self.assertIn('without measured eviction', report['errors'][0])

    def test_scheduler_stall_invalidates_sample(self):
        clock = Clock()
        counts = iter(range(20))
        report = residence.run([Client(), Client()], config(), clock=clock,
                               sleep=lambda duration: clock.sleep(duration + 5),
                               eviction_counter=lambda: next(counts))
        self.assertFalse(report['passed'])
        self.assertIn('stall', report['errors'][0])

    def test_preprotection_first_cannot_silently_become_postprotection(self):
        clock = Clock()
        clients = [Client(), Client()]
        original = clients[0].read_outcomes
        def slow_warm(keys, values):
            clock.now += 3
            return original(keys, values)
        clients[0].read_outcomes = slow_warm
        counts = iter(range(20))
        report = residence.run(clients, config(), clock=clock, sleep=clock.sleep,
                               eviction_counter=lambda: next(counts))
        self.assertFalse(report['passed'])
        self.assertIn('scheduling window', report['errors'][0])

    def test_final_durations_share_one_clock_sample(self):
        class ClosingClock(Clock):
            armed = False

            def __call__(self):
                if self.armed:
                    self.now += .001
                return self.now

        clock = ClosingClock()
        clients = [Client(), Client()]
        original = clients[1].read_outcomes
        cooled_first = residence.disk.key_for(config()['seed'], config()['cohort_size'])

        def final_cooled(keys, values):
            if keys[0] == cooled_first:
                # Model clock progress or preemption only at finalization, not
                # during the earlier synthetic scheduling and age assertions.
                clock.armed = True
            return original(keys, values)

        clients[1].read_outcomes = final_cooled
        report = residence.run(clients, config(), clock=clock, sleep=clock.sleep,
            eviction_counter=lambda: max(0, sum(len(client.store) for client in clients) - 20))
        self.assertTrue(report['passed'])
        self.assertAlmostEqual(report['elapsed_sec'], report['measurement_started_sec'] +
                               report['measurement_elapsed_sec'], places=9)
        self.assertEqual(residence.check_report(report, config())['get_attempts'], 68)

    def test_main_hot_refill_flag_is_preserved_even_in_startup_failure_report(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'run'
            with mock.patch.object(residence.disk, 'load_library', return_value=object()), \
                    mock.patch.object(residence.disk, 'Client') as factory:
                self.assertEqual(residence.main(['--lib', 'unused', '--members', 'unused',
                    '--profile', 'default', '--hot-refill', '--output', str(output)]), 1)
                factory.assert_not_called()
            report = json.loads((output / 'report.json').read_text())
            self.assertIs(report['config']['hot_refill'], True)
            self.assertFalse(report['passed'])

    def test_main_missing_symbols_writes_failure_and_never_opens(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'run'
            with mock.patch.object(residence.disk, 'load_library', return_value=object()), \
                    mock.patch.object(residence.disk, 'Client') as factory:
                self.assertEqual(residence.main(['--lib', 'unused', '--members', 'unused',
                    '--profile', 'accelerated', '--output', str(output)]), 1)
                factory.assert_not_called()
            report = json.loads((output / 'report.json').read_text())
            self.assertFalse(report['passed'])
            self.assertIn('status symbols', report['errors'][0])

    def test_main_partial_open_closes_owned_first_handle(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / 'run'
            client = Client()
            library = type('Library', (), {'dfkv_get_auto_status': True,
                                          'dfkv_batch_get_auto_status': True})()
            with mock.patch.object(residence.disk, 'load_library', return_value=library), \
                    mock.patch.object(residence.disk, 'Client', side_effect=[client, RuntimeError('open failed')]):
                self.assertEqual(residence.main(['--lib', 'unused', '--members', 'unused',
                    '--profile', 'accelerated', '--output', str(output)]), 1)
            self.assertTrue(client.closed)
            self.assertFalse(json.loads((output / 'report.json').read_text())['passed'])


if __name__ == '__main__':
    unittest.main()
