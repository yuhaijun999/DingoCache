"""Offline synthetic prefix accounting; no native client or strategy proof."""

import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/validation'))
import prefix_chain as prefix


def reseal(document):
    document['sha256'] = prefix.checksum(document)
    return document


def one_request(tokens=(16, 16, 16, 16), lookup=None, load=None):
    trace, scenario = prefix.generate()
    trace['requests'] = trace['requests'][:1]
    request = trace['requests'][0]
    request['blocks'] = [dict(namespace='demo', key=f'k{i}', generation=0, tokens=n)
                         for i, n in enumerate(tokens)]
    reseal(trace)
    scenario['trace_sha256'] = trace['sha256']
    scenario['requests'] = [dict(request_id=request['request_id'],
                                 lookup=['hit'] * len(tokens) if lookup is None else lookup,
                                 load=['adopted'] * len(tokens) if load is None else load)]
    reseal(scenario)
    return trace, scenario


class PrefixChainTest(unittest.TestCase):
    def test_generated_trace_contains_no_results_and_is_deterministic(self):
        trace, scenario = prefix.generate()
        self.assertEqual((trace, scenario), prefix.generate())
        self.assertNotIn('lookup', json.dumps(trace))
        self.assertEqual(len(trace['requests']), 4)
        prefix.validate_trace(trace)
        prefix.validate_scenario(trace, scenario)

    def test_human_example_has_distinct_prefixes_not_equal_object_hit_benefit(self):
        trace, scenario = prefix.generate()
        ledger = prefix.replay(trace, scenario)
        self.assertEqual([r['adopted_tokens'] for r in ledger['rows']], [48, 16, 0, 64])
        self.assertEqual([s['lookup'].count('hit') for s in scenario['requests']], [3, 3, 3, 4])
        summary = prefix.summarize(trace, scenario, ledger)
        self.assertEqual(summary['eligible_requests'], 4)
        self.assertEqual(summary['target_tokens'], 256)
        self.assertEqual(summary['known_adopted_tokens'], 128)
        self.assertEqual(summary['metrics']['mean_adopted_tokens'], {'numerator': 32, 'denominator': 1})
        self.assertEqual(summary['metrics']['token_reuse_ratio'], {'numerator': 1, 'denominator': 2})
        self.assertEqual(summary['metrics']['full_prefix_hit_rate'], {'numerator': 1, 'denominator': 4})
        self.assertTrue(ledger['fake_only'])
        self.assertFalse(ledger['formal_ab'])

    def test_short_circuit_does_not_call_or_claim_suffix_hits(self):
        trace, scenario = one_request(lookup=['hit', 'miss', 'hit', 'hit'])
        backend = prefix.FakeBackend(scenario)
        ledger = prefix.replay(trace, scenario, backend=backend)
        self.assertEqual([(c['phase'], c['block_index']) for c in backend.calls],
                         [('lookup', 0), ('lookup', 1), ('load', 0)])
        self.assertEqual([b['lookup'] for b in ledger['rows'][0]['blocks']],
                         ['hit', 'miss', 'not_attempted', 'not_attempted'])
        self.assertEqual([b['load'] for b in ledger['rows'][0]['blocks']],
                         ['adopted', 'not_attempted', 'not_attempted', 'not_attempted'])

    def test_variable_tokens_and_request_average_differ_from_token_weight(self):
        trace, scenario = prefix.generate()
        trace['requests'] = trace['requests'][:2]
        trace['requests'][0]['blocks'] = [dict(namespace='n', key='a', generation=0, tokens=1)]
        trace['requests'][1]['blocks'] = [dict(namespace='n', key='b', generation=0, tokens=9)]
        reseal(trace)
        scenario['requests'] = [dict(request_id=r['request_id'], lookup=[state], load=['adopted'])
                                for r, state in zip(trace['requests'], ['hit', 'miss'])]
        scenario['trace_sha256'] = trace['sha256']
        reseal(scenario)
        summary = prefix.replay(trace, scenario)['summary']
        self.assertEqual(summary['metrics']['token_reuse_ratio'], {'numerator': 1, 'denominator': 10})
        self.assertEqual(summary['metrics']['mean_request_reuse_ratio'], {'numerator': 1, 'denominator': 2})

    def test_all_hit_all_miss_and_single_block(self):
        for states, expected in [(['hit'], 7), (['miss'], 0), (['hit', 'hit'], 14),
                                 (['miss', 'miss'], 0)]:
            with self.subTest(states=states):
                trace, scenario = one_request((7,) * len(states), lookup=states)
                ledger = prefix.replay(trace, scenario)
                self.assertEqual(ledger['rows'][0]['adopted_tokens'], expected)
                self.assertEqual(ledger['summary']['unknown_requests'], 0)

    def test_lookup_is_not_adoption_and_load_stops_at_first_rejection(self):
        trace, scenario = one_request(load=['adopted', 'rejected', 'adopted', 'adopted'])
        backend = prefix.FakeBackend(scenario)
        ledger = prefix.replay(trace, scenario, backend=backend)
        row = ledger['rows'][0]
        self.assertEqual(row['candidate_tokens'], 64)
        self.assertEqual(row['adopted_tokens'], 16)
        self.assertEqual([c['block_index'] for c in backend.calls if c['phase'] == 'load'], [0, 1])
        self.assertEqual(ledger['summary']['load_outcomes']['rejected'], 1)

    def test_unknown_adoption_keeps_partial_lower_bound_and_full_denominator(self):
        trace, scenario = one_request(load=['adopted', 'unknown', 'adopted', 'adopted'])
        ledger = prefix.replay(trace, scenario)
        row, summary = ledger['rows'][0], ledger['summary']
        self.assertIsNone(row['adopted_tokens'])
        self.assertEqual(row['adopted_lower_bound_tokens'], 16)
        self.assertEqual(summary['eligible_requests'], 1)
        self.assertEqual(summary['unknown_requests'], 1)
        self.assertEqual(summary['status'], 'inconclusive')
        self.assertIsNone(summary['metrics']['mean_adopted_tokens'])
        self.assertEqual(summary['lower_bounds']['mean_adopted_tokens'], {'numerator': 16, 'denominator': 1})
        self.assertIsNone(summary['unreused_target_tokens'])

    def test_timeout_and_lookup_unknown_are_not_misses(self):
        for phase, state in [('lookup', 'timeout'), ('lookup', 'unknown'), ('load', 'timeout')]:
            with self.subTest(phase=phase, state=state):
                trace, scenario = one_request()
                scenario['requests'][0][phase][1] = state
                reseal(scenario)
                ledger = prefix.replay(trace, scenario)
                self.assertEqual(ledger['summary']['lookup_outcomes']['miss'], 0)
                self.assertEqual(ledger['summary']['unknown_requests'], int(phase == 'load'))
                if phase == 'load':
                    self.assertIsNone(ledger['rows'][0]['adopted_tokens'])
                else:
                    self.assertEqual(ledger['rows'][0]['adopted_tokens'], 16)
                    self.assertEqual(ledger['summary']['candidate_incomplete_requests'], 1)
                self.assertEqual(ledger['rows'][0]['adopted_lower_bound_tokens'], 16)

    def test_definite_io_failure_and_stale_are_distinct_from_miss(self):
        for phase, state in [('lookup', 'io_error'), ('lookup', 'stale'), ('load', 'io_error')]:
            with self.subTest(phase=phase, state=state):
                trace, scenario = one_request()
                scenario['requests'][0][phase][1] = state
                reseal(scenario)
                ledger = prefix.replay(trace, scenario)
                self.assertEqual(ledger['rows'][0]['adopted_tokens'], 16)
                self.assertEqual(ledger['summary']['lookup_outcomes']['miss'], 0)
                self.assertEqual(ledger['summary'][phase + '_outcomes'][state], 1)

    def test_window_uses_scheduled_half_open_membership_not_completion(self):
        trace, scenario = prefix.generate()
        trace['window_ns'] = [1, 3]
        reseal(trace)
        scenario['trace_sha256'] = trace['sha256']
        scenario['call_ns'] = 100
        reseal(scenario)
        ledger = prefix.replay(trace, scenario)
        self.assertEqual([r['terminal'] for r in ledger['rows']][0::3], ['outside_window', 'outside_window'])
        self.assertEqual(ledger['summary']['eligible_requests'], 2)
        self.assertEqual(ledger['summary']['censored_requests'], 2)
        self.assertGreater(ledger['rows'][1]['finished_ns'], trace['window_ns'][1])
        self.assertIsNone(ledger['rows'][3]['started_ns'])
        self.assertEqual(ledger['summary']['target_tokens'], 128)

    def test_zero_target_has_ledger_but_no_business_denominator(self):
        trace, scenario = one_request(tokens=())
        ledger = prefix.replay(trace, scenario)
        self.assertEqual(ledger['summary']['planned_requests'], 1)
        self.assertEqual(ledger['summary']['eligible_requests'], 0)
        self.assertEqual(ledger['summary']['zero_target_requests'], 1)
        self.assertIsNone(ledger['summary']['metrics']['token_reuse_ratio'])
        self.assertEqual(ledger['summary']['status'], 'not_applicable')
        self.assertEqual(ledger['rows'][0]['terminal'], 'zero_target')

    def test_empty_trace_and_no_window_eligible_request_are_not_passes(self):
        trace, scenario = one_request()
        trace['window_ns'] = [10, 11]
        reseal(trace)
        scenario['trace_sha256'] = trace['sha256']
        reseal(scenario)
        self.assertEqual(prefix.replay(trace, scenario)['summary']['status'], 'not_applicable')
        trace['requests'] = []
        reseal(trace)
        scenario['trace_sha256'] = trace['sha256']
        scenario['requests'] = []
        reseal(scenario)
        self.assertEqual(prefix.replay(trace, scenario)['summary']['planned_requests'], 0)

    def test_repeated_business_requests_and_shared_prefix_are_not_deduplicated(self):
        trace, scenario = prefix.generate()
        self.assertEqual(trace['requests'][0]['blocks'], trace['requests'][1]['blocks'])
        self.assertEqual(prefix.replay(trace, scenario)['summary']['eligible_requests'], 4)

    def test_identity_token_conflict_is_rejected(self):
        trace, scenario = prefix.generate()
        trace['requests'][1]['blocks'][0]['tokens'] = 17
        reseal(trace)
        with self.assertRaisesRegex(ValueError, 'identity|tokens'):
            prefix.validate_trace(trace)

    def test_fake_backend_is_fresh_single_use_even_for_empty_trace(self):
        for empty in (False, True):
            with self.subTest(empty=empty):
                trace, scenario = prefix.generate()
                if empty:
                    trace['requests'] = []
                    reseal(trace)
                    scenario['trace_sha256'] = trace['sha256']
                    scenario['requests'] = []
                    reseal(scenario)
                backend = prefix.FakeBackend(scenario)
                prefix.replay(trace, scenario, backend=backend)
                with self.assertRaisesRegex(ValueError, 'fresh|used'):
                    prefix.replay(trace, scenario, backend=backend)

    def test_backend_must_bind_the_current_scenario(self):
        trace, scenario = prefix.generate()
        other = copy.deepcopy(scenario)
        other['requests'][0]['request_id'] = 'different-request'
        reseal(other)
        backend = prefix.FakeBackend(other)
        with self.assertRaisesRegex(ValueError, 'scenario'):
            prefix.replay(trace, scenario, backend=backend)
        matching = prefix.FakeBackend(scenario)
        ledger = prefix.replay(trace, scenario, backend=matching)
        self.assertEqual(prefix.summarize(trace, scenario, ledger)['known_adopted_tokens'], 128)

    def test_replay_rejects_non_fake_backend(self):
        trace, scenario = prefix.generate()
        with self.assertRaisesRegex(ValueError, 'FakeBackend'):
            prefix.replay(trace, scenario, backend=object())

    def test_chain_rejects_duplicate_physical_identity_at_distinct_positions(self):
        trace, _ = prefix.generate()
        trace['requests'][0]['blocks'][1] = dict(trace['requests'][0]['blocks'][0])
        reseal(trace)
        with self.assertRaisesRegex(ValueError, 'duplicate object'):
            prefix.validate_trace(trace)

    def test_variable_block_tokens_use_prefix_sum_not_block_count(self):
        trace, scenario = one_request((2, 3, 5), lookup=['hit', 'miss', 'hit'])
        ledger = prefix.replay(trace, scenario)
        self.assertEqual(ledger['rows'][0]['candidate_tokens'], 2)
        self.assertEqual(ledger['rows'][0]['adopted_tokens'], 2)
        self.assertEqual(ledger['summary']['target_tokens'], 10)
        self.assertEqual(ledger['summary']['metrics']['token_reuse_ratio'],
                         {'numerator': 1, 'denominator': 5})

    def test_partial_unknown_and_known_requests_keep_total_target_denominator(self):
        trace, scenario = prefix.generate()
        scenario['requests'][0]['load'][1] = 'unknown'
        reseal(scenario)
        summary = prefix.replay(trace, scenario)['summary']
        self.assertEqual(summary['eligible_requests'], 4)
        self.assertEqual(summary['target_tokens'], 256)
        self.assertEqual(summary['known_requests'], 3)
        self.assertEqual(summary['known_adopted_tokens'], 80)
        self.assertEqual(summary['confirmed_adopted_lower_bound_tokens'], 96)
        self.assertEqual(summary['lower_bounds']['mean_adopted_tokens'],
                         {'numerator': 24, 'denominator': 1})
        self.assertEqual(summary['lower_bounds']['token_reuse_ratio'],
                         {'numerator': 3, 'denominator': 8})
        self.assertIsNone(summary['metrics']['full_prefix_hit_rate'])

    def test_zero_target_and_censored_requests_make_no_backend_calls(self):
        trace, scenario = one_request(tokens=())
        backend = prefix.FakeBackend(scenario)
        prefix.replay(trace, scenario, backend=backend)
        self.assertEqual(backend.calls, [])
        trace, scenario = one_request()
        trace['window_ns'] = [10, 11]
        reseal(trace)
        scenario['trace_sha256'] = trace['sha256']
        reseal(scenario)
        backend = prefix.FakeBackend(scenario)
        ledger = prefix.replay(trace, scenario, backend=backend)
        self.assertEqual(backend.calls, [])
        self.assertFalse(ledger['rows'][0]['candidate_observation_complete'])

    def test_limits_empty_names_and_bad_digests_are_rejected(self):
        changes = [lambda t: t['window_ns'].__setitem__(1, True),
                   lambda t: t.update(window_ns=[2, 1]),
                   lambda t: t['requests'][0].update(request_id=''),
                   lambda t: t['requests'][0]['blocks'][0].update(namespace=chr(10)),
                   lambda t: t['requests'][0]['blocks'][0].update(key='x' * 257),
                   lambda t: t['requests'][0]['blocks'][0].update(generation=prefix.MAX_INT + 1),
                   lambda t: t['requests'][0].update(blocks=t['requests'][0]['blocks'] * 65)]
        for change in changes:
            with self.subTest(change=change):
                trace, _ = prefix.generate()
                change(trace)
                reseal(trace)
                with self.assertRaises(ValueError):
                    prefix.validate_trace(trace)
        trace, scenario = prefix.generate()
        scenario['sha256'] = 'x' * 64
        with self.assertRaises(ValueError):
            prefix.validate_scenario(trace, scenario)

    def test_trace_rejects_unsupported_mode_fields_duplicate_and_invalid_numbers(self):
        mutations = [lambda t: t.update(mode='multishard'),
                     lambda t: t['requests'][0].update(local_tokens=16),
                     lambda t: t['requests'][0]['blocks'][0].update(shards=2),
                     lambda t: t['requests'][0].update(scheduled_ns=True),
                     lambda t: t['requests'][0]['blocks'][0].update(tokens=0),
                     lambda t: t['requests'][0]['blocks'][0].update(generation=-1),
                     lambda t: t['requests'][1].update(request_id=t['requests'][0]['request_id']),
                     lambda t: t['requests'].reverse()]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                trace, _ = prefix.generate()
                mutation(trace)
                reseal(trace)
                with self.assertRaises(ValueError):
                    prefix.validate_trace(trace)

    def test_scenario_rejects_wrong_trace_ids_lengths_states_and_boolean_clock(self):
        mutations = [lambda s: s.update(trace_sha256='0' * 64),
                     lambda s: s['requests'][0].update(request_id='other'),
                     lambda s: s['requests'][0]['lookup'].pop(),
                     lambda s: s['requests'][0]['lookup'].__setitem__(0, 'probably'),
                     lambda s: s.update(call_ns=True),
                     lambda s: s['requests'].pop()]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                trace, scenario = prefix.generate()
                mutation(scenario)
                reseal(scenario)
                with self.assertRaises(ValueError):
                    prefix.validate_scenario(trace, scenario)

    def test_report_rejects_omitted_extra_reordered_rows_and_forged_summary(self):
        mutations = [lambda r: r['rows'].pop(),
                     lambda r: r['rows'].append(copy.deepcopy(r['rows'][0])),
                     lambda r: r['rows'].reverse(),
                     lambda r: r['summary'].update(known_adopted_tokens=999),
                     lambda r: r['rows'][0].update(adopted_tokens=65),
                     lambda r: r['rows'][0].update(candidate_tokens=65),
                     lambda r: r['rows'][0].update(started_ns=-1),
                     lambda r: r.update(formal_ab=True)]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                trace, scenario = prefix.generate()
                ledger = prefix.replay(trace, scenario)
                mutation(ledger)
                reseal(ledger)
                with self.assertRaises(ValueError):
                    prefix.summarize(trace, scenario, ledger)

    def test_rehashed_ledger_identity_and_script_result_tampering_are_rejected(self):
        changes = [('namespace', 'other'), ('key', 'other'), ('generation', 1),
                   ('tokens', True), ('lookup', 'miss'), ('load', 'unknown')]
        for field, value in changes:
            with self.subTest(field=field):
                trace, scenario = prefix.generate()
                ledger = prefix.replay(trace, scenario)
                ledger['rows'][0]['blocks'][0][field] = value
                reseal(ledger)
                with self.assertRaises(ValueError):
                    prefix.summarize(trace, scenario, ledger)

    def test_report_rejects_claiming_unattempted_suffix_as_hit(self):
        trace, scenario = one_request(lookup=['hit', 'miss', 'hit', 'hit'])
        ledger = prefix.replay(trace, scenario)
        ledger['rows'][0]['blocks'][2]['lookup'] = 'hit'
        reseal(ledger)
        with self.assertRaises(ValueError):
            prefix.summarize(trace, scenario, ledger)

    def test_checksum_and_flags_are_checked_not_just_self_consistent_summary(self):
        trace, scenario = prefix.generate()
        ledger = prefix.replay(trace, scenario)
        ledger['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            prefix.summarize(trace, scenario, ledger)
        trace['sha256'] = '0' * 64
        with self.assertRaises(ValueError):
            prefix.validate_trace(trace)


class PrefixChainCliTest(unittest.TestCase):
    def run_cli(self, *args):
        return subprocess.run([sys.executable, '-I', str(ROOT/'deploy/validation/prefix_chain.py'), *map(str, args)],
                              cwd=tempfile.gettempdir(), capture_output=True, text=True, timeout=10)

    def test_actual_cli_generate_replay_check_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            sample, ledger, checked = base/'sample', base/'ledger.json', base/'checked.json'
            generated = self.run_cli('generate', '--output', sample)
            self.assertEqual(generated.returncode, 0, generated.stderr)
            self.assertTrue((sample/'trace.json').is_file())
            args = ['--trace', sample/'trace.json', '--scenario', sample/'scenario.json']
            replayed = self.run_cli('replay', *args, '--output', ledger)
            self.assertEqual(replayed.returncode, 0, replayed.stderr)
            result = self.run_cli('check', *args, '--ledger', ledger, '--output', checked)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(checked.read_text())['known_adopted_tokens'], 128)
            for command in [('generate', '--output', sample),
                            ('replay', *args, '--output', ledger),
                            ('check', *args, '--ledger', ledger, '--output', sample/'trace.json')]:
                failed = self.run_cli(*command)
                self.assertNotEqual(failed.returncode, 0)
                self.assertNotIn('Traceback', failed.stderr)
            self.assertEqual(json.loads(ledger.read_text())['summary']['known_adopted_tokens'], 128)

    def test_cli_rejects_symlink_output_duplicate_json_invalid_and_large_input(self):
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            self.assertEqual(self.run_cli('generate', '--output', base/'sample').returncode, 0)
            trace, scenario = base/'sample/trace.json', base/'sample/scenario.json'
            (base/'alias').symlink_to(base/'new-target')
            args = ['replay', '--trace', trace, '--scenario', scenario, '--output', base/'alias']
            self.assertNotEqual(self.run_cli(*args).returncode, 0)
            self.assertFalse((base/'new-target').exists())
            for contents in ['{"schema":1,"schema":2}', 'NaN', '[', ' ' * (prefix.MAX_FILE_BYTES + 1)]:
                (base/'bad.json').write_text(contents)
                failed = self.run_cli('replay', '--trace', base/'bad.json', '--scenario', scenario,
                                      '--output', base/'new.json')
                self.assertNotEqual(failed.returncode, 0)
                self.assertNotIn('Traceback', failed.stderr)
                self.assertFalse((base/'new.json').exists())


if __name__ == '__main__':
    unittest.main()
