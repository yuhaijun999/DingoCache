"""Stateful fake prefix accounting; no production policy or hardware proof."""

import copy
import itertools
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/validation'))
import prefix_dynamic as dynamic


def seal(value):
    value['sha256'] = dynamic.checksum(value)
    return value


def plan(actions, capacity=3, objects=None):
    if objects is None:
        objects = [dict(object_id=key, namespace='n', key=key, generation=0,
                        tokens=10, bytes=1) for key in 'ABCDE']
    actions = copy.deepcopy(actions)
    faults = []
    for index, action in enumerate(actions):
        for item in action.pop('faults', []):
            faults.append(dict(action_index=index, **item))
    return seal(dict(schema=dynamic.PLAN_SCHEMA, synthetic=True,
                     replacement='test-fifo-bytes', capacity_bytes=capacity,
                     objects=objects, actions=actions, faults=faults))


def put(key, status='ack'):
    return dict(op='put', object_id=key, status=status)


def request(name, keys, refill=False, faults=None):
    return dict(op='request', request_id=name, chain_id=name,
                object_ids=list(keys), refill=refill,
                faults=[] if faults is None else faults)


def fault(index, phase, status):
    return dict(block_index=index, phase=phase, status=status)


class PrefixDynamicTest(unittest.TestCase):
    def run_plan(self, value):
        ledger = dynamic.replay(value)
        self.assertEqual(dynamic.check(value, ledger), ledger['summary'])
        return ledger

    def test_generate_deterministic_and_hand_calculated(self):
        value = dynamic.generate()
        self.assertEqual(value, dynamic.generate())
        ledger = self.run_plan(value)
        self.assertEqual([row['adopted_tokens'] for row in ledger['requests']],
                         [20, 20, 30, 0])
        self.assertEqual(ledger['summary']['target_tokens'], 120)
        self.assertEqual(ledger['summary']['known_adopted_tokens'], 70)
        self.assertEqual(ledger['summary']['refill_ack_bytes'], 1)
        self.assertEqual(ledger['summary']['evictions'], 1)

    def test_refill_never_retroactively_changes_first_result(self):
        ledger = self.run_plan(plan([put('A'), request('first', 'ABC', True),
                                     request('again', 'ABC')]))
        self.assertEqual([row['adopted_tokens'] for row in ledger['requests']], [10, 30])
        self.assertEqual(ledger['summary']['refill_attempts'], 2)
        self.assertEqual(ledger['summary']['refill_ack_bytes'], 2)
        self.assertEqual(ledger['summary']['recompute_proxy_tokens'], 20)
        self.assertEqual(ledger['summary']['target_tokens'], 60)

    def test_middle_head_tail_missing_boundary(self):
        for missing, expected in [('A', 0), ('B', 10), ('C', 20)]:
            with self.subTest(missing=missing):
                actions = [put(key) for key in 'ABC' if key != missing]
                row = self.run_plan(plan(actions + [request('r', 'ABC')]))['requests'][0]
                self.assertEqual(row['adopted_tokens'], expected)
                self.assertEqual(row['first_unconfirmed_block'], 'ABC'.index(missing))
                calls = [event for event in row['events'] if event['phase'] == 'lookup']
                self.assertEqual(len(calls), 'ABC'.index(missing) + 1)

    def test_shared_prefix_branches_and_request_denominator(self):
        ledger = self.run_plan(plan([put('A'), put('B'), put('C'), put('D'),
                                    request('abc', 'ABC'), request('abd', 'ABD'),
                                    request('ab', 'AB'), request('repeat', 'ABC')], capacity=4))
        self.assertEqual(ledger['summary']['eligible_requests'], 4)
        self.assertEqual(ledger['summary']['target_tokens'], 110)
        self.assertEqual(ledger['summary']['known_adopted_tokens'], 110)
        self.assertEqual(ledger['summary']['full_prefix_hit_rate'],
                         dict(numerator=1, denominator=1))

    def test_fifo_hits_do_not_refresh_and_capacity_competition(self):
        ledger = self.run_plan(plan([put('A'), put('B'), request('hot', 'A'),
                                    put('C'), request('loss', 'AB')], capacity=2))
        self.assertEqual(ledger['requests'][1]['adopted_tokens'], 0)
        self.assertEqual(ledger['summary']['evicted_previously_adopted_objects'], 1)
        self.assertEqual(ledger['final_residents'], ['B', 'C'])

    def test_ack_rewrite_refreshes_fifo_once(self):
        ledger = self.run_plan(plan([put('A'), put('B'), put('A'), put('C')], capacity=2))
        self.assertEqual(ledger['final_residents'], ['A', 'C'])
        self.assertEqual(ledger['summary']['occupancy_bytes'], 2)

    def test_failed_write_never_evicts_or_mutates(self):
        ledger = self.run_plan(plan([put('A'), put('B', 'io_error'),
                                    request('r', 'AB')], capacity=1))
        self.assertEqual(ledger['final_residents'], ['A'])
        self.assertEqual(ledger['summary']['evictions'], 0)
        self.assertEqual(ledger['requests'][0]['adopted_tokens'], 10)

    def test_remove_present_absent_and_revisit(self):
        ledger = self.run_plan(plan([put('A'), dict(op='remove', object_id='A'),
                                    dict(op='remove', object_id='A'), request('r', 'A')]))
        self.assertEqual(ledger['summary']['remove_hits'], 1)
        self.assertEqual(ledger['requests'][0]['adopted_tokens'], 0)

    def test_failed_refill_does_not_stop_other_suffix_writes(self):
        ledger = self.run_plan(plan([request('r', 'ABC', True,
                                     [fault(1, 'refill', 'io_error')]), request('next', 'ABC')]))
        self.assertEqual(ledger['final_residents'], ['A', 'C'])
        self.assertEqual(ledger['summary']['refill_attempts'], 3)
        self.assertEqual(ledger['summary']['refill_ack_bytes'], 2)
        self.assertEqual(ledger['requests'][1]['adopted_tokens'], 10)

    def test_unknown_load_retains_lower_bound_and_disables_refill(self):
        for status in ('unknown', 'timeout'):
            with self.subTest(status=status):
                ledger = self.run_plan(plan([put('A'), put('B'),
                    request('r', 'AB', True, [fault(1, 'load', status)]), request('next', 'AB')]))
                row = ledger['requests'][0]
                self.assertIsNone(row['adopted_tokens'])
                self.assertFalse(row['adoption_known'])
                self.assertEqual(row['first_unconfirmed_block'], 1)
                self.assertEqual(row['adopted_lower_bound_tokens'], 10)
                self.assertEqual(row['refill_suppressed'], 'unknown_adoption')
                self.assertEqual(ledger['summary']['refill_attempts'], 0)
                self.assertEqual(ledger['summary']['target_tokens'], 40)
                self.assertEqual(ledger['summary']['confirmed_adopted_lower_bound_tokens'], 30)
                self.assertIsNone(ledger['summary']['token_reuse_ratio'])
                self.assertEqual(ledger['summary']['status'], 'inconclusive')

    def test_lookup_failure_does_not_become_miss_or_refill(self):
        for status in ('io_error', 'timeout', 'unknown'):
            with self.subTest(status=status):
                ledger = self.run_plan(plan([put('A'), request('r', 'A', True,
                    [fault(0, 'lookup', status)])]))
                self.assertEqual(ledger['requests'][0]['adopted_tokens'], 0)
                self.assertEqual(ledger['summary']['refill_attempts'], 0)
                self.assertEqual(ledger['summary']['lookup_outcomes'][status], 1)
                self.assertEqual(ledger['summary']['lookup_outcomes']['miss'], 0)

    def test_load_rejected_recompute_and_refill_is_separate(self):
        ledger = self.run_plan(plan([put('A'), put('B'), request('r', 'AB', True,
                                     [fault(1, 'load', 'rejected')])]))
        self.assertEqual(ledger['requests'][0]['adopted_tokens'], 10)
        self.assertEqual(ledger['summary']['recompute_proxy_tokens'], 10)
        self.assertEqual(ledger['summary']['refill_ack_bytes'], 1)

    def test_generation_and_namespace_are_distinct(self):
        objects = [dict(object_id=name, namespace=ns, key='same', generation=g,
                        tokens=10, bytes=1) for name, ns, g in
                   [('A', 'n', 0), ('B', 'n', 1), ('C', 'other', 0)]]
        ledger = self.run_plan(plan([put('A'), request('r', 'ABC')], objects=objects))
        self.assertEqual(ledger['requests'][0]['adopted_tokens'], 10)
        self.assertEqual(ledger['final_residents'], ['A'])

    def test_byte_capacity_variable_tokens_and_oversize(self):
        objects = [dict(object_id=key, namespace='n', key=key, generation=0,
                        tokens=tokens, bytes=size) for key, tokens, size in
                   [('A', 3, 2), ('B', 7, 3), ('C', 20, 6)]]
        ledger = self.run_plan(plan([put('A'), put('B'), put('C'), request('r', 'ABC')],
                                    capacity=5, objects=objects))
        self.assertEqual(ledger['requests'][0]['adopted_tokens'], 10)
        self.assertEqual(ledger['summary']['oversize_writes'], 1)
        self.assertEqual(ledger['summary']['occupancy_bytes'], 5)
        self.assertEqual(ledger['summary']['token_reuse_ratio'],
                         dict(numerator=1, denominator=3))

    def test_empty_plan_and_zero_target_are_not_applicable(self):
        for actions in ([], [request('empty', '')]):
            summary = self.run_plan(plan(actions))['summary']
            self.assertEqual(summary['status'], 'not_applicable')
            self.assertIsNone(summary['token_reuse_ratio'])
            self.assertEqual(summary['eligible_requests'], 0)

    def test_injection_must_be_reached_and_writes_cannot_be_unknown(self):
        cases = [plan([request('r', 'AB', False, [fault(1, 'load', 'io_error')])]),
                 plan([put('A', 'unknown')]),
                 plan([request('r', 'A', True, [fault(0, 'refill', 'unknown')])])]
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                dynamic.replay(value)

    def test_strict_fields_types_limits_identities_and_metadata(self):
        base = plan([request('r', 'AB')])
        cases = []
        for field, value in [('capacity_bytes', True), ('capacity_bytes', 0),
                             ('replacement', 'lifetime'), ('synthetic', False)]:
            changed = copy.deepcopy(base)
            changed[field] = value
            cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['extra'] = 1
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['objects'][0]['bytes'] = 0
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['objects'][1]['key'] = 'A'
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['actions'][0]['object_ids'] = ['A', 'A']
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['actions'].append(copy.deepcopy(changed['actions'][0]))
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['actions'][0]['object_ids'] = ['missing']
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['objects'] *= 1000
        cases.append(seal(changed))
        changed = copy.deepcopy(base)
        changed['sha256'] = '0' * 64
        cases.append(changed)
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                dynamic.replay(value)

    def test_dependency_aliases_and_duplicate_faults_are_rejected(self):
        cases = [plan([request('ab', 'AB'), request('cb', 'CB')]),
                 plan([request('r', 'A', False,
                               [fault(0, 'lookup', 'io_error'),
                                fault(0, 'lookup', 'timeout')])])]
        out_of_range = plan([request('r', 'A')])
        out_of_range['faults'] = [dict(action_index=0, block_index=1,
                                       phase='lookup', status='io_error')]
        cases.append(seal(out_of_range))
        for value in cases:
            with self.subTest(value=value), self.assertRaises(ValueError):
                dynamic.replay(value)

    def test_new_chains_and_repeated_hot_chain_compete_without_hidden_reads(self):
        actions = [put('A'), put('B'), request('hot-0', 'AB')]
        for index, key in enumerate('CDE'):
            actions += [put(key), request(f'new-{index}', key)]
        actions += [request('hot-again', 'AB')]
        ledger = self.run_plan(plan(actions, capacity=3))
        self.assertEqual(ledger['summary']['eligible_requests'], 5)
        self.assertEqual(ledger['summary']['target_tokens'], 70)
        self.assertEqual(ledger['summary']['known_adopted_tokens'], 50)
        self.assertEqual(ledger['summary']['lookup_outcomes']['hit'], 5)
        self.assertEqual(ledger['summary']['lookup_outcomes']['miss'], 1)
        self.assertEqual(ledger['summary']['evicted_previously_adopted_objects'], 2)
        self.assertEqual(ledger['requests'][-1]['lookup'], ['miss', 'not_attempted'])

    def test_refill_can_evict_its_own_prefix_without_changing_frozen_adoption(self):
        ledger = self.run_plan(plan([put('A'), request('first', 'ABC', True),
                                     request('again', 'ABC')], capacity=2))
        self.assertEqual([row['adopted_tokens'] for row in ledger['requests']], [10, 0])
        self.assertEqual(ledger['final_residents'], ['B', 'C'])
        self.assertEqual(ledger['summary']['refill_attempts'], 2)
        self.assertEqual(ledger['summary']['evicted_previously_adopted_objects'], 1)

    def test_request_average_is_not_token_weighted_and_failed_load_not_hit(self):
        objects = [dict(object_id=key, namespace='n', key=key, generation=0,
                        tokens=tokens, bytes=1) for key, tokens in [('A', 1), ('B', 9)]]
        ledger = self.run_plan(plan([put('A'), put('B'), request('small', 'A'),
                                     request('large', 'B', False,
                                             [fault(0, 'load', 'rejected')])], objects=objects))
        summary = ledger['summary']
        self.assertEqual(summary['token_reuse_ratio'], dict(numerator=1, denominator=10))
        self.assertEqual(summary['mean_request_reuse_ratio'], dict(numerator=1, denominator=2))
        self.assertEqual(summary['lookup_outcomes']['hit'], 2)
        self.assertEqual(summary['load_outcomes']['adopted'], 1)

    def test_rehashed_ledger_tampering_rejected(self):
        value = dynamic.generate()
        ledger = dynamic.replay(value)
        changes = [lambda x: x['requests'].pop(),
                   lambda x: x['requests'].reverse(),
                   lambda x: x['summary'].update(known_adopted_tokens=999),
                   lambda x: x['events'][0].update(status='io_error'),
                   lambda x: x['final_residents'].reverse(),
                   lambda x: x['requests'][0].update(adopted_tokens=True),
                   lambda x: x.update(extra=True)]
        for change in changes:
            altered = copy.deepcopy(ledger)
            change(altered)
            seal(altered)
            with self.subTest(change=change), self.assertRaises(ValueError):
                dynamic.check(value, altered)

    def test_exhaustive_small_residency_prefix_oracle(self):
        for size in range(1, 5):
            for bits in itertools.product((False, True), repeat=size):
                keys = 'ABCD'[:size]
                actions = [put(key) for key, present in zip(keys, bits) if present]
                ledger = self.run_plan(plan(actions + [request('r', keys)], capacity=5))
                expected = next((i for i, bit in enumerate(bits) if not bit), size) * 10
                self.assertEqual(ledger['requests'][0]['adopted_tokens'], expected)

    def test_definite_load_io_failure_keeps_exact_prefix_and_no_refill(self):
        ledger = self.run_plan(plan([put('A'), put('B'), request('r', 'AB', True,
                                     [fault(1, 'load', 'io_error')])]))
        row = ledger['requests'][0]
        self.assertEqual(row['adopted_tokens'], 10)
        self.assertTrue(row['adoption_known'])
        self.assertEqual(row['refill_suppressed'], 'read_failure')
        self.assertEqual(ledger['summary']['load_outcomes']['io_error'], 1)
        self.assertEqual(ledger['summary']['refill_attempts'], 0)

    def test_independent_serial_interleaving_reference(self):
        # List-based reference intentionally does not call model/cache helpers.
        sizes, tokens = dict(A=2, B=1, C=2), dict(A=3, B=5, C=7)
        objects = [dict(object_id=key, namespace='n', key=key, generation=0,
                        tokens=tokens[key], bytes=sizes[key]) for key in 'ABC']
        alphabet = [('put', 'A'), ('put', 'B'), ('remove', 'A'),
                    ('request', 'AB', False), ('request', 'AB', True),
                    ('request', 'AC', True)]
        for capacity in range(1, 4):
            for sequence in itertools.product(alphabet, repeat=3):
                resident, adopted, occupancy, actions = [], [], [], []
                target = refill_bytes = attempts = evictions = 0
                for index, item in enumerate(sequence):
                    op, key = item[:2]
                    if op == 'request':
                        first = next((i for i, value in enumerate(key)
                                      if value not in resident), len(key))
                        adopted.append(sum(tokens[value] for value in key[:first]))
                        target += sum(tokens[value] for value in key)
                        writes = key[first:] if item[2] else []
                        actions.append(request(str(index), key, item[2]))
                    elif op == 'remove':
                        if key in resident:
                            resident.remove(key)
                        writes = []
                        actions.append(dict(op='remove', object_id=key))
                    else:
                        writes = [key]
                        actions.append(put(key))
                    for value in writes:
                        attempts += op == 'request'
                        if sizes[value] > capacity:
                            continue
                        if value in resident:
                            resident.remove(value)
                        while sum(sizes[entry] for entry in resident) + sizes[value] > capacity:
                            resident.pop(0)
                            evictions += 1
                        resident.append(value)
                        if op == 'request':
                            refill_bytes += sizes[value]
                    occupancy.append(sum(sizes[value] for value in resident))
                ledger = dynamic.replay(plan(actions, capacity, objects))
                observed = (ledger['final_residents'],
                            [row['adopted_tokens'] for row in ledger['requests']],
                            [row['occupancy_bytes'] for row in ledger['action_states']],
                            ledger['summary']['target_tokens'],
                            ledger['summary']['refill_ack_bytes'],
                            ledger['summary']['refill_attempts'], ledger['summary']['evictions'])
                self.assertEqual(observed, (resident, adopted, occupancy, target,
                                            refill_bytes, attempts, evictions),
                                 (capacity, sequence))

    def test_cli_isolated_from_other_directory_and_exclusive_outputs(self):
        script = ROOT / 'deploy/validation/prefix_dynamic.py'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def run(*args):
                return subprocess.run([sys.executable, '-I', str(script), *map(str, args)],
                                      cwd=root, capture_output=True, text=True, timeout=10)
            sample, ledger, summary = root/'plan.json', root/'ledger.json', root/'summary.json'
            self.assertEqual(run('generate', '--output', sample).returncode, 0)
            self.assertEqual(run('replay', '--plan', sample, '--output', ledger).returncode, 0)
            self.assertEqual(run('check', '--plan', sample, '--ledger', ledger,
                                 '--output', summary).returncode, 0)
            self.assertEqual(json.loads(summary.read_text())['known_adopted_tokens'], 70)
            original = sample.read_bytes()
            self.assertEqual(run('replay', '--plan', sample, '--output', sample).returncode, 1)
            self.assertEqual(sample.read_bytes(), original)
            alias = root/'alias'
            alias.symlink_to(sample)
            self.assertEqual(run('generate', '--output', alias).returncode, 1)
            self.assertEqual(run('generate', '--output', sample).returncode, 1)
            bad = root/'bad.json'
            bad.write_text('{"schema":1,"schema":2}')
            self.assertEqual(run('replay', '--plan', bad, '--output', root/'badout').returncode, 1)


if __name__ == '__main__':
    unittest.main()
