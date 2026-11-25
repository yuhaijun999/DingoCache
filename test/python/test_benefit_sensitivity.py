"""Offline sensitivity accounting; fake memory clients do not prove RDMA."""

import copy
from decimal import Decimal
from fractions import Fraction
import json
from pathlib import Path
import random
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'deploy/validation'))
import benefit_sensitivity as benefit
import long_residence as residence


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Client:
    transport = 'rdma'

    def __init__(self, *, lose_hot=False, lose_first=False):
        self.lose_hot = lose_hot
        self.lose_first = lose_first
        self.batches = 0

    def put(self, keys, values):
        pass

    def read_outcomes(self, keys, values):
        self.batches += 1
        # Two warm, two preage and one ready batch precede measurement.
        miss = self.lose_hot and self.batches == 6
        return [{'outcome': 'get_miss' if miss else 'get_hit', 'status': int(miss)}
                for key in keys]

    def read_outcome(self, key, value):
        # Refill verification is a second read of the same first-reuse key.
        first = key not in getattr(self, 'seen', set())
        self.seen = getattr(self, 'seen', set()) | {key}
        return 'get_miss' if self.lose_first and first else 'get_hit'


def report(*, lose_hot=False, lose_first=False):
    config = residence.profile('accelerated', hot_refill=True)
    config.update(protect_sec=2, delays_sec=[1, 3, 4], steps=6, prefill=10,
                  cohort_size=2, arrival_batch=2, length=4, preage_sec=2)
    clock = Clock()
    clients = [Client(lose_hot=lose_hot, lose_first=lose_first) for _ in range(2)]
    value = residence.run(clients, config, clock=clock, sleep=clock.sleep,
                          eviction_counter=lambda: max(0, clock.now - 2))
    residence.check_report(value, config)
    return value


class BenefitSensitivityTest(unittest.TestCase):
    def test_continuous_stream_cannot_silently_omit_business_reads(self):
        value = report()
        value['config']['continuous_reuse'] = True
        # Even a validated new ledger cannot use this fixed-cohort cost model.
        with mock.patch.object(residence, 'check_report') as validate:
            with self.assertRaisesRegex(ValueError, 'continuous'):
                benefit.summarize(value)
            validate.assert_not_called()

    def test_complete_ledgers_separate_business_reads_from_verify(self):
        value = benefit.summarize(report(lose_first=True))
        self.assertEqual(value['groups']['hot'], {'attempts': 24, 'hits': 24, 'misses': 0})
        self.assertEqual(value['groups']['first_3'], {'attempts': 4, 'hits': 0, 'misses': 4})
        self.assertEqual(value['refill_acks'], 12)
        self.assertEqual(value['refill_payload_bytes'], 48)
        self.assertEqual(value['verify_hits_excluded'], 12)
        self.assertEqual(value['full_totals']['get_attempts'], 80)

    def test_comparison_counts_each_scheduled_miss_and_write_saving(self):
        comparison = benefit.compare(report(lose_first=True), report())
        self.assertEqual(comparison['savings']['first_3_miss'], 4)
        self.assertEqual(comparison['savings']['hot_miss'], 0)
        self.assertEqual(comparison['savings']['refill_ack'], 12)
        self.assertEqual(comparison['savings']['refill_payload_byte'], 48)
        self.assertNotIn('winner', comparison)

    def test_more_hot_misses_are_negative_savings_and_no_ratio_division(self):
        comparison = benefit.compare(report(), report(lose_hot=True))
        self.assertEqual(comparison['savings']['hot_miss'], -4)
        self.assertEqual(comparison['savings']['refill_ack'], -4)
        self.assertEqual(comparison['savings']['refill_payload_byte'], -16)

    def test_complete_same_report_is_zero_baseline_safe(self):
        comparison = benefit.compare(report(), report())
        self.assertTrue(all(value == 0 for value in comparison['savings'].values()))
        weights = dict.fromkeys(comparison['savings'], 0)
        self.assertEqual(benefit.evaluate(comparison, weights)['net_saving'], '0')
        weights.pop('hot_miss')
        self.assertEqual(benefit.break_even(comparison, weights, 'hot_miss')['condition'], 'tie_for_all_nonnegative_weights')

    def test_no_implicit_business_weights(self):
        comparison = benefit.compare(report(), report())
        for weights in ({}, {'hot_miss': 1}, dict(comparison['savings'], extra=1)):
            with self.assertRaises(ValueError):
                benefit.evaluate(comparison, weights)

    def test_negative_bool_nonfinite_and_nonnumeric_costs_rejected(self):
        comparison = benefit.compare(report(), report())
        for bad in (-1, True, float('nan'), float('inf'), 'NaN', 'bad', None):
            weights = dict.fromkeys(comparison['savings'], 0)
            weights['hot_miss'] = bad
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                benefit.evaluate(comparison, weights)

    def test_extreme_finite_weights_reject_decimal_overflow(self):
        comparison = benefit.compare(report(lose_first=True), report())
        weights = dict.fromkeys(comparison['savings'], 0)
        weights['first_3_miss'] = '1e999999999'
        with self.assertRaisesRegex(ValueError, 'decimal range'):
            benefit.evaluate(comparison, weights)
        weights.pop('hot_miss')
        with self.assertRaisesRegex(ValueError, 'decimal range'):
            benefit.break_even(comparison, weights, 'hot_miss')

    def test_underflow_and_unbounded_cost_text_rejected(self):
        comparison = benefit.compare(report(lose_first=True), report())
        for bad in ('1e-999999999', '9' * 257, '1e' + '9' * 257):
            weights = dict.fromkeys(comparison['savings'], 0)
            weights['first_3_miss'] = bad
            with self.subTest(value=bad[:20]), self.assertRaises(ValueError):
                benefit.evaluate(comparison, weights)

    def test_cost_units_are_per_miss_ack_and_payload_byte(self):
        comparison = benefit.compare(report(lose_first=True), report())
        weights = dict.fromkeys(comparison['savings'], 0)
        weights.update(first_1_miss='1.5', first_3_miss=2, first_4_miss=3,
                       refill_ack='0.25', refill_payload_byte='0.125')
        result = benefit.evaluate(comparison, weights)
        self.assertEqual(result['net_saving'], '35')
        self.assertTrue(result['hypothetical_only'])

    def test_break_even_both_directions_and_zero_slope(self):
        comparison = benefit.compare(report(lose_first=True), report())
        comparison['savings']['hot_miss'] = -8
        weights = dict.fromkeys(comparison['savings'], 0)
        weights['hot_miss'] = 1
        weights.pop('first_3_miss')
        boundary = benefit.break_even(comparison, weights, 'first_3_miss')
        self.assertEqual(boundary['threshold'], '2')
        self.assertEqual(boundary['condition'], 'weight_greater_than_threshold')
        weights = dict.fromkeys(comparison['savings'], 0)
        weights['first_3_miss'] = 1
        weights.pop('hot_miss')
        boundary = benefit.break_even(comparison, weights, 'hot_miss')
        self.assertEqual(boundary['threshold'], '0.5')
        self.assertEqual(boundary['condition'], 'weight_less_than_threshold')
        comparison['savings']['hot_miss'] = 0
        self.assertEqual(benefit.break_even(comparison, weights, 'hot_miss')['condition'], 'positive_for_all_nonnegative_weights')
        weights['first_3_miss'] = 0
        weights['refill_ack'] = 1
        comparison['savings']['refill_ack'] = -1
        self.assertEqual(benefit.break_even(comparison, weights, 'hot_miss')['condition'], 'negative_for_all_nonnegative_weights')

    def test_break_even_unknown_or_already_supplied_variable_rejected(self):
        comparison = benefit.compare(report(), report())
        for variable in ('unknown', 'hot_miss'):
            with self.assertRaises(ValueError):
                benefit.break_even(comparison, dict.fromkeys(comparison['savings'], 0), variable)

    def test_incomplete_or_failed_raw_ledger_rejected(self):
        for mutate in (lambda r: r.update(passed=False),
                       lambda r: r['rows'].pop(),
                       lambda r: r['actual'].update(hot_gets=0),
                       lambda r: r.update(verify_hits=99),
                       lambda r: r.update(transport_modes=['tcp', 'tcp'])):
            value = report()
            mutate(value)
            with self.assertRaises(ValueError):
                benefit.compare(report(), value)

    def test_unmatched_load_and_refill_mode_rejected(self):
        for field, value in (('steps', 5), ('hot_refill', False), ('seed', 1)):
            value_report = report()
            value_report['config'][field] = value
            with self.assertRaises(ValueError):
                benefit.compare(report(), value_report)

    def test_hot_and_delayed_writes_stay_separate_and_verifies_never_become_hits(self):
        baseline = report(lose_first=True)
        candidate = report(lose_hot=True)
        comparison = benefit.compare(baseline, candidate)
        self.assertEqual(comparison['baseline']['hot_refill_acks'], 0)
        self.assertEqual(comparison['baseline']['delayed_refill_acks'], 12)
        self.assertEqual(comparison['candidate']['hot_refill_acks'], 4)
        self.assertEqual(comparison['candidate']['delayed_refill_acks'], 0)
        self.assertEqual(comparison['savings']['verify_read'], 8)
        self.assertEqual(comparison['candidate']['groups']['hot']['hits'], 20)
        self.assertEqual(comparison['candidate']['verify_hits_excluded'], 4)

    def test_old_complete_no_refill_reports_remain_compatible(self):
        value = report()
        value['config'].pop('hot_refill')
        value.pop('hot_refill')
        value.pop('hot_read_summary')
        for row in value['rows']:
            row.pop('hot_outcomes', None)
        self.assertEqual(benefit.compare(value, copy.deepcopy(value))['savings']['hot_miss'], 0)
        value['rows'][0]['count'] -= 1
        with self.assertRaises(ValueError):
            benefit.summarize(value)

    def test_negative_threshold_keeps_nonnegative_domain(self):
        comparison = benefit.compare(report(lose_first=True), report())
        weights = dict.fromkeys(comparison['savings'], 0)
        weights['refill_ack'] = 1
        weights.pop('first_3_miss')
        boundary = benefit.break_even(comparison, weights, 'first_3_miss')
        self.assertEqual(boundary['threshold'], '-3')
        self.assertIn('weight >= 0', boundary['domain'])

    def test_cli_explicit_weights_and_invalid_input_no_success_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            baseline, candidate = report(lose_first=True), report()
            (root / 'a.json').write_text(json.dumps(baseline))
            (root / 'b.json').write_text(json.dumps(candidate))
            weights = dict.fromkeys(benefit.compare(baseline, candidate)['savings'], 0)
            weights['first_3_miss'] = 1
            (root / 'weights.json').write_text(json.dumps(weights))
            args = ['--baseline', str(root / 'a.json'), '--candidate', str(root / 'b.json'),
                    '--weights', str(root / 'weights.json'), '--output', str(root / 'out.json')]
            self.assertEqual(benefit.main(args), 0)
            self.assertEqual(json.loads((root / 'out.json').read_text())['evaluation']['net_saving'], '4')
            (root / 'out.json').unlink()
            candidate['passed'] = False
            (root / 'b.json').write_text(json.dumps(candidate))
            with self.assertRaises(SystemExit) as error:
                benefit.main(args)
            self.assertEqual(error.exception.code, 2)
            self.assertFalse((root / 'out.json').exists())

    def test_large_cancellation_is_exact_and_input_order_independent(self):
        comparison = {'savings': {'large_positive': 1, 'small_positive': 1, 'large_negative': -1}}
        weights = {'large_positive': '1e60', 'small_positive': 1, 'large_negative': '1e60'}
        self.assertEqual(benefit.evaluate(comparison, weights)['net_saving'], '1')
        self.assertEqual(benefit.evaluate(comparison, dict(reversed(list(weights.items()))))['net_saving'], '1')
        fixed = {'large_positive': '1e60', 'large_negative': '1e60'}
        self.assertEqual(benefit.break_even(comparison, fixed, 'small_positive')['threshold'], '0')

    def test_recurring_threshold_retains_exact_tie_fraction(self):
        comparison = {'savings': {'benefit': 3, 'cost': -1}}
        boundary = benefit.break_even(comparison, {'cost': 1}, 'benefit')
        self.assertEqual(boundary['exact_threshold'], {'numerator': '1', 'denominator': '3'})
        self.assertTrue(boundary['threshold_is_display_only'])
        self.assertEqual(len(boundary['threshold'].split('.')[1]), 50)

    def test_widely_separated_valid_decimal_scales_are_not_lost(self):
        comparison = {'savings': {'large': 1, 'small': 1, 'cancel': -1}}
        result = benefit.evaluate(comparison, {'large': '1e1000', 'small': '1e-1000', 'cancel': '1e1000'})
        self.assertEqual(result['net_saving'], '1E-1000')
        comparison['savings']['large'] = 1 << 63
        with self.assertRaises(ValueError):
            benefit.evaluate(comparison, {'large': 1, 'small': 1, 'cancel': 1})

    def test_reduced_power_two_and_five_denominators_remain_exact(self):
        for cost in ('0.000030517578125', '0.000000000032768'):
            comparison = {'savings': {'benefit': 1, 'cancel': -1, 'variable': 1}}
            result = benefit.evaluate(comparison, {'benefit': cost, 'cancel': 0, 'variable': 0})
            self.assertEqual(Decimal(result['net_saving']), Decimal(cost))
            self.assertEqual(Decimal(result['contributions']['benefit']), Decimal(cost))
            boundary = benefit.break_even(comparison, {'benefit': cost, 'cancel': 0}, 'variable')
            self.assertEqual(Decimal(boundary['fixed_saving']), Decimal(cost))
            result = benefit.evaluate(comparison, {'benefit': cost, 'cancel': cost, 'variable': 0})
            self.assertEqual(result['net_saving'], '0')

    def test_bounded_decimal_sweep_preserves_exact_net_and_threshold(self):
        random_source = random.Random(0xDF600)
        for _ in range(40):
            comparison = {'savings': {name: random_source.randint(-20, 20)
                                      for name in ('a', 'b', 'c')}}
            weights = {name: f"{random_source.randint(0, 100000)}e{random_source.choice((-1000, -80, -15, 0, 15, 80, 990))}"
                       for name in comparison['savings']}
            expected = sum((Fraction(Decimal(weight)) * comparison['savings'][name]
                            for name, weight in weights.items()), Fraction(0))
            self.assertEqual(Fraction(Decimal(benefit.evaluate(comparison, weights)['net_saving'])), expected)
            variable = 'c'
            weights.pop(variable)
            boundary = benefit.break_even(comparison, weights, variable)
            slope = comparison['savings'][variable]
            if slope:
                fixed = sum((Fraction(Decimal(weight)) * comparison['savings'][name]
                             for name, weight in weights.items()), Fraction(0))
                exact = boundary['exact_threshold']
                self.assertEqual(Fraction(int(exact['numerator']), int(exact['denominator'])), -fixed / slope)

    def test_cli_preserves_all_inputs_and_existing_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [root / name for name in ('a.json', 'b.json', 'weights.json', 'old.json')]
            paths[0].write_text(json.dumps(report()))
            paths[1].write_text(json.dumps(report()))
            paths[2].write_text(json.dumps(dict.fromkeys(benefit.compare(report(), report())['savings'], 0)))
            paths[3].write_text('retained output')
            original = {path: path.read_bytes() for path in paths}
            for output in paths:
                with self.subTest(output=output.name):
                    with self.assertRaises(SystemExit) as error:
                        benefit.main(['--baseline', str(paths[0]), '--candidate', str(paths[1]),
                                      '--weights', str(paths[2]), '--output', str(output)])
                    self.assertEqual(error.exception.code, 2)
                    self.assertEqual({path: path.read_bytes() for path in paths}, original)

    def test_cli_without_weights_outputs_no_winner(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'a.json').write_text(json.dumps(report(lose_first=True)))
            (root / 'b.json').write_text(json.dumps(report()))
            output = root / 'out.json'
            self.assertEqual(benefit.main(['--baseline', str(root / 'a.json'), '--candidate', str(root / 'b.json'), '--output', str(output)]), 0)
            result = json.loads(output.read_text())
            self.assertNotIn('evaluation', result)
            self.assertNotIn('winner', result)
            self.assertIn('baseline_sha256', result)


if __name__ == '__main__':
    unittest.main()
