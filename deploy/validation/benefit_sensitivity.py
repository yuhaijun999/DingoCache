#!/usr/bin/env python3
"""Offline, hypothetical cost sensitivity for validated residence client ledgers.

No default business weights or production winner are supplied. Miss cost excludes
separately weighted refill/verification work; for an all-inclusive miss cost, set
all write and verification weights to zero. Costs share one operator-chosen unit:
per scheduled miss, acknowledged refill PUT, payload byte, and verification GET.
Cost text is limited to 256 characters with decimal exponents within +/-1000.
ACK and payload costs are collinear for fixed-size objects, not independently
measured device costs. This validates client ledgers, not server/thermal evidence.
"""

import argparse
from decimal import Decimal, InvalidOperation, localcontext
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import sys

import long_residence as residence


def summarize(report):
    """Reject incomplete raw reports before deriving scheduled business counts."""
    try:
        config = report['config']
        totals = residence.check_report(report, config)
        groups = {name: {'attempts': 0, 'hits': 0, 'misses': 0}
                  for name in ['hot', 'cooled'] + [f'first_{d}' for d in config['delays_sec']]}
        for row in report['rows']:
            if row['kind'] in ('hot', 'cooled'):
                name, hits, misses = row['kind'], row['hits'], row['misses']
            elif row['kind'] == 'first':
                name = f"first_{row['target_age_sec']}"
                hits = int(row['outcome'] == 'get_hit')
                misses = 1 - hits
            else:
                continue
            groups[name]['attempts'] += hits + misses
            groups[name]['hits'] += hits
            groups[name]['misses'] += misses
        hot = report.get('hot_refill', {})
        delayed = report['refill_puts']
        hot_puts = hot.get('refill_puts', 0)
        return {'config': dict(config), 'groups': groups,
                'hot_refill_acks': hot_puts, 'delayed_refill_acks': delayed,
                'refill_acks': delayed + hot_puts,
                'refill_payload_bytes': report['refill_payload_bytes'] + hot.get('refill_payload_bytes', 0),
                'verify_hits_excluded': report['verify_hits'] + hot.get('verify_hits', 0),
                'verify_reads': report['verify_gets'] + hot.get('verify_gets', 0),
                'full_totals': totals}
    except (KeyError, TypeError, IndexError, ValueError) as error:
        raise ValueError('invalid residence client ledger: ' + str(error)) from error


def compare(baseline_report, candidate_report):
    """Positive coefficients save cost relative to baseline; no implicit weights.

    Only protect_sec may differ in the workload configuration. Actual policy,
    environment and effective parameters require the independent native auditor.
    """
    baseline, candidate = summarize(baseline_report), summarize(candidate_report)
    configs = [{k: v for k, v in summary['config'].items() if k != 'protect_sec'}
               for summary in (baseline, candidate)]
    if configs[0] != configs[1]:
        raise ValueError('unmatched workload or refill mode')
    savings = {}
    for name, group in baseline['groups'].items():
        other = candidate['groups'][name]
        if group['attempts'] != other['attempts']:
            raise ValueError('unmatched business-read denominator')
        savings[name + '_miss'] = group['misses'] - other['misses']
    for name, field in [('refill_ack', 'refill_acks'),
                        ('refill_payload_byte', 'refill_payload_bytes'),
                        ('verify_read', 'verify_reads')]:
        savings[name] = baseline[field] - candidate[field]
    return {'model': 'residence-cost-sensitivity-v1', 'formal_ab': False,
            'hypothetical_only': True, 'baseline': baseline, 'candidate': candidate,
            'savings': savings,
            'cost_contract': 'miss excludes separately weighted refill and verification; '
                             'for all-inclusive misses set refill_ack/refill_payload_byte/verify_read to zero',
            'scope': 'validated client ledgers; no server audit, real business weights or production approval'}


def _weights(comparison, weights, *, variable=None):
    coefficients = comparison.get('savings')
    if (not isinstance(coefficients, dict) or not coefficients
            or any(type(value) is not int or not -(1 << 63) <= value < (1 << 63)
                   for value in coefficients.values())):
        raise ValueError('invalid integer savings coefficients')
    if variable is not None and variable not in coefficients:
        raise ValueError('unknown break-even variable')
    expected = set(coefficients) - ({variable} if variable is not None else set())
    if not isinstance(weights, dict) or set(weights) != expected:
        raise ValueError('every fixed weight must be explicit; no extra weights')
    parsed = {}
    for name, value in weights.items():
        if type(value) not in (int, float, str):
            raise ValueError('weights must be finite nonnegative decimal costs')
        text = str(value)
        if len(text) > 256:
            raise ValueError('decimal cost text exceeds 256 characters')
        try:
            number = Decimal(text)
        except InvalidOperation as error:
            raise ValueError('invalid decimal cost') from error
        if not number.is_finite() or number < 0:
            raise ValueError('weights must be finite nonnegative decimal costs')
        if not -1000 <= number.as_tuple().exponent <= 1000 or number.adjusted() > 1000:
            raise ValueError('cost arithmetic exceeds decimal range: exponent must be within +/-1000')
        parsed[name] = number
    return parsed


def _exact_decimal(value):
    # Finite-decimal denominators contain only 2/5 factors. Their digit count is
    # not a precision bound: 1/32768 needs 11 significant digits, not 5.
    denominator = value.denominator
    powers = []
    for factor in (2, 5):
        count = 0
        while denominator % factor == 0:
            denominator //= factor
            count += 1
        powers.append(count)
    if denominator != 1:
        raise ValueError('nonterminating exact decimal')
    with localcontext() as context:
        context.prec = len(str(abs(value.numerator))) + max(powers) + 2
        return str(Decimal(value.numerator) / Decimal(value.denominator))


def evaluate(comparison, weights):
    """Exact, order-independent net savings, never a production recommendation."""
    parsed = _weights(comparison, weights)
    terms = {name: Fraction(cost) * comparison['savings'][name] for name, cost in parsed.items()}
    net = sum(terms.values(), Fraction(0))
    return {'hypothetical_only': True, 'net_saving': _exact_decimal(net),
            'contributions': {name: _exact_decimal(value) for name, value in terms.items()},
            'arithmetic': 'exact finite-decimal costs and integer counts',
            'weights': {name: str(value) for name, value in parsed.items()},
            'interpretation': 'positive saves hypothetical cost; zero ties; negative adds cost; '
                              'not a production acceptance result'}


def break_even(comparison, fixed_weights, variable):
    """Solve net_saving > 0 exactly; round only the displayed decimal threshold.

    Exact numerator/denominator identify the tie even for recurring decimals.
    Negative thresholds retain an explicit nonnegative domain. Zero slopes have
    no finite break-even and never cause division by zero.
    """
    parsed = _weights(comparison, fixed_weights, variable=variable)
    fixed = sum((Fraction(value) * comparison['savings'][name]
                 for name, value in parsed.items()), Fraction(0))
    slope = comparison['savings'][variable]
    boundary = -fixed / slope if slope else None
    with localcontext() as context:
        context.prec = 50
        threshold = (str(Decimal(boundary.numerator) / Decimal(boundary.denominator))
                     if boundary is not None else None)
    condition = ('weight_greater_than_threshold' if slope > 0 else
                 'weight_less_than_threshold' if slope < 0 else
                 'positive_for_all_nonnegative_weights' if fixed > 0 else
                 'negative_for_all_nonnegative_weights' if fixed < 0 else
                 'tie_for_all_nonnegative_weights')
    return {'hypothetical_only': True, 'variable': variable, 'coefficient': slope,
            'fixed_saving': _exact_decimal(fixed), 'threshold': threshold, 'condition': condition,
            'exact_threshold': ({'numerator': str(boundary.numerator),
                                 'denominator': str(boundary.denominator)}
                                if boundary is not None else None),
            'domain': 'weight >= 0; equality at the exact rational threshold is a tie',
            'threshold_display_significant_digits': 50, 'threshold_is_display_only': True}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, required=True)
    parser.add_argument('--candidate', type=Path, required=True)
    parser.add_argument('--weights', type=Path, help='JSON object: every cost explicit, no defaults')
    parser.add_argument('--break-even', help='omit this variable from --weights')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args(argv)
    if args.break_even and not args.weights:
        parser.error('--break-even requires explicit fixed --weights')
    try:
        contents = [path.read_bytes() for path in (args.baseline, args.candidate)]
        result = compare(*(json.loads(content) for content in contents))
        result.update(baseline_sha256=hashlib.sha256(contents[0]).hexdigest(),
                      candidate_sha256=hashlib.sha256(contents[1]).hexdigest())
        if args.weights:
            weights = json.loads(args.weights.read_text())
            result['evaluation'] = (break_even(result, weights, args.break_even) if args.break_even
                                    else evaluate(result, weights))
        with args.output.open('x') as output:
            output.write(json.dumps(result, indent=2) + '\n')
    except (OSError, ValueError) as error:
        parser.error(str(error))
    return 0


if __name__ == '__main__':
    sys.exit(main())
