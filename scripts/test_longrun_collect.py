import contextlib
import gzip
import importlib.util
import io
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    'longrun_collect', Path(__file__).with_name('longrun_collect.py'))
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)
DAY = ['2026-10-09', '10', '11', '12', '9', '100']
ROW = 'sz000001|' + '|'.join(DAY) + '\n'


class LongrunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.bulk = Path(self.temp.name)
        self.env = patch.dict(os.environ, {}, clear=False)
        self.env.start()
        self.addCleanup(self.env.stop)
        os.environ.pop('STOCKS_UNIVERSE_JSON', None)
        for name, value in {
            'BULK': str(self.bulk), 'UNIVERSE': str(self.bulk / 'universe.csv'),
            'PROGRESS': str(self.bulk / '_longrun_progress.json'),
            'RECENT': str(self.bulk / 'day_qfq_longrun_test.csv.gz'),
        }.items():
            p = patch.object(collector, name, value)
            p.start()
            self.addCleanup(p.stop)
        p = patch.object(collector.time, 'sleep')
        p.start()
        self.addCleanup(p.stop)
        p = patch.object(collector.urllib.request, 'urlopen',
                         side_effect=AssertionError('real network forbidden'))
        p.start()
        self.addCleanup(p.stop)

    def universe(self, text='sz000001|平安银行\n'):
        Path(collector.UNIVERSE).write_text(text, encoding='utf-8')

    def test_missing_and_empty_universe_fail(self):
        with self.assertRaises((ValueError, FileNotFoundError)):
            collector.run_kline()
        self.universe('')
        with self.assertRaises(ValueError):
            collector.run_kline()

    def test_json_structure_and_bad_items(self):
        path = self.bulk / 'symbols.json'
        os.environ['STOCKS_UNIVERSE_JSON'] = str(path)
        good = [['sz000001', '000001', '平安银行'],
                ['sh688001', '688001', '华兴源创'],
                ['bj920001', '920001', '测试'],
                ['sh000001', '000001', '指数'],
                ['sz159001', '159001', '基金']]
        path.write_text(json.dumps(good), encoding='utf-8')
        self.assertEqual(collector.load_universe(), ['sz000001', 'sh688001', 'bj920001'])
        for bad in ([], {}, [['sz000001', '600000', '错配']],
                    [['xx000001', '000001', '错误']], [['sz000001']],
                    [['sz00000x', '00000x', '错误']]):
            path.write_text(json.dumps(bad), encoding='utf-8')
            with self.assertRaises(ValueError, msg=repr(bad)):
                collector.load_universe()

    def test_plain_gzip_deduplicate_and_scope(self):
        self.universe()
        (self.bulk / 'day_qfq_a.csv').write_text(ROW, encoding='utf-8')
        with gzip.open(self.bulk / 'day_qfq_b.csv.gz', 'wt') as out:
            out.write(ROW + ROW.replace('sz000001', 'sz000002'))
        self.assertEqual(collector.load_covered(), {'sz000001', 'sz000002'})
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout), patch.object(collector, 'fetch_kline') as fetch:
            self.assertEqual(collector.run_kline(), 0)
        fetch.assert_not_called()
        self.assertIn('covered=1', stdout.getvalue())
        self.assertFalse(Path(collector.RECENT).exists())
        self.assertFalse(Path(collector.PROGRESS).exists())

    def test_gzip_only_is_already_covered(self):
        self.universe()
        with gzip.open(self.bulk / 'day_qfq_a.csv.gz', 'wt') as out:
            out.write(ROW)
        with patch.object(collector, 'fetch_kline') as fetch:
            self.assertEqual(collector.run_kline(), 0)
        fetch.assert_not_called()
        self.assertFalse(Path(collector.RECENT).exists())

    def test_raw_day_only_and_unadjusted_request(self):
        for node, expected in (({'day': [DAY], 'qfqday': [DAY[:1] + ['99'] * 5]}, 1),
                               ({'qfqday': [DAY]}, 0)):
            body = json.dumps({'data': {'sz000001': node}}).encode()
            with patch.object(collector.urllib.request, 'urlopen',
                              return_value=io.BytesIO(body)) as request:
                rows, _ = collector.fetch_kline('sz000001')
            self.assertEqual(len(rows), expected)
            if rows:
                self.assertEqual(rows[0][1], 10.0)
            self.assertNotIn('qfq', request.call_args.args[0].full_url.split('?')[1])

    def test_all_requests_fail_returns_nonzero(self):
        self.universe()
        with patch.object(collector, 'fetch_kline', return_value=([], 'network')), \
                patch.object(collector.time, 'sleep') as sleep:
            self.assertNotEqual(collector.run_kline(), 0)
        self.assertGreaterEqual(sleep.call_args.args[0], 0.8)
        self.assertFalse(Path(collector.RECENT).exists())

    def test_cli_allows_codespace_long_budget_but_rejects_invalid(self):
        with patch.object(sys, 'argv', ['collector', 'kline', '--max-hours', '8']), \
                patch.object(collector, 'run_kline', return_value=0) as run:
            self.assertEqual(collector.main(), 0)
        run.assert_called_once_with(max_hours=8.0)
        for value in ('0', '-1', 'nan', 'inf'):
            with patch.object(sys, 'argv', ['collector', 'kline', '--max-hours', value]), \
                    contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    collector.main()
                self.assertEqual(error.exception.code, 2)

    def test_success_is_pipe_readable_and_resume_preserves_history(self):
        self.universe('sz000001|银行\nsz000002|万科\n')
        history = self.bulk / 'day_qfq_old.csv'
        history.write_text(ROW, encoding='utf-8')
        before = history.read_bytes()
        with patch.object(collector, 'fetch_kline', return_value=([DAY], None)) as fetch:
            self.assertEqual(collector.run_kline(), 0)
            fetch.assert_called_once_with('sz000002')
        with gzip.open(collector.RECENT, 'rt') as source:
            lines = source.readlines()
        self.assertEqual(len(lines), 1)
        self.assertEqual(len(lines[0].strip().split('|')), 7)
        self.assertEqual(collector.load_covered(), {'sz000001', 'sz000002'})
        output_before = Path(collector.RECENT).read_bytes()
        with patch.object(collector, 'fetch_kline') as fetch:
            self.assertEqual(collector.run_kline(), 0)
        fetch.assert_not_called()
        self.assertEqual(history.read_bytes(), before)
        self.assertEqual(Path(collector.RECENT).read_bytes(), output_before)


if __name__ == '__main__':
    unittest.main()
