"""로컬 모의 테스트: 서버/SSH/fio/실제 Kafka 실행 없음."""
import importlib.util
import json
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('suite', ROOT / 'tools/run_experiment_suite.py')
suite = importlib.util.module_from_spec(spec)
spec.loader.exec_module(suite)


class SuiteTests(unittest.TestCase):
    def test_5hz_four_scenarios_three_rounds(self):
        result, calls, output = self.run_mock([0] * 12, extra=[
            '--hz-list', '5', '--rounds', '3', '--scenario-set', 'precreate-auto',
            '--delete-modes', 'IMMEDIATE', '--intervals', '1000', '300000',
            '--iostat', '--cleanup-timeout-seconds', '600', '--require-cleanup-success'])
        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 12)
        for command in calls:
            self.assertIn('--iostat', command)
            self.assertEqual(command[command.index('--cleanup-timeout-seconds') + 1], '600')
        modes = [(c[c.index('--topic-mode') + 1], '--no-delete' in c,
                  c[c.index('--scan-interval-ms') + 1]) for c in calls]
        self.assertEqual(modes, [('precreate', True, '1000'), ('auto', True, '1000'),
                                 ('auto', False, '1000'), ('auto', False, '300000')] * 3)

    def test_stop_when_cleanup_not_confirmed(self):
        result, calls, output = self.run_mock([1], extra=['--require-cleanup-success'])
        self.assertEqual(result, 1)
        self.assertEqual(len(calls), 1)

    def test_retry_wait_default_one_minute(self):
        self.assertEqual(suite.parser().parse_args([]).retry_delay_seconds, 60)
        clock = [0.0]
        def sleep(seconds):
            clock[0] += seconds
        messages = []
        with patch.object(suite.time, 'monotonic', lambda: clock[0]), patch.object(suite.time, 'sleep', sleep):
            suite.wait_before_retry(60, lambda: False, messages.append)
        self.assertEqual(clock[0], 60)
        self.assertEqual(len(messages), 60)

    def test_interrupt_during_retry_wait(self):
        clock = [0.0]
        def sleep(seconds):
            clock[0] += seconds
        with patch.object(suite.time, 'monotonic', lambda: clock[0]), patch.object(suite.time, 'sleep', sleep):
            suite.wait_before_retry(60, lambda: clock[0] >= 3, lambda text: None)
        self.assertEqual(clock[0], 3)

    def run_mock(self, statuses, extra=None, create_csv=False):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root = Path(temp.name)
        calls = []
        queue = iter(statuses)

        def spawn(command, **kwargs):
            calls.append(command)
            status = next(queue)
            if create_csv:
                directory = Path(command[command.index('--output-dir') + 1])
                label = command[command.index('--experiment-label') + 1]
                (directory / f'{label}_metrics.csv').write_text('header\n1\n')
                (directory / f'{label}.run.txt').write_text(f'FINAL_EXIT_CODE={status}\n')
                (directory / f'{label}_deletion.csv').write_text('not collected')
            return types.SimpleNamespace(wait=lambda: status, stdout=iter(['mock output\n']))

        fcntl = types.SimpleNamespace(LOCK_EX=1, LOCK_NB=2, flock=lambda *args: None)
        argv = ['suite', '--output-dir', str(root), '--hz-list', '3', '--rounds', '1',
                '--delete-modes', 'BATCH', '--intervals', '1000', '300000',
                '--retry-delay-seconds', '0'] + (extra or [])
        with patch.object(suite, 'BASE', root), patch.dict(sys.modules, fcntl=fcntl), \
                patch.object(sys, 'argv', argv), patch.object(suite.subprocess, 'Popen', spawn), \
                patch.object(suite.signal, 'signal'):
            result = suite.main()
        output, = root.glob('suite_*')
        return result, calls, output

    def test_retry_once_and_continue_collect_failed_and_success(self):
        result, calls, output = self.run_mock([1, 0, 1, 1], create_csv=True)
        self.assertEqual(result, 1)
        self.assertEqual(len(calls), 4)
        labels = [c[c.index('--experiment-label') + 1] for c in calls]
        self.assertEqual(labels, ['r1_c1_try1', 'r1_c1_try2', 'r1_c2_try1', 'r1_c2_try2'])
        self.assertEqual(len({c[c.index('--prefix') + 1] for c in calls}), 4)
        self.assertEqual(len(list(output.glob('*_metrics.csv'))), 4)
        self.assertFalse((output / 'metrics').exists())
        self.assertEqual(len(list((output / 'artifacts').glob('*_metrics.csv'))), 4)
        self.assertEqual(len(list(output.glob('*_deletion.csv'))), 0)
        manifest = json.loads((output / 'metrics_manifest.json').read_text())
        self.assertEqual([r['final_exit_code'] for r in manifest], ['1', '0', '1', '1'])
        self.assertEqual(suite.collect_metrics(output), 4)

    def test_no_retry_on_interrupt(self):
        result, calls, _ = self.run_mock([130])
        self.assertEqual(result, 130)
        self.assertEqual(len(calls), 1)

    def test_retry_can_be_disabled(self):
        result, calls, _ = self.run_mock([1, 0], ['--failure-retries', '0'])
        self.assertEqual(result, 1)
        self.assertEqual(len(calls), 2)

    def test_full_round_order(self):
        result, calls, _ = self.run_mock([0] * 24, ['--hz-list', '3', '4', '5', '--rounds', '2',
                                                    '--delete-modes', 'BATCH', 'IMMEDIATE'])
        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 24)
        self.assertEqual([c[c.index('--hz') + 1] for c in calls], (['3'] * 4 + ['4'] * 4 + ['5'] * 4) * 2)

    def test_collision_never_overwrites(self):
        _, _, output = self.run_mock([0, 0], create_csv=True)
        first = next(output.glob('*_metrics.csv'))
        first.write_text('different')
        with self.assertRaises(FileExistsError):
            suite.collect_metrics(output)

    def test_precreate_auto_matrix_with_trim(self):
        result, calls, output = self.run_mock([0] * 60, [
            '--scenario-set', 'precreate-auto', '--hz-list', '1', '2', '3', '4', '5',
            '--rounds', '2', '--delete-modes', 'BATCH', 'IMMEDIATE', '--fstrim',
            '--fstrim-wait-seconds', '600', '--step-delay-seconds', '10'])
        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 60)
        def value(c, key):
            return c[c.index(key) + 1]
        for start in range(0, 60, 6):
            block = calls[start:start + 6]
            self.assertEqual([value(c, '--topic-mode') for c in block], ['precreate'] + ['auto'] * 5)
            self.assertEqual(['--no-delete' in c for c in block], [True, True, False, False, False, False])
            self.assertEqual([value(c, '--deletion-mode') for c in block[2:]], ['BATCH', 'BATCH', 'IMMEDIATE', 'IMMEDIATE'])
        self.assertTrue(all('--fstrim' in c and '--no-fstrim' not in c and '--no-fio' in c for c in calls))
        self.assertTrue(all(value(c, '--fstrim-wait-seconds') == '600' and value(c, '--step-delay-seconds') == '10' for c in calls))
        self.assertEqual([value(c, '--hz') for c in calls], sum(([str(hz)] * 6 for hz in range(1, 6)), []) * 2)

    def test_retry_repeats_trim(self):
        result, calls, _ = self.run_mock([1, 0], ['--intervals', '1000', '--fstrim'])
        self.assertEqual(result, 0)
        self.assertEqual(len(calls), 2)
        self.assertTrue(all('--fstrim' in c for c in calls))

    def test_flat_finalize_preserves_pair_and_index(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)
            tag = 'r1_c1_try2_3Hz_auto_fio0_del-on-batch_scan1000_check1000_60min_repeat1_20261008_180000_1234_5678_start_20261008_180001_KST_end_PENDING'
            (directory / (tag + '_metrics.csv')).write_text('value\n1\n')
            (directory / (tag + '.run.txt')).write_text(f'TAG={tag}\nPRODUCER_CSV={directory / (tag + "_metrics.csv")}\n')
            index = directory / 'runs.tsv'
            index.write_text(f'tag\thz\tmetadata\tproducer_csv\n{tag}\t3\t{tag}.run.txt\t{tag}_metrics.csv\n')
            subprocess.run([sys.executable, str(ROOT / 'tools/finalize_experiment.py'),
                            '--directory', temp, '--tag', tag, '--index', str(index), '--status', '20'],
                           check=True, capture_output=True)
            csv, = directory.glob('*_metrics.csv')
            self.assertNotIn('PENDING', csv.name)
            self.assertLess(len(csv.name.encode()), 255)
            metadata = csv.with_name(csv.name.replace('_metrics.csv', '.run.txt'))
            self.assertIn('FINAL_EXIT_CODE=20', metadata.read_text())
            self.assertNotIn('PENDING', index.read_text())


if __name__ == '__main__':
    unittest.main()
