import importlib.util
import argparse
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('lab', ROOT / 'tool/performance/lab.py')
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)


def frames(ui_ms, raster_ms=2.0, period=16667, start=1_000_000):
    return [{'v': start + i * period, 'b': start + i * period + 100, 'ui': int(u * 1000), 'uit': int(u * 1000),
             'rs': start + i * period + 100 + int(u * 1000), 'r': int(raster_ms * 1000), 'total': 0,
             'ph': {'layout': int(u * 600), 'semantics': int(u * 200)}} for i, u in enumerate(ui_ms)]


def record(scenario='steady-scroll', ui=(2, 3, 4), **extra):
    return {'theme': 'brutal-light', 'scenario': scenario, 'kind': 'continuous', 'displayRefreshRate': 60.0,
            'viewWidth': 1000, 'viewHeight': 700, 'frames': frames(ui), **extra}


class SummaryTest(unittest.TestCase):
    def test_budgets_and_gate(self):
        row = lab.summarize_record(record(ui=[2, 9, 20]))
        self.assertEqual(row['over8Ui'], 2)
        self.assertEqual(row['over16Ui'], 1)
        self.assertFalse(row['gate60'])
        self.assertEqual(row['semanticsShare'], round(100 * (400 + 1800 + 4000) / 31000, 1))
        self.assertTrue(lab.summarize_record(record(ui=[2, 9, 12]))['gate60'])

    def test_stutter_counts_skipped_vsyncs_only_for_continuous(self):
        r = record(ui=[2, 2, 2])
        r['frames'][2]['v'] += 20000  # one doubled frame interval
        self.assertEqual(lab.summarize_record(r)['stutters'], 1)
        r['kind'] = 'transitions'
        self.assertEqual(lab.summarize_record(r)['stutters'], 0)

    def test_mount_steps_cost_per_row(self):
        r = record('mount-plain', ui=[10, 1, 20, 2], kind='mount', rowKind='plain')
        f = r['frames']
        r['steps'] = [{'t0': f[0]['b'] - 10, 't1': f[2]['b'] - 10, 'newRows': 5},
                      {'t0': f[2]['b'] - 10, 't1': f[3]['b'] + 10, 'newRows': 4}]
        m = lab.summarize_record(r)['mount']
        self.assertEqual(m['rowsMounted'], 9)
        self.assertEqual(sorted([2000, 5000]), [lab.pct([2000, 5000], .5), m['perRowUs']['max']])

    def test_compare_flags_regressions(self):
        base = {'hello': {'fixtureVersion': 'v1'}, 'env': {}, 'scenarios': [lab.summarize_record(record(ui=[2] * 50))]}
        worse = {'hello': {'fixtureVersion': 'v1'}, 'env': {}, 'scenarios': [lab.summarize_record(record(ui=[2] * 49 + [20]))]}
        result = lab.compare(base, worse)
        self.assertGreater(result['regressions'], 0)
        self.assertIn('60 Hz gate pass→FAIL', result['rows'][0]['regressions'])
        self.assertEqual(lab.compare(base, base)['regressions'], 0)
        other = dict(worse, hello={'fixtureVersion': 'v2'})
        self.assertIn('fixture versions differ; numbers are not comparable', lab.compare(base, other)['problems'])

    def test_live_navigation_work_is_preserved_in_summary(self):
        r = record('nav-computer-detail', kind='transitions')
        r['opens'] = [
            {'channel': name, 'visibleMs': 20, 't0': 1_000_000, 't1': 2_000_000,
             'liveActivity': {'events': [{'event': 'agent:created'}, {'event': 'agent:deleted'}],
                              'accepted': {'agentCreate': 1, 'agentDelete': 1}, 'workspaceNotifications': 3},
             'predictiveBackGestures': int(name == 'back'), 'imageResponsesStarted': 2,
             'cacheReads': 1, 'cacheWritesCompleted': 4,
             'requests': {'GET /agents': 2}, 'responses': {'GET /agents': 2}}
            for name in ('enter', 'back')]
        work = lab.summarize_record(r)['navigationActivity']
        self.assertEqual(work['transitions'], 2)
        self.assertEqual(work['events'], 4)
        self.assertEqual(work['accepted'], {'agentCreate': 2, 'agentDelete': 2})
        self.assertEqual(work['predictiveBackGestures'], 1)
        self.assertEqual(work['cacheWritesCompleted'], 8)
        self.assertEqual(work['requests'], {'GET /agents': 4})
        self.assertEqual(work['responses'], {'GET /agents': 4})
        self.assertNotIn('navigationActivity', lab.summarize_record(record()))

    def test_summarize_run_directory(self):
        with tempfile.TemporaryDirectory() as d:
            run = Path(d)
            (run / 'brutal-light-steady-scroll.json').write_text(json.dumps(record()))
            (run / 'brutal-light-steady-scroll.attribution.json').write_text(json.dumps({
                'gcCount': 3, 'cpuWorstUiFrames': {'samples': 10, 'categories': {'fontFallback': 8},
                                                   'appInclusive': [{'name': 'A.build', 'value': 5}], 'self': [], 'inclusive': []},
                'rasterWorstFrames': {'inclusiveUs': [{'name': 'ReactorGLES::React', 'value': 1500}]}}))
            summary = lab.summarize(run)
            self.assertEqual(summary['env']['renderer'], 'Impeller (OpenGLES)')
            self.assertTrue(summary['gate60Passed'])
            text = (run / 'summary.md').read_text()
            self.assertIn('fontFallback 80.0%', text)
            self.assertIn('| steady-scroll | brutal-light | 3 |', text)

    def test_keyboard_windows_ignore_the_pause(self):
        r = record('keyboard', ui=[2, 20, 9, 2, 3], kind='keyboard', keyboardCycles=1)
        for frame, builds, layouts, paints in zip(r['frames'], (5000, 900, 800, 1, 2), (9, 40, 30, 1, 1), (9, 20, 18, 1, 1)):
            frame['builds'] = builds
            frame['layouts'] = layouts
            frame['paints'] = paints
        r['frames'][1]['r'] = 18000
        r['frames'][2]['r'] = 9000
        f = r['frames']
        r['keyboard'] = [
            {'direction': 'open', 't0': f[1]['b'], 't1': f[2]['b'], 'cpuUs': 1200},
            {'direction': 'close', 't0': f[2]['b'], 't1': f[3]['b'], 'cpuUs': 800},
        ]
        row = lab.summarize_record(r)
        k = row['keyboard']
        self.assertEqual(k['frames'], 2)
        self.assertEqual(k['worstUiMs'], 20.0)
        self.assertEqual(k['worstRasterMs'], 18.0)
        self.assertEqual(k['over8Ui'], 2)
        self.assertEqual(k['over16Ui'], 1)
        self.assertEqual(k['over8Raster'], 2)
        self.assertEqual(k['over16Raster'], 1)
        self.assertEqual(k['builds'], {'sum': 1700, 'max': 900, 'mean': 850.0})
        self.assertEqual(k['layouts'], {'sum': 70, 'max': 40, 'mean': 35.0})
        self.assertEqual(k['paints'], {'sum': 38, 'max': 20, 'mean': 19.0})
        self.assertEqual(k['uiThreadCpuUs'], 2000)
        text = lab.markdown({'run': 'kb', 'env': {}, 'hello': {}, 'scenarios': [row], 'gate60Passed': False})
        self.assertIn('Soft keyboard', text)
        self.assertIn('850.0', text)
        self.assertIn('35.0', text)
        self.assertIn('19.0', text)
        self.assertIn('a reference, not a gate', text)
        self.assertNotIn('1700/900', text)

    def test_keyboard_thread_time_joins_animation_frames_only(self):
        r = record('keyboard', ui=[4, 5], kind='keyboard')
        f = r['frames']
        r['keyboard'] = [{'direction': 'open', 't0': f[0]['b'], 't1': f[1]['b'] + 1, 'cpuUs': 100}]
        attribution = {
            'threadTime': {
                'collected': True,
                'uiThreads': ['io.flutter.1.ui'],
                'rasterThreads': ['io.flutter.1.raster'],
                'uiEvents': 4, 'uiEventsWithTdur': 4, 'uiEventsWithTts': 4,
                'rasterEvents': 4, 'rasterEventsWithTdur': 0, 'rasterEventsWithTts': 0,
            },
            'frameThreadTime': [
                {'b': f[0]['b'], 'uiCpuUs': 1500, 'uiEvent': 'Animator::BeginFrame', 'rasterEvent': 'Rasterizer::Draw'},
                {'b': f[1]['b'], 'uiCpuUs': 2500, 'uiEvent': 'Animator::BeginFrame'},
            ],
        }
        row = lab.summarize_record(r, attribution)
        self.assertEqual(row['keyboard']['uiCpuUs']['p50'], 1500)
        self.assertNotIn('rasterCpuUs', row['keyboard'])
        text = lab.markdown({'run': 'kb', 'env': {}, 'hello': {}, 'scenarios': [row], 'gate60Passed': True})
        self.assertIn('raster thread has 4 events and no tdur', text)
        self.assertIn('UI median 1.5 ms CPU', text)

    def test_proc_scene_cpu_names_its_source(self):
        r = record('keyboard', ui=[4, 5], kind='keyboard')
        f = r['frames']
        r['keyboard'] = [{'direction': 'open', 't0': f[0]['b'], 't1': f[1]['b'] + 1}]
        attribution = {
            'procThreadCpu': {
                'source': 'proc-schedstat',
                'collected': True,
                'clkTck': 100,
                'ui': {'tid': 10, 'comm': 'lutter.linkperf', 'matchedBy': 'main-thread', 'ticks': 12, 'ms': 120.0},
                'raster': {'tid': 11, 'comm': '1.raster', 'matchedBy': 'name', 'ticks': 8, 'ms': 80.0},
            },
        }
        row = lab.summarize_record(r, attribution)
        text = lab.markdown({'run': 'kb', 'env': {}, 'hello': {}, 'scenarios': [row], 'gate60Passed': True})
        self.assertIn('scene CPU from /proc schedstat, not timeline tts', text)
        self.assertIn('UI 120.0 ms (main thread `lutter.linkperf`', text)
        self.assertIn('raster 80.0 ms (`1.raster`)', text)

    def test_keyboard_count_change_does_not_change_the_comparison(self):
        def scenario(builds):
            r = record('keyboard', ui=[20, 9], kind='keyboard', keyboardCycles=1)
            for frame, count in zip(r['frames'], builds):
                frame['builds'] = count
                frame['layouts'] = 10
                frame['paints'] = 4
            f = r['frames']
            r['keyboard'] = [{'direction': 'open', 't0': f[0]['b'] - 1, 't1': f[-1]['b'] + 10, 'cpuUs': 1500}]
            return lab.summarize_record(r)

        before = {'hello': {'fixtureVersion': 'v1'}, 'env': {}, 'scenarios': [scenario([900, 800])]}
        after = {'hello': {'fixtureVersion': 'v1'}, 'env': {}, 'scenarios': [scenario([3, 2])]}
        better = lab.compare(before, after)
        self.assertEqual(better['rows'][0]['regressions'], [])
        self.assertEqual(better['rows'][0]['improvements'], [])
        self.assertEqual(better['regressions'], 0)
        worse = lab.compare(after, before)
        self.assertEqual(worse['rows'][0]['regressions'], [])
        self.assertEqual(worse['rows'][0]['improvements'], [])
        self.assertEqual(worse['regressions'], 0)
        self.assertEqual(better['rows'][0]['metrics']['keyboard.builds'][0]['mean'], 850)
        self.assertEqual(better['rows'][0]['metrics']['keyboard.builds'][1]['mean'], 2.5)
        text = lab.compare_markdown(better, 'before', 'after')
        self.assertIn('850.0→2.5', text)
        self.assertIn('a reference, not a gate', text)
        self.assertNotIn('deterministic check', text)

    def test_window_overlap_rejects_a_short_share(self):
        aligned = lab.window_overlap(0, 1000, 10, 1010)
        self.assertTrue(aligned['ok'])
        self.assertGreaterEqual(aligned['old'], 0.95)
        self.assertGreaterEqual(aligned['new'], 0.95)
        drifted = lab.window_overlap(0, 4210, 4020, 8230)
        self.assertFalse(drifted['ok'])
        self.assertLess(drifted['old'], 0.95)

    def test_paired_ratio_interval(self):
        same = [(10, 10)] * 12
        ci = lab.paired_ratio_ci(same, samples=200)
        self.assertEqual(lab.ratio_verdict(ci, high_limit=1.03, low_limit=0.97), 'same')
        slower = [(10, 12)] * 12
        slow_ci = lab.paired_ratio_ci(slower, samples=200)
        self.assertEqual(lab.ratio_verdict(slow_ci), 'regression')
        self.assertEqual(lab.ratio_verdict({'low': 0.98, 'high': 1.08}), 'uncertain')
        self.assertEqual(lab.ratio_verdict({'low': 0.90, 'high': 1.02}), 'pass')
        self.assertEqual(lab.paired_ratio_ci([])['rounds'], 0)
        self.assertEqual(lab.ratio_verdict(lab.paired_ratio_ci([])), 'no-data')
        same_batches = [([10, 100], [10, 100])] * 8
        cluster = lab.cluster_ratio_ci(same_batches, samples=200)
        self.assertEqual(cluster['pairs'], 8)
        self.assertEqual(lab.ratio_verdict(cluster, high_limit=1.03, low_limit=0.97), 'same')
        self.assertEqual(lab.cluster_ratio_ci([])['pairs'], 0)


class AndroidRunSafetyTest(unittest.TestCase):
    def test_changed_official_app_fails_even_when_metric_comparison_passes(self):
        with tempfile.TemporaryDirectory(prefix='raft-lab-safety-') as directory:
            base = Path(directory)
            original = base / 'profile.apk'
            original.write_bytes(b'original test artifact')
            output = base / 'run'
            args = argparse.Namespace(out=output, device='owned-test-device',
                flutter_args=[f'--use-application-binary={original}'], themes='elegant-light',
                scenarios='steady-scroll', seconds=8, semantics='platform', no_trace=True,
                reference=base / 'reference', max_load=None)
            before = {'version': 4001, 'ceDataInode': '123'}
            after = {'version': 4001, 'ceDataInode': '789'}

            def harness(argv, **kwargs):
                # Simulate a concurrent build replacing the source path. The
                # runner must install its frozen artifact instead.
                original.write_bytes(b'replaced by a different build')
                binary_arg = next(arg for arg in argv if arg.startswith('--use-application-binary='))
                binary = Path(binary_arg.split('=', 1)[1])
                self.assertEqual(binary, (output / 'test-profile.apk').resolve())
                self.assertEqual(binary.read_bytes(), b'original test artifact')
                self.assertIn('--keep-app-running', argv)
                return argparse.Namespace(returncode=0)

            def summary(out):
                (out / 'summary.md').write_text('fake metrics')
                return {}

            with patch.object(lab, 'flutter_guard') as guard, \
                 patch.object(lab, 'protected_state', side_effect=[before, after]), \
                 patch.object(lab, 'environment', return_value={'loadavg': [0, 0, 0]}), \
                 patch.object(lab.os, 'getloadavg', return_value=(0, 0, 0), create=True), \
                 patch.object(lab, 'wait_for_display'), patch.object(lab, 'wait_for_quiet_host'), \
                 patch.object(lab.subprocess, 'run', side_effect=harness), \
                 patch.object(lab, 'summarize', side_effect=summary), \
                 patch.object(lab, 'compare_cmd', return_value=0), \
                 contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(lab.run(args), 1)
            self.assertEqual(guard.call_count, 2)
            env = json.loads((output / 'env.json').read_text())
            self.assertEqual(env['exitCode'], 1)
            self.assertIn('protectedAppError', env)


if __name__ == '__main__':
    unittest.main()
