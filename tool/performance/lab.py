#!/usr/bin/env python3
"""Raft perf lab: run the fixed profile-mode scenario suite, summarise every
frame against the 8.33 ms (120 Hz) target and the 16.7 ms (60 Hz) gate, and
compare two runs.

  python3 tool/performance/lab.py run --out .local/performance/lab/<name>
  python3 tool/performance/lab.py summarize <run-dir>
  python3 tool/performance/lab.py compare <base-dir> <candidate-dir>

See docs/performance-lab.md.
"""
import argparse
import datetime
import json
import math
import os
from pathlib import Path
import platform
import random
import re
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
APP = ROOT / 'apps/raft_flutter'
TARGET = 'integration_test/perf_lab_test.dart'
DRIVER = 'test_driver/perf_lab_driver.dart'
sys.path.insert(0, str(ROOT / 'tool'))
from android_app import SafetyError, flutter_guard, protected_state
BUDGET_120 = 1e6 / 120  # 8333 us
BUDGET_60 = 1e6 / 60    # 16667 us
SCENARIO_ORDER = ['steady-scroll', 'fling', 'image-scroll', 'older-drag', 'scrollbar-drag', 'scrollbar-sweep', 'arrival-scrolled-up', 'resize', 'keyboard', 'idle-final',
                  'open-cold', 'open-warm', 'channel-switch', 'thread-open', 'section-drag', 'section-back-drag', 'section-cycle']
ROW_KINDS = ['plain', 'markdown', 'code', 'image', 'attachments', 'reactions', 'thread-summary', 'task']


# ---------------------------------------------------------------- statistics

def pct(values, q):
    """Nearest-rank percentile; 0 for an empty list."""
    if not values:
        return 0
    s = sorted(values)
    return s[max(0, min(len(s) - 1, math.ceil(q * len(s)) - 1))]


def dist(values):
    return {'p50': pct(values, .5), 'p90': pct(values, .9), 'p99': pct(values, .99), 'max': max(values) if values else 0,
            'mean': sum(values) / len(values) if values else 0}


def ms(us):
    return round(us / 1000, 2)


def frames_in(record, t0, t1):
    return [f for f in record.get('frames', []) if t0 <= f['b'] < t1]


def _work(frames, key):
    """Per-frame count summary. None when any frame omitted it.

    `mean` is the reference figure. The sum and max move when the frame
    count moves, so compare does not treat them as a gate.
    """
    if not frames or any(key not in frame for frame in frames):
        return None
    values = [frame[key] for frame in frames]
    return {'sum': int(sum(values)), 'max': int(max(values)), 'mean': sum(values) / len(values)}


def _attach_thread_time(frames, attribution):
    """Copy per-frame timeline tdur onto frame rows. Returns the presence note."""
    if not attribution:
        return None
    by_b = {item['b']: item for item in attribution.get('frameThreadTime') or [] if 'b' in item}
    for frame in frames:
        item = by_b.get(frame.get('b'))
        if not item:
            continue
        for key in ('uiCpuUs', 'rasterCpuUs', 'uiEvent', 'rasterEvent'):
            if item.get(key) is not None:
                frame[key] = item[key]
    return attribution.get('threadTime')


def _present_dist(frames, key):
    values = [frame[key] for frame in frames if key in frame]
    if not values:
        return None
    return {'p50': pct(values, .5), 'p90': pct(values, .9), 'max': max(values), 'frames': len(values)}


def _thread_time_line(keyboard, thread_time):
    """One line: per-frame timeline CPU next to the wall-clock median."""
    def side(cpu_key, wall_key, label):
        cpu = keyboard.get(cpu_key)
        wall = keyboard.get(wall_key)
        cpu_s = '-' if not cpu else f"{ms(cpu['p50'])} ms"
        wall_s = '-' if wall is None else f'{ms(wall)} ms'
        return f'{label} median {cpu_s} CPU, {wall_s} wall'

    def coverage(prefix):
        events = thread_time.get(f'{prefix}Events')
        if events is None:
            return f'{prefix} tdur was not collected'
        with_tdur = thread_time.get(f'{prefix}EventsWithTdur', 0)
        names = thread_time.get(f'{prefix}Threads') or []
        if not names and events == 0:
            return f'no {prefix} thread in the timeline'
        if events and with_tdur == 0:
            tts = thread_time.get(f'{prefix}EventsWithTts', 0)
            if tts:
                return f'{prefix} thread has {events} events, no tdur, {tts} with tts'
            return f'{prefix} thread has {events} events and no tdur'
        return f'{prefix} events {with_tdur}/{events} with tdur'

    if thread_time.get('collected') is False:
        return 'timeline was not collected, so tdur is absent.'
    return f"{side('uiCpuUs', 'uiWallP50Us', 'UI')}; {side('rasterCpuUs', 'rasterWallP50Us', 'raster')}. {coverage('ui')}. {coverage('raster')}."


def _proc_cpu_line(proc):
    """Whole-scene kernel CPU. Names the source so it is not read as timeline tts."""
    if not proc or proc.get('collected') is False:
        if proc and proc.get('error'):
            return f"scene CPU from /proc schedstat was not collected ({proc['error']})."
        return None

    def side(key, label):
        item = proc.get(key) or {}
        if item.get('missing') or 'ms' not in item:
            return f'{label} missing'
        comm = item.get('comm') or '?'
        ms = item['ms']
        text = f'{ms:.1f}' if isinstance(ms, float) else str(ms)
        if item.get('matchedBy') == 'main-thread':
            return f'{label} {text} ms (main thread `{comm}`; UI runs on the platform thread, kernel name is not 1.ui)'
        return f'{label} {text} ms (`{comm}`)'

    window = proc.get('windowMs')
    span = '' if window is None else f' Window {window / 1000:.1f} s.'
    return f"scene CPU from /proc schedstat, not timeline tts: {side('ui', 'UI')}; {side('raster', 'raster')}.{span}"


def paired_ratio_ci(pairs, samples=4000, seed=1):
    """95% interval of the median new/old ratio, resampling rounds.

    `pairs` is a sequence of (old, new), one entry per alternating round.
    A round with a non-positive old value is dropped. Rounds that shared a
    process are not independent samples. Use `cluster_ratio_ci` for that
    count. `compare` does not call this.
    """
    ratios = [new / old for old, new in pairs if old > 0 and new >= 0]
    if not ratios:
        return {'rounds': 0, 'low': None, 'high': None, 'median': None}

    def median(xs):
        ordered = sorted(xs)
        n = len(ordered)
        mid = n // 2
        if n % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2

    rng = random.Random(seed)
    n = len(ratios)
    boots = sorted(median([ratios[rng.randrange(n)] for _ in range(n)]) for _ in range(samples))
    return {
        'rounds': n,
        'low': boots[int(0.025 * (samples - 1))],
        'high': boots[int(0.975 * (samples - 1))],
        'median': median(ratios),
    }


def cluster_ratio_ci(batch_pairs, samples=4000, seed=1):
    """95% interval of the median launch-batch ratio.

    Each item is `(old_batch, new_batch)`: the scene-CPU values from one old
    launch and one new launch. The pair ratio is mean(new) / mean(old).
    Bootstrap draws those launches whole, so rounds that shared a process
    stay together. The independent sample count is the number of launch
    pairs. `compare` does not call this.
    """
    ratios = []
    for old, new in batch_pairs:
        old_values = [value for value in old if value > 0]
        new_values = [value for value in new if value >= 0]
        if not old_values or not new_values:
            continue
        old_mean = sum(old_values) / len(old_values)
        new_mean = sum(new_values) / len(new_values)
        ratios.append(new_mean / old_mean)
    if not ratios:
        return {'pairs': 0, 'low': None, 'high': None, 'median': None}

    def median(xs):
        ordered = sorted(xs)
        n = len(ordered)
        mid = n // 2
        if n % 2:
            return ordered[mid]
        return (ordered[mid - 1] + ordered[mid]) / 2

    rng = random.Random(seed)
    n = len(ratios)
    boots = sorted(median([ratios[rng.randrange(n)] for _ in range(n)]) for _ in range(samples))
    return {
        'pairs': n,
        'low': boots[int(0.025 * (samples - 1))],
        'high': boots[int(0.975 * (samples - 1))],
        'median': median(ratios),
    }


def window_overlap(old_start, old_end, new_start, new_end, *, minimum=0.95):
    """How much of each window sits inside the other.

    `ok` is false when the overlap is under `minimum` of either duration.
    A pair that fails this is not one shared moment of load.
    """
    old_span = old_end - old_start
    new_span = new_end - new_start
    overlap = min(old_end, new_end) - max(old_start, new_start)
    if overlap < 0 or old_span <= 0 or new_span <= 0:
        return {'ok': False, 'old': 0.0, 'new': 0.0, 'overlap': max(overlap, 0)}
    return {
        'ok': overlap / old_span >= minimum and overlap / new_span >= minimum,
        'old': overlap / old_span,
        'new': overlap / new_span,
        'overlap': overlap,
    }


def ratio_verdict(ci, *, high_limit=1.05, low_limit=None):
    """Whole interval within the limit passes. Entirely past it is a regression."""
    low, high = ci.get('low'), ci.get('high')
    if low is None or high is None:
        return 'no-data'
    if low_limit is not None:
        return 'same' if low >= low_limit and high <= high_limit else 'not-same'
    if high <= high_limit:
        return 'pass'
    if low > high_limit:
        return 'regression'
    return 'uncertain'


def stutters(record):
    """Frames that arrived more than 1.5 vsync periods after the previous one
    during a continuously animated scenario (dropped / doubled frame)."""
    if record.get('kind') not in ('continuous',):
        return 0
    rate = record.get('displayRefreshRate') or 60
    period = 1e6 / rate
    v = [f['v'] for f in record.get('frames', [])]
    return sum(1 for a, b in zip(v, v[1:]) if b - a > 1.5 * period)


def summarize_record(record, attribution=None):
    frames = record.get('frames', [])
    thread_time = _attach_thread_time(frames, attribution)
    for f in frames:
        # UI thread time per frame: engine build span plus the semantics,
        # finalize-tree and post-frame work that runs after scene submission.
        f.setdefault('uit', f['ui'])
    ui = [f['uit'] for f in frames]
    raster = [f['r'] for f in frames]
    n = len(frames)
    over = lambda xs, b: sum(1 for x in xs if x > b)
    either8 = sum(1 for f in frames if f['uit'] > BUDGET_120 or f['r'] > BUDGET_120)
    phases = {}
    for f in frames:
        for k, v in (f.get('ph') or {}).items():
            phases[k] = phases.get(k, 0) + v
    ui_total = sum(ui) or 1
    row = {
        'theme': record['theme'], 'scenario': record['scenario'], 'kind': record.get('kind'),
        'frames': n, 'elapsedSeconds': record.get('elapsedSeconds'),
        'displayRefreshRate': record.get('displayRefreshRate'),
        'view': f"{record.get('viewWidth', 0):.0f}x{record.get('viewHeight', 0):.0f}",
        'uiUs': dist(ui), 'uiBuildUs': dist([f['ui'] for f in frames]), 'rasterUs': dist(raster),
        'over8Ui': over(ui, BUDGET_120), 'over8Raster': over(raster, BUDGET_120), 'over8Either': either8,
        'over8EitherPct': round(100 * either8 / n, 1) if n else 0,
        'over16Ui': over(ui, BUDGET_60), 'over16Raster': over(raster, BUDGET_60),
        'stutters': stutters(record),
        'phaseShare': {k: round(100 * v / ui_total, 1) for k, v in sorted(phases.items(), key=lambda kv: -kv[1])},
        'semanticsShare': round(100 * phases.get('semantics', 0) / ui_total, 1),
        'semanticsEnabled': record.get('semanticsEnabled'),
        'cpuSeconds': record.get('cpuSeconds'),
        'uiThreadCpuSeconds': record.get('uiThreadCpuSeconds'),
        'rssDeltaMb': round(((record.get('rssAfter') or 0) - (record.get('rssBefore') or 0)) / 2**20, 1),
        'clockMatched': record.get('clockMatched'),
        'failure': record.get('failure'),
    }
    row['gate60'] = n > 0 and row['over16Ui'] == 0 and row['over16Raster'] == 0 and row['stutters'] == 0 and not row['failure']
    worst = sorted(frames, key=lambda f: -f['uit'])[:3]
    row['worstUiFrames'] = [{'uiMs': ms(f['uit']), 'buildMs': ms(f['ui']), 'rasterMs': ms(f['r']), 'phasesMs': {k: ms(v) for k, v in sorted((f.get('ph') or {}).items(), key=lambda kv: -kv[1])[:4]}} for f in worst]
    blocks = record.get('blockTotalsUs') or {}
    row['topBlocksMs'] = {k: ms(v) for k, v in sorted(blocks.items(), key=lambda kv: -kv[1])[:10]}
    for key in ('idleSeconds', 'movedPx', 'anchorDriftPx', 'anchorLostFrames', 'olderPagesServed', 'rowsBefore', 'rowsAfter', 'imageReads', 'arrivals', 'resizeSteps',
                'touchMoves', 'acceptedSection', 'taskRows', 'visibleChatTags', 'fileRows', 'decodedFileThumbnails', 'retainedTaskCards', 'chatRows', 'taggedChatRows', 'sectionVisits'):
        if key in record:
            row[key] = record[key]
    if 'opens' in record:
        opens = []
        for o in record['opens']:
            fs = frames_in(record, o['t0'], o['t1'])
            opens.append({'channel': o['channel'], 'visibleMs': round(o['visibleMs'], 1),
                          'worstUiMs': ms(max((f['uit'] for f in fs), default=0)),
                          'worstRasterMs': ms(max((f['r'] for f in fs), default=0)),
                          'uiSumMs': ms(sum(f['uit'] for f in fs)), 'frames': len(fs)})
        row['opens'] = opens
        vis = [o['visibleMs'] for o in opens if o['visibleMs'] >= 0]
        row['visibleMs'] = {'p50': pct(vis, .5), 'max': max(vis) if vis else -1}
        live = [o for o in record['opens'] if 'liveActivity' in o]
        if live:
            accepted, requests, responses = {}, {}, {}
            for o in live:
                for target, source in ((accepted, o['liveActivity'].get('accepted', {})),
                                       (requests, o.get('requests', {})),
                                       (responses, o.get('responses', {}))):
                    for key, count in source.items():
                        target[key] = target.get(key, 0) + count
            row['navigationActivity'] = {
                'transitions': len(live),
                'events': sum(len(o['liveActivity'].get('events', [])) for o in live),
                'accepted': accepted,
                'workspaceNotifications': sum(o['liveActivity'].get('workspaceNotifications', 0) for o in live),
                'predictiveBackGestures': sum(o.get('predictiveBackGestures', 0) for o in live),
                'imageResponsesStarted': sum(o.get('imageResponsesStarted', 0) for o in live),
                'cacheReads': sum(o.get('cacheReads', 0) for o in live),
                'cacheWritesCompleted': sum(o.get('cacheWritesCompleted', 0) for o in live),
                'requests': requests,
                'responses': responses,
            }
    if 'steps' in record:
        per_row, per_row_cpu, mount_ui, follow, rows = [], [], [], [], []
        for s in record['steps']:
            fs = frames_in(record, s['t0'], s['t1'])
            if not fs:
                continue
            mount_ui.append(fs[0]['uit'])
            follow.append(sum(f['uit'] for f in fs[1:]))
            rows.append(s['newRows'])
            if s['newRows'] > 0:
                per_row.append(fs[0]['uit'] / s['newRows'])
                if 'cpuUs' in s:
                    per_row_cpu.append(s['cpuUs'] / s['newRows'])
        row['mount'] = {'rowKind': record.get('rowKind'), 'steps': len(mount_ui), 'rowsMounted': sum(rows),
                        'perRowUs': dist(per_row), 'perRowCpuUs': dist(per_row_cpu), 'mountFrameUs': dist(mount_ui), 'followUpUs': dist(follow),
                        'coldFirstStepUs': mount_ui[0] if mount_ui else 0}
    if 'keyboard' in record:
        anim, episodes, cpu, cpu_known = [], [], 0, bool(record['keyboard'])
        for episode in record['keyboard']:
            fs = frames_in(record, episode['t0'], episode['t1'])
            anim.extend(fs)
            if 'cpuUs' not in episode:
                cpu_known = False
            else:
                cpu += episode['cpuUs']
            item = {'direction': episode.get('direction'), 'frames': len(fs),
                    'worstUiMs': ms(max((f['uit'] for f in fs), default=0)),
                    'worstRasterMs': ms(max((f['r'] for f in fs), default=0))}
            if 'cpuUs' in episode:
                item['cpuUs'] = episode['cpuUs']
            episodes.append(item)
        ui_anim = [f['uit'] for f in anim]
        raster_anim = [f['r'] for f in anim]
        keyboard = {
            'cycles': record.get('keyboardCycles'), 'episodes': episodes, 'frames': len(anim),
            'worstUiMs': ms(max(ui_anim, default=0)), 'worstRasterMs': ms(max(raster_anim, default=0)),
            'over8Ui': over(ui_anim, BUDGET_120), 'over8Raster': over(raster_anim, BUDGET_120),
            'over16Ui': over(ui_anim, BUDGET_60), 'over16Raster': over(raster_anim, BUDGET_60),
        }
        for key in ('builds', 'layouts', 'paints'):
            counted = _work(anim, key)
            if counted is not None:
                keyboard[key] = counted
        if cpu_known:
            keyboard['uiThreadCpuUs'] = cpu
        for key in ('uiCpuUs', 'rasterCpuUs'):
            counted = _present_dist(anim, key)
            if counted is not None:
                keyboard[key] = counted
        if ui_anim:
            keyboard['uiWallP50Us'] = pct(ui_anim, .5)
        if raster_anim:
            keyboard['rasterWallP50Us'] = pct(raster_anim, .5)
        row['keyboard'] = keyboard
    if thread_time:
        row['threadTime'] = thread_time
    if attribution and attribution.get('procThreadCpu'):
        row['procThreadCpu'] = attribution['procThreadCpu']
    if attribution:
        worst_cpu = attribution.get('cpuWorstUiFrames') or {}
        total = worst_cpu.get('samples') or 0
        row['gcCount'] = attribution.get('gcCount')
        gc = attribution.get('gcEvents') or {}
        row['gcTimeMs'] = ms(sum(v.get('totalUs', 0) for v in gc.values()))
        row['gcInWorstUiFrames'] = len(attribution.get('gcInWorstUiFrames') or [])
        row['worstUiAttribution'] = {
            'samples': total,
            'categories': {k: round(100 * v / total, 1) for k, v in sorted((worst_cpu.get('categories') or {}).items(), key=lambda kv: -kv[1])} if total else {},
            'app': [{'name': e['name'], 'pct': round(100 * e['value'] / total, 1)} for e in (worst_cpu.get('appInclusive') or [])[:8]] if total else [],
            'all': [{'name': e['name'], 'pct': round(100 * e['value'] / total, 1)} for e in (worst_cpu.get('inclusive') or [])[:25]] if total else [],
            'self': [{'name': e['name'], 'pct': round(100 * e['value'] / total, 1)} for e in (worst_cpu.get('self') or [])[:8]] if total else [],
        }
        raster = (attribution.get('rasterWorstFrames') or {}).get('inclusiveUs') or []
        row['worstRasterAttribution'] = [{'name': e['name'], 'ms': ms(e['value'])} for e in raster[:8]]
    return row


def load_run(run):
    run = Path(run)
    records = []
    for path in sorted(run.glob('*.json')):
        if path.name.endswith('.attribution.json') or path.name.startswith(('lab-', 'env', 'summary', 'compare', 'driver-')):
            continue
        data = json.loads(path.read_text())
        if isinstance(data, dict) and 'scenario' in data and 'frames' in data:
            att = run / f"{data['theme']}-{data['scenario']}.attribution.json"
            records.append((data, json.loads(att.read_text()) if att.is_file() else None))
    return records


def order_key(row):
    s = row['scenario']
    if s.startswith('mount-'):
        k = s[6:]
        return (1, ROW_KINDS.index(k) if k in ROW_KINDS else 99, row['theme'])
    return (0, SCENARIO_ORDER.index(s) if s in SCENARIO_ORDER else 99, row['theme'])


def summarize(run):
    run = Path(run)
    rows = sorted((summarize_record(r, a) for r, a in load_run(run)), key=order_key)
    env = json.loads((run / 'env.json').read_text()) if (run / 'env.json').is_file() else {}
    names = ' '.join(e['name'] for _, a in load_run(run) if a for e in (a.get('rasterWorstFrames') or {}).get('inclusiveUs', []))
    backend = next((label for key, label in (('GLES', 'Impeller (OpenGLES)'), ('Vulkan', 'Impeller (Vulkan)'), ('VK', 'Impeller (Vulkan)'), ('Metal', 'Impeller (Metal)'), ('MTL', 'Impeller (Metal)')) if key in names), None)
    if backend or names:
        env['renderer'] = backend or 'Skia (no Impeller raster events)'
    hello = json.loads((run / 'lab-hello.json').read_text()) if (run / 'lab-hello.json').is_file() else {}
    notes = [json.loads(l) for l in (run / 'lab-notes.jsonl').read_text().splitlines()] if (run / 'lab-notes.jsonl').is_file() else []
    summary = {'format': 'raft-perf-lab-v1', 'run': run.name, 'env': env, 'hello': hello, 'notes': notes, 'scenarios': rows,
               'gate60Passed': bool(rows) and all(r['gate60'] for r in rows)}
    (run / 'summary.json').write_text(json.dumps(summary, indent=1, ensure_ascii=False) + '\n')
    (run / 'summary.md').write_text(markdown(summary))
    return summary


def fmt_dist(d):
    return f"{ms(d['p50'])} / {ms(d['p90'])} / {ms(d['p99'])} / {ms(d['max'])}"


def fmt_cpu(r):
    """Process CPU of the scenario: seconds, share of one core, and per frame
    (the lab process: UI, raster, GC helpers and the VM-service sampler)."""
    cpu, elapsed = r.get('cpuSeconds'), r.get('elapsedSeconds')
    if cpu is None:
        return '-'
    share = f'{100 * cpu / elapsed:.0f}%' if elapsed else '-'
    per = f"{1000 * cpu / r['frames']:.1f}" if r.get('frames') else '-'
    ui = r.get('uiThreadCpuSeconds')
    ui_per = f", UI thread {1000 * ui / r['frames']:.2f} ms/frame" if ui is not None and r.get('frames') else ''
    return f'{cpu:.2f} / {share} / {per}{ui_per}'


def markdown(summary):
    env, hello = summary.get('env', {}), summary.get('hello', {})
    out = [f"# Perf lab run `{summary['run']}`", '']
    out.append(f"- Commit: `{env.get('commit', '?')}`{' (dirty)' if env.get('dirty') else ''}; fixture `{hello.get('fixtureVersion', '?')}`")
    out.append(f"- Engine: Flutter {env.get('flutter', {}).get('frameworkVersion', '?')} (engine `{str(env.get('flutter', {}).get('engineRevision', '?'))[:10]}`), renderer: {env.get('renderer', '?')}")
    out.append(f"- Host: {env.get('os', '?')}; CPU: {env.get('cpu', '?')}; GPU: {env.get('gpu', '?')}")
    load = lambda key: ', '.join(f'{v:.1f}' for v in env.get(key) or []) or '?'
    out.append(f"- Host load (1/5/15 min) at start: {load('loadavg')}; at end: {load('loadavgEnd')}" + (' — **NOISY HOST: other work was running; do not use these numbers as a baseline**' if env.get('noisyHost') else ''))
    first = summary['scenarios'][0] if summary['scenarios'] else {}
    out.append(f"- Display: {hello.get('displayRefreshRate', '?')} Hz, DPR {hello.get('devicePixelRatio', '?')}, view {first.get('view', '?')} physical px; semantics mode `{hello.get('semantics', '?')}` (embedder requested: {hello.get('platformSemanticsEnabled', '?')}); phase collection {'on' if hello.get('phases') else 'off'}")
    out.append(f"- Gate (60 Hz: no UI or raster frame over 16.7 ms, no stutter): **{'PASS' if summary['gate60Passed'] else 'FAIL'}**. Target: every frame within 8.33 ms (120 Hz).")
    out += ['', '## Frames (ms: p50 / p90 / p99 / max)', '',
            '| Scenario | Theme | Frames | UI thread (build+layout+paint+semantics) | Raster | >8.33 ms (UI/R, either %) | >16.7 ms (UI/R) | Stutter | GC | Semantics % | Process CPU (s, % of a core, ms/frame) | Gate 60 |',
            '|---|---|---|---|---|---|---|---|---|---|---|---|']
    for r in summary['scenarios']:
        out.append(f"| {r['scenario']} | {r['theme']} | {r['frames']} | {fmt_dist(r['uiUs'])} | {fmt_dist(r['rasterUs'])} | {r['over8Ui']}/{r['over8Raster']} ({r['over8EitherPct']}%) | {r['over16Ui']}/{r['over16Raster']} | {r['stutters']} | {r.get('gcCount', '-')} | {r['semanticsShare']} | {fmt_cpu(r)} | {'pass' if r['gate60'] else 'FAIL'} |")
    sections = [r for r in summary['scenarios'] if r['scenario'].startswith('section-')]
    if sections:
        out += ['', '## Populated mobile sections', '',
                '| Scenario | Theme | Chat rows | Visible task tags | Tasks / mounted cards | Files / decoded thumbnails | Accepted sections |',
                '|---|---|---|---|---|---|---|']
        for r in sections:
            visits = ' → '.join(r.get('sectionVisits', [])) or r.get('acceptedSection', '?')
            out.append(f"| {r['scenario']} | {r['theme']} | {r.get('chatRows', r.get('taggedChatRows', '?'))} | {r.get('visibleChatTags', '?')} | {r.get('taskRows', '?')} / {r.get('retainedTaskCards', '?')} | {r.get('fileRows', '?')} / {r.get('decodedFileThumbnails', '?')} | {visits} |")
    mounts = [r for r in summary['scenarios'] if 'mount' in r]
    if mounts:
        out += ['', '## New-row mount cost (UI µs of the frame that mounts rows, divided by rows mounted)', '',
                '| Row kind | Theme | Rows | Per row p50 / p90 / max | Per row UI-thread CPU p50 / p90 (us) | Mount frame p50 / max (ms) | Follow-up frames p50 (ms) | First (cold) step (ms) |', '|---|---|---|---|---|---|---|---|']
        for r in mounts:
            m = r['mount']
            out.append(f"| {m['rowKind']} | {r['theme']} | {m['rowsMounted']} | {round(m['perRowUs']['p50'])} / {round(m['perRowUs']['p90'])} / {round(m['perRowUs']['max'])} | {round(m['perRowCpuUs']['p50'])} / {round(m['perRowCpuUs']['p90'])} | {ms(m['mountFrameUs']['p50'])} / {ms(m['mountFrameUs']['max'])} | {ms(m['followUpUs']['p50'])} | {ms(m['coldFirstStepUs'])} |")
    trans = [r for r in summary['scenarios'] if 'opens' in r]
    if trans:
        out += ['', '## Transitions', '', '| Scenario | Theme | Opens | Visible ms (p50 / max) | Worst UI frame per open (ms) |', '|---|---|---|---|---|']
        for r in trans:
            out.append(f"| {r['scenario']} | {r['theme']} | {len(r['opens'])} | {r['visibleMs']['p50']} / {r['visibleMs']['max']} | {', '.join(str(o['worstUiMs']) for o in r['opens'])} |")
    keys = [r for r in summary['scenarios'] if 'keyboard' in r]
    if keys:
        out += ['', '## Soft keyboard', '',
                'The rows below count only the inset slides. The hold between an open and a close is not included. 8.33 ms and 16.7 ms are this lab\'s frame budgets. Build, layout and paint figures are the average count per frame, a reference, not a gate. Slide UI CPU is the UI thread\'s schedstat time over the slides. The lines under the table are per-frame timeline thread CPU (`tdur`) next to the wall-clock median. Scene CPU, when the next line is present, is `/proc` schedstat nanoseconds for the whole scenario, not `tts`.',
                '',
                '| Theme | Frames | Worst UI ms | Worst raster ms | >8.33 ms (UI/R) | >16.7 ms (UI/R) | Builds per frame | Layouts per frame | Paints per frame | Slide UI CPU ms |',
                '|---|---|---|---|---|---|---|---|---|---|']
        def work_pair(k, name):
            counted = k.get(name)
            if not counted:
                return '-'
            return f"{counted['mean']:.1f}"
        for r in keys:
            k = r['keyboard']
            cpu = k.get('uiThreadCpuUs')
            cpu_cell = '-' if cpu is None else f'{cpu / 1000:.1f}'
            out.append(f"| {r['theme']} | {k['frames']} | {k['worstUiMs']} | {k['worstRasterMs']} | {k['over8Ui']}/{k['over8Raster']} | {k['over16Ui']}/{k['over16Raster']} | {work_pair(k, 'builds')} | {work_pair(k, 'layouts')} | {work_pair(k, 'paints')} | {cpu_cell} |")
            out.append(f"- {r['theme']}: {_thread_time_line(k, r.get('threadTime') or {})}")
            proc_line = _proc_cpu_line(r.get('procThreadCpu'))
            if proc_line:
                out.append(f"- {r['theme']}: {proc_line}")
    out += ['', '## Worst-frame attribution', '']
    for r in summary['scenarios']:
        att = r.get('worstUiAttribution') or {}
        phases = ', '.join(f"{k} {v}%" for k, v in list(r['phaseShare'].items())[:5])
        out.append(f"- **{r['scenario']} / {r['theme']}**: worst UI {', '.join(str(f['uiMs']) for f in r['worstUiFrames'])} ms; phase share {phases or 'n/a'}")
        if att.get('categories'):
            out.append('  - worst-frame CPU by category (inclusive %): ' + ', '.join(f"{k} {v}%" for k, v in att['categories'].items()))
        if att.get('app'):
            out.append('  - app code (inclusive % of worst-frame samples): ' + '; '.join(f"{e['name']} {e['pct']}%" for e in att['app'][:5]))
        if att.get('self'):
            out.append('  - hottest leaf functions: ' + '; '.join(f"{e['name']} {e['pct']}%" for e in att['self'][:4]))
        if r.get('worstRasterAttribution'):
            out.append('  - raster: ' + '; '.join(f"{e['name']} {e['ms']}ms" for e in r['worstRasterAttribution'][:4]))
        extra = {k: r[k] for k in ('movedPx', 'anchorDriftPx', 'olderPagesServed', 'imageReads', 'gcTimeMs', 'gcInWorstUiFrames') if k in r}
        if extra:
            out.append('  - ' + ', '.join(f"{k} {v}" for k, v in extra.items()))
    if summary.get('notes'):
        out += ['', '## Notes', ''] + [f"- {json.dumps(n, ensure_ascii=False)}" for n in summary['notes']]
    return '\n'.join(out) + '\n'


# ---------------------------------------------------------------- compare

COMPARED = [('uiUs', 'p90'), ('uiUs', 'p99'), ('uiUs', 'max'), ('rasterUs', 'p90'), ('rasterUs', 'p99'), ('rasterUs', 'max')]


def compare(base, cand, threshold=0.10, floor_us=500):
    b = {(r['theme'], r['scenario']): r for r in base['scenarios']}
    c = {(r['theme'], r['scenario']): r for r in cand['scenarios']}
    problems, rows = [], []
    if base.get('hello', {}).get('fixtureVersion') != cand.get('hello', {}).get('fixtureVersion'):
        problems.append('fixture versions differ; numbers are not comparable')
    views = lambda s: sorted({r.get('view') for r in s['scenarios']})
    if views(base) != views(cand):
        problems.append(f'view sizes differ: {views(base)} vs {views(cand)} (layout cost scales with width)')
    for name, run in (('base', base), ('candidate', cand)):
        if run.get('env', {}).get('noisyHost'):
            problems.append(f'{name} run was taken on a noisy host (load above limit)')
    for key in ('cpu', 'gpu', 'renderer', 'os'):
        if base.get('env', {}).get(key) != cand.get('env', {}).get(key):
            problems.append(f'environment differs: {key}')
    if base.get('env', {}).get('flutter', {}).get('engineRevision') != cand.get('env', {}).get('flutter', {}).get('engineRevision'):
        problems.append('environment differs: Flutter engine')
    for key in sorted(set(b) & set(c), key=lambda k: order_key({'theme': k[0], 'scenario': k[1]})):
        x, y = b[key], c[key]
        row = {'theme': key[0], 'scenario': key[1], 'regressions': [], 'improvements': [], 'metrics': {}}
        for group, q in COMPARED:
            old, new = x[group][q], y[group][q]
            row['metrics'][f'{group}.{q}'] = (old, new)
            # Single-frame maxima are reported but too noisy to flag; frames
            # beyond the 60 Hz budget are flagged through over16 counts.
            if q == 'max':
                continue
            if new > old * (1 + threshold) and new - old > floor_us:
                row['regressions'].append(f'{group[:-2]} {q} {ms(old)}→{ms(new)} ms')
            elif new < old * (1 - threshold) and old - new > floor_us:
                row['improvements'].append(f'{group[:-2]} {q} {ms(old)}→{ms(new)} ms')
        for metric in ('over16Ui', 'over16Raster', 'stutters'):
            row['metrics'][metric] = (x[metric], y[metric])
            if y[metric] > x[metric]:
                row['regressions'].append(f'{metric} {x[metric]}→{y[metric]}')
            elif y[metric] < x[metric]:
                row['improvements'].append(f'{metric} {x[metric]}→{y[metric]}')
        row['metrics']['over8EitherPct'] = (x['over8EitherPct'], y['over8EitherPct'])
        if y['over8EitherPct'] > x['over8EitherPct'] + 2:
            row['regressions'].append(f"frames over 8.33 ms {x['over8EitherPct']}%→{y['over8EitherPct']}%")
        elif y['over8EitherPct'] < x['over8EitherPct'] - 2:
            row['improvements'].append(f"frames over 8.33 ms {x['over8EitherPct']}%→{y['over8EitherPct']}%")
        if x['gate60'] and not y['gate60']:
            row['regressions'].append('60 Hz gate pass→FAIL')
        if 'mount' in x and 'mount' in y:
            old, new = x['mount']['perRowUs']['p50'], y['mount']['perRowUs']['p50']
            row['metrics']['mount.perRow.p50'] = (old, new)
            if new > old * (1 + threshold) and new - old > 100:
                row['regressions'].append(f'per-row mount p50 {round(old)}→{round(new)} µs')
            elif new < old * (1 - threshold) and old - new > 100:
                row['improvements'].append(f'per-row mount p50 {round(old)}→{round(new)} µs')
        if 'visibleMs' in x and 'visibleMs' in y:
            row['metrics']['visibleMs.p50'] = (x['visibleMs']['p50'], y['visibleMs']['p50'])
        if 'keyboard' in x and 'keyboard' in y:
            kx, ky = x['keyboard'], y['keyboard']
            for field in ('frames', 'worstUiMs', 'worstRasterMs', 'over8Ui', 'over8Raster', 'over16Ui', 'over16Raster', 'uiThreadCpuUs'):
                row['metrics'][f'keyboard.{field}'] = (kx.get(field), ky.get(field))
            for count in ('builds', 'layouts', 'paints'):
                old, new = kx.get(count) or {}, ky.get(count) or {}
                # Native counts move with the frame count. The per-frame mean
                # stays in the report as a reference and does not change the
                # comparison.
                row['metrics'][f'keyboard.{count}'] = ({'mean': old.get('mean')}, {'mean': new.get('mean')})
        rows.append(row)
    missing = sorted(set(b) ^ set(c))
    return {'problems': problems, 'missing': [list(k) for k in missing], 'rows': rows,
            'regressions': sum(len(r['regressions']) for r in rows)}


def compare_markdown(result, base_name, cand_name):
    out = [f'# Perf lab compare: `{base_name}` → `{cand_name}`', '']
    for p in result['problems']:
        out.append(f'- WARNING: {p}')
    if result['missing']:
        out.append(f"- Scenarios present in only one run: {', '.join('/'.join(k) for k in result['missing'])}")
    out += ['', '| Scenario | Theme | UI p99 (ms) | Raster p99 (ms) | >8.33 % | >16.7 UI/R | Regressions | Improvements |', '|---|---|---|---|---|---|---|---|']
    for r in result['rows']:
        m = r['metrics']
        out.append(f"| {r['scenario']} | {r['theme']} | {ms(m['uiUs.p99'][0])}→{ms(m['uiUs.p99'][1])} | {ms(m['rasterUs.p99'][0])}→{ms(m['rasterUs.p99'][1])} | {m['over8EitherPct'][0]}→{m['over8EitherPct'][1]} | {m['over16Ui'][0]}/{m['over16Raster'][0]}→{m['over16Ui'][1]}/{m['over16Raster'][1]} | {'**' + '; '.join(r['regressions']) + '**' if r['regressions'] else ''} | {'; '.join(r['improvements'])} |")
    keyboard_rows = [r for r in result['rows'] if 'keyboard.worstUiMs' in r['metrics']]
    if keyboard_rows:
        out += ['', '## Soft keyboard (animation frames only)', '',
                'Worst milliseconds and the 8.33 / 16.7 ms counts are the slide frames. UI-thread CPU is on-CPU time during those slides. Build, layout and paint figures are the average count per frame, a reference, not a gate.',
                '',
                '| Scenario | Theme | Worst UI ms | Worst raster ms | >8.33 UI/R | >16.7 UI/R | Builds per frame | Layouts per frame | Paints per frame | UI-thread CPU ms |',
                '|---|---|---|---|---|---|---|---|---|---|']
        for r in keyboard_rows:
            m = r['metrics']
            def side(field, index, scale=1):
                value = m.get(f'keyboard.{field}', (None, None))[index]
                if value is None:
                    return '-'
                return f'{value / scale:.1f}' if scale != 1 else str(value)
            def duo(ui_field, raster_field):
                def one(index):
                    ui = m.get(f'keyboard.{ui_field}', (None, None))[index]
                    raster = m.get(f'keyboard.{raster_field}', (None, None))[index]
                    if ui is None or raster is None:
                        return '-'
                    return f'{ui}/{raster}'
                return f'{one(0)}→{one(1)}'
            def means(name):
                def one(index):
                    counted = m.get(f'keyboard.{name}', ({}, {}))[index] or {}
                    value = counted.get('mean')
                    return '-' if value is None else f'{value:.1f}'
                return f'{one(0)}→{one(1)}'
            out.append(
                f"| {r['scenario']} | {r['theme']} | {side('worstUiMs', 0)}→{side('worstUiMs', 1)} | {side('worstRasterMs', 0)}→{side('worstRasterMs', 1)} | {duo('over8Ui', 'over8Raster')} | {duo('over16Ui', 'over16Raster')} | {means('builds')} | {means('layouts')} | {means('paints')} | {side('uiThreadCpuUs', 0, 1000)}→{side('uiThreadCpuUs', 1, 1000)} |")
    out += ['', f"Total regressions: {result['regressions']}"]
    return '\n'.join(out) + '\n'


# ---------------------------------------------------------------- run

def capture(args, **kw):
    try:
        return subprocess.run(args, capture_output=True, text=True, timeout=60, **kw).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return ''


def environment(device):
    env = {'os': f'{platform.system()} {platform.release()}', 'machine': platform.machine(), 'device': device,
           'commit': capture(['git', 'rev-parse', 'HEAD'], cwd=ROOT),
           'dirty': bool(capture(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=ROOT)),
           'startedAt': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'loadavg': os.getloadavg()}
    try:
        env['flutter'] = json.loads(capture([str(ROOT / 'tool/flutter'), '--version', '--machine']))
    except ValueError:
        env['flutter'] = {}
    if platform.system() == 'Linux':
        model = re.search(r'model name\s*:\s*(.+)', Path('/proc/cpuinfo').read_text())
        env['cpu'] = f"{model.group(1) if model else '?'} x{os.cpu_count()}"
        gov = Path('/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor')
        env['cpuGovernor'] = gov.read_text().strip() if gov.is_file() else None
        glx = capture(['glxinfo', '-B'])
        renderer = re.search(r'OpenGL renderer string:\s*(.+)', glx)
        env['gpu'] = renderer.group(1) if renderer else '?'
        env['session'] = os.environ.get('XDG_SESSION_TYPE')
    elif platform.system() == 'Darwin':
        env['cpu'] = f"{capture(['sysctl', '-n', 'machdep.cpu.brand_string'])} x{os.cpu_count()}"
        displays = capture(['system_profiler', 'SPDisplaysDataType'])
        chip = re.search(r'Chipset Model:\s*(.+)', displays)
        env['gpu'] = chip.group(1) if chip else '?'
        env['displays'] = re.findall(r'Resolution:\s*(.+)|UI Looks like:\s*(.+)', displays)
    return env


def other_instance():
    if platform.system() != 'Linux':
        return []
    lines = capture(['ps', '-eo', 'pid,args']).splitlines()
    return [l.strip() for l in lines if 'bundle/raft_flutter' in l]


def wait_for_display(args):
    # A simultaneous old/new pair is the measurement. The host pins each
    # process with taskset and sets this; waiting for the other to exit
    # would put them back in sequence.
    if os.environ.get('RAFT_LAB_ALLOW_CONCURRENT') == '1':
        return
    waited = 0
    while other_instance():
        if not args.wait:
            sys.exit('Another raft_flutter bundle is running on this display:\n' + '\n'.join(other_instance()) + '\nRe-run with --wait to poll.')
        time.sleep(15)
        waited += 15
        if waited > args.wait:
            sys.exit('Timed out waiting for the other raft_flutter instance to exit')


def max_load(args):
    return args.max_load if args.max_load is not None else (os.cpu_count() or 4) * 0.35


def wait_for_quiet_host(args):
    """Frame times scale with host contention (other builds/test suites on
    the same machine). Wait for the 1-minute load to drop below the limit."""
    waited = 0
    while os.getloadavg()[0] > max_load(args):
        if waited >= args.wait_quiet:
            if args.allow_noisy:
                print(f'WARNING: host load {os.getloadavg()[0]:.1f} exceeds {max_load(args):.1f}; results will be marked noisy', flush=True)
                return
            sys.exit(f'Host load {os.getloadavg()[0]:.1f} exceeds {max_load(args):.1f}; wait (--wait-quiet) or pass --allow-noisy')
        time.sleep(15)
        waited += 15


def run(args):
    out = args.out.resolve()
    if out.exists():
        sys.exit(f'{out} exists; use a new output directory so earlier runs stay unchanged')
    device = args.device or ('macos' if platform.system() == 'Darwin' else 'linux')
    argv = [str(ROOT / 'tool/flutter'), 'drive', '--profile', f'--driver={DRIVER}', f'--target={TARGET}', '-d', device, '--no-pub',
            *args.flutter_args]
    android = device not in ('macos', 'linux', 'windows', 'chrome', 'edge', 'web-server')
    if android:
        argv.append('--keep-app-running')
    try:
        flutter_guard(argv[1:], cwd=APP)
        protected_before = protected_state(device) if android else None
    except SafetyError as error:
        sys.exit(f'STOP: {error}')
    if device == 'linux' and not os.environ.get('DISPLAY'):
        sys.exit('DISPLAY must name the real X11 display (see docs/performance-lab.md)')
    wait_for_display(args)
    wait_for_quiet_host(args)
    out.mkdir(parents=True)
    if android:
        # Freeze the verified test artifact; a concurrent Gradle build must not
        # replace the path that Flutter will later install.
        for index, argument in enumerate(argv):
            if argument.startswith('--use-application-binary='):
                source = argument.split('=', 1)[1]
                value_index = index
                break
            if argument == '--use-application-binary':
                source = argv[index + 1]
                value_index = index + 1
                break
        binary = out / 'test-profile.apk'
        shutil.copyfile((APP / source).resolve(), binary)
        binary.chmod(0o444)
        argv[value_index] = f'--use-application-binary={binary}' if argv[value_index].startswith('--use-application-binary=') else str(binary)
        try:
            flutter_guard(argv[1:], cwd=APP)
        except SafetyError as error:
            sys.exit(f'STOP: staged test APK verification failed: {error}')
    env = environment(device)
    if android:
        env['protectedAppBefore'] = protected_before
    (out / 'env.json').write_text(json.dumps(env, indent=1) + '\n')
    defines = {'RAFT_LAB_THEMES': args.themes, 'RAFT_LAB_SCENARIOS': args.scenarios or '', 'RAFT_LAB_SECONDS': str(args.seconds),
               'RAFT_LAB_SEMANTICS': args.semantics, 'RAFT_LAB_PHASES': str(not args.no_trace).lower()}
    rounds = os.environ.get('RAFT_LAB_ROUNDS')
    if rounds:
        defines['RAFT_LAB_ROUNDS'] = rounds
    child = dict(os.environ, RAFT_PERF_OUT=str(out), RAFT_LAB_ROOT=str(ROOT), RAFT_LAB_DEVICE=device, **defines)
    if device == 'linux':
        child['GDK_BACKEND'] = os.environ.get('RAFT_LAB_GDK_BACKEND', 'x11')  # 'wayland' with WAYLAND_DISPLAY measures native Wayland
    argv += [f'--dart-define={k}={v}' for k, v in defines.items()]
    print('Running:', ' '.join(argv), flush=True)
    for attempt in range(1, 4):
        wait_for_display(args)
        with (out / 'runner.log').open('w') as log:
            status = subprocess.run(argv, cwd=APP, env=child, stdout=log, stderr=subprocess.STDOUT).returncode
        text = (out / 'runner.log').read_text(errors='replace')
        # The desktop app is single-instance: a launch racing another
        # worktree's run exits at once. Keep the failed log and retry.
        if 'Application failed to start' not in text or (out / 'lab-hello.json').exists():
            break
        (out / 'runner.log').rename(out / f'runner-failed-start-{attempt}.log')
        time.sleep(20)
    impeller = re.search(r'Using the Impeller rendering backend \(([^)]+)\)', text)
    if android:
        try:
            env['protectedAppAfter'] = protected_state(device)
            if env['protectedAppAfter'] != protected_before:
                raise SafetyError('Official App identity changed during the benchmark.')
        except SafetyError as error:
            status = 1
            env['protectedAppError'] = str(error)
            print(f'STOP: {error} No reinstall/restore attempt will be made.', flush=True)
    env['renderer'] = f'Impeller ({impeller.group(1)})' if impeller else 'unknown (derived from raster events in summarize)'
    env['exitCode'] = status
    env['endedAt'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    env['loadavgEnd'] = os.getloadavg()
    env['maxLoad'] = max_load(args)
    env['noisyHost'] = max(env['loadavg'][0], env['loadavgEnd'][0]) > env['maxLoad']
    (out / 'env.json').write_text(json.dumps(env, indent=1) + '\n')
    summary = summarize(out)
    print((out / 'summary.md').read_text())
    if args.reference:
        comparison_status = compare_cmd(argparse.Namespace(base=args.reference, candidate=out, out=None, no_fail=False))
        return 1 if status else comparison_status
    return 0 if status == 0 else 1


def compare_cmd(args):
    base, cand = Path(args.base), Path(args.candidate)
    sb = summarize(base) if not (base / 'summary.json').is_file() else json.loads((base / 'summary.json').read_text())
    sc = summarize(cand) if not (cand / 'summary.json').is_file() else json.loads((cand / 'summary.json').read_text())
    result = compare(sb, sc)
    text = compare_markdown(result, base.name, cand.name)
    target = Path(args.out) if args.out else cand / f'compare-{base.name}.md'
    target.write_text(text)
    target.with_suffix('.json').write_text(json.dumps(result, indent=1, ensure_ascii=False) + '\n')
    print(text)
    return 1 if result['regressions'] and not args.no_fail else 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    r = sub.add_parser('run', help='run the suite in profile mode')
    r.add_argument('--out', type=Path, required=True)
    r.add_argument('--themes', default='brutal-light,elegant-light', help='comma list: brutal-light, brutal-dark, elegant-light, elegant-dark')
    r.add_argument('--scenarios', help='comma list; "mount-*" selects every row kind')
    r.add_argument('--seconds', type=int, default=8, help='duration of continuous scenarios')
    r.add_argument('--semantics', choices=['platform', 'on', 'off'], default='platform',
                   help='platform: as the embedder requests (Linux GTK: always on); on: forced (VoiceOver); off: forced off (usual macOS)')
    r.add_argument('--no-trace', action='store_true', help='disable framework phase collection and engine timeline (cleanest timings, no phase/raster attribution)')
    r.add_argument('--device', help='flutter device id (default: linux or macos)')
    r.add_argument('--reference', type=Path, help='compare against this earlier run directory')
    r.add_argument('--max-load', type=float, help='1-minute load average above which the host counts as noisy (default 0.35 x CPUs)')
    r.add_argument('--wait-quiet', type=int, default=1800, help='seconds to wait for a quiet host before giving up')
    r.add_argument('--allow-noisy', action='store_true', help='run anyway on a loaded host (summary is marked noisy)')
    r.add_argument('--wait', type=int, default=0, help='seconds to wait for another raft_flutter instance to exit (Linux)')
    r.add_argument('flutter_args', nargs='*', help='extra flutter drive arguments after --, e.g. -- --no-enable-impeller')
    s = sub.add_parser('summarize', help='rebuild summary.json/summary.md for a run')
    s.add_argument('run', type=Path)
    c = sub.add_parser('compare', help='before/after comparison against the budgets')
    c.add_argument('base', type=Path)
    c.add_argument('candidate', type=Path)
    c.add_argument('--out', type=Path)
    c.add_argument('--no-fail', action='store_true')
    args = parser.parse_args(argv)
    if args.command == 'run':
        return run(args)
    if args.command == 'summarize':
        summarize(args.run)
        print((args.run / 'summary.md').read_text())
        return 0
    return compare_cmd(args)


if __name__ == '__main__':
    raise SystemExit(main())
