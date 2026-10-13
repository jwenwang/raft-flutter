// Debug/profile-only probe: renders one mixed Latin + Chinese string at the
// theme body style for weights 400/500/600/700, sizes 14/20 and the theme
// families (brutal, elegant, system fonts), with the zh fallback list. Real
// renderer, no flutter_test. Two grids are captured with
// RenderRepaintBoundary.toImage: the mixed string (sheet.png) and a Chinese-only
// string (cjk.png) for ink measurement.
//
// Desktop (Linux / macOS):
//   tool/flutter run -d <linux|macos> --profile \
//     -t integration_test/cjk_weight_probe_main.dart
//   (from apps/raft_flutter). The probe serves on http://127.0.0.1:8799:
//   /sheet.png /cjk.png /info.json. With env CJK_OUT=<dir> it also writes
//   them there (not usable in the macOS sandbox) and with CJK_EXIT=1 exits.
//   CJK_ONLY=<weight> renders a single weight (font-file attribution via
//   /proc/self/maps on Linux).
// Android: build with --android-project-arg=raftApplicationIdSuffix=.linkperf,
//   adb forward tcp:8799 tcp:8799, then curl the same URLs.
import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:typed_data';
import 'dart:ui' as ui;

import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:flutter/rendering.dart';
import 'package:raft_flutter/platform/system_fonts.dart';
import 'package:raft_ui/raft_ui.dart';

const mixed = 'Raft 团队协作 消息 设置 Agent 工作空间 1234';
const cjkOnly = '团队协作 消息 设置 工作空间';
const sizes = [14.0, 20.0];
const allWeights = [400, 500, 600, 700];
const cellW = 540.0;
const cellH = 52.0;
const labelW = 96.0;
const headerH = 44.0;
const ratio = 2.0;

final columns = <({String name, RaftFamily family, bool system, double size})>[
  for (final c in const [
    ('brutal', RaftFamily.brutal, false),
    ('elegant', RaftFamily.elegant, false),
    ('system', RaftFamily.elegant, true),
  ])
    for (final s in sizes)
      (name: '${c.$1} ${s.toInt()}px', family: c.$2, system: c.$3, size: s),
];

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await NativeSystemFonts.instance.initialize();
  final only = int.tryParse(Platform.environment['CJK_ONLY'] ?? '');
  final weights = only == null ? allWeights : [only];
  final mixedKey = GlobalKey();
  final cjkKey = GlobalKey();
  runApp(
    MaterialApp(
      debugShowCheckedModeBanner: false,
      locale: const Locale.fromSubtags(languageCode: 'zh', scriptCode: 'Hans'),
      home: Material(
        color: Colors.white,
        child: SingleChildScrollView(
          scrollDirection: Axis.horizontal,
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              _Grid(key: mixedKey, text: mixed, weights: weights),
              _Grid(key: cjkKey, text: cjkOnly, weights: weights),
            ],
          ),
        ),
      ),
    ),
  );
  await Future<void>.delayed(const Duration(seconds: 2));
  await WidgetsBinding.instance.endOfFrame;
  final sheet = await _png(mixedKey);
  final cjk = await _png(cjkKey);
  final info = jsonEncode({
    'platform': defaultTargetPlatform.name,
    'ratio': ratio,
    'mixed': mixed,
    'cjk': cjkOnly,
    'cellW': cellW,
    'cellH': cellH,
    'labelW': labelW,
    'headerH': headerH,
    'weights': weights,
    'columns': [
      for (final c in columns)
        {
          'name': c.name,
          'size': c.size,
          'system': c.system,
          'family': c.family.name,
          'canvas': _hex(RaftTokens.theme(c.family).canvas),
          'ink': _hex(RaftTokens.theme(c.family).ink),
        },
    ],
    'fontFiles': _mappedFonts(),
  });
  final out = Platform.environment['CJK_OUT'];
  if (out != null) {
    Directory(out).createSync(recursive: true);
    File('$out/sheet.png').writeAsBytesSync(sheet);
    File('$out/cjk.png').writeAsBytesSync(cjk);
    File('$out/info.json').writeAsStringSync(info);
  }
  final server = await HttpServer.bind(InternetAddress.loopbackIPv4, 8799);
  debugPrint('CJKPROBE ready http://127.0.0.1:8799/sheet.png ${out ?? ""}');
  server.listen((r) {
    final p = r.uri.path;
    final bytes = p == '/sheet.png'
        ? sheet
        : p == '/cjk.png'
        ? cjk
        : utf8.encode(info);
    r.response.headers.contentType = p == '/info.json'
        ? ContentType.json
        : ContentType('image', 'png');
    r.response.add(bytes);
    r.response.close();
  });
  if (Platform.environment['CJK_EXIT'] == '1') exit(0);
}

String _hex(Color c) =>
    '#${(c.toARGB32() & 0xffffff).toRadixString(16).padLeft(6, '0')}';

/// Font files the process has mapped (Linux only), for fallback attribution.
List<String> _mappedFonts() {
  try {
    return {
      for (final l in File('/proc/self/maps').readAsLinesSync())
        if (RegExp(r'\.(ttc|otf|ttf|woff2?)$').hasMatch(l.trim()))
          l.trim().substring(l.trim().indexOf('/')),
    }.toList()..sort();
  } on Object {
    return const [];
  }
}

Future<Uint8List> _png(GlobalKey key) async {
  final boundary =
      key.currentContext!.findRenderObject()! as RenderRepaintBoundary;
  final image = await boundary.toImage(pixelRatio: ratio);
  final data = await image.toByteData(format: ui.ImageByteFormat.png);
  return data!.buffer.asUint8List();
}

class _Grid extends StatelessWidget {
  const _Grid({super.key, required this.text, required this.weights});

  final String text;
  final List<int> weights;

  @override
  Widget build(BuildContext context) {
    const label = TextStyle(
      fontFamily: 'monospace',
      fontSize: 12,
      color: Colors.black54,
    );
    return RepaintBoundary(
      child: ColoredBox(
        color: Colors.white,
        child: Column(
          children: [
            SizedBox(
              height: headerH,
              child: Row(
                children: [
                  const SizedBox(width: labelW),
                  for (final c in columns)
                    SizedBox(
                      width: cellW,
                      child: Text(c.name, style: label),
                    ),
                ],
              ),
            ),
            for (final w in weights)
              Row(
                children: [
                  SizedBox(
                    width: labelW,
                    child: Text('w$w', style: label),
                  ),
                  for (final c in columns)
                    _Cell(column: c, weight: w, text: text),
                ],
              ),
          ],
        ),
      ),
    );
  }
}

class _Cell extends StatelessWidget {
  const _Cell({required this.column, required this.weight, required this.text});

  final ({String name, RaftFamily family, bool system, double size}) column;
  final int weight;
  final String text;

  @override
  Widget build(BuildContext context) {
    final locale = const Locale.fromSubtags(
      languageCode: 'zh',
      scriptCode: 'Hans',
    );
    final theme = raftTheme(
      column.family,
      systemFonts: column.system,
      systemFontFamilies: column.system
          ? NativeSystemFonts.instance.value
          : const RaftFontFamilies(),
      locale: locale,
      textLocale: locale,
    );
    final t = RaftTokens.theme(column.family);
    return Theme(
      data: theme,
      child: Container(
        width: cellW,
        height: cellH,
        color: t.canvas,
        alignment: Alignment.centerLeft,
        padding: const EdgeInsets.only(left: 8),
        child: Text(
          text,
          maxLines: 1,
          softWrap: false,
          style: theme.textTheme.bodyMedium!.copyWith(
            fontSize: column.size,
            height: 1.4,
            fontWeight: FontWeight.values[weight ~/ 100 - 1],
            color: t.ink,
          ),
        ),
      ),
    );
  }
}
