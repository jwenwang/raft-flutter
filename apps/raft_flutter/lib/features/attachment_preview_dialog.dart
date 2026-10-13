import 'dart:async';
import 'dart:convert';

import 'package:dio/dio.dart';
import 'package:flutter/foundation.dart';
import 'package:flutter/material.dart';
import 'package:raft_ui/raft_ui.dart';

import '../data/attachment_image_store.dart';
import '../data/user_error.dart';
import '../data/workspace_controller.dart';
import '../platform/attachment_files.dart';
import '../platform/attachment_http.dart';
import '../platform/attachment_player.dart';
import '../platform/attachment_player_factory.dart';
import '../platform/attachment_player_sessions.dart';
import '../platform/attachment_preview_files.dart';
import '../platform/attachment_stream.dart';
import '../platform/mp4_hdr_probe.dart';
import '../platform/native_sharing.dart';
import '../data/attachment_transfer.dart';
import 'attachment_comments_view.dart';
import 'attachment_transfer_view.dart';

String? attachmentPreviewKind(Map<String, dynamic> metadata) {
  final name = '${metadata['filename'] ?? ''}'.toLowerCase();
  final mime = '${metadata['mimeType'] ?? ''}'
      .toLowerCase()
      .split(';')
      .first
      .trim();
  bool ext(List<String> values) => values.any(name.endsWith);
  if (ext(['.pdf']) || mime == 'application/pdf') return 'pdf';
  if (ext(['.mp4', '.webm', '.mov']) ||
      ['video/mp4', 'video/webm', 'video/quicktime'].contains(mime)) {
    return 'video';
  }
  if (ext([
        '.mp3',
        '.wav',
        '.m4a',
        '.aac',
        '.ogg',
        '.oga',
        '.opus',
        '.weba',
        '.flac',
      ]) ||
      [
        'audio/mpeg',
        'audio/mp3',
        'audio/wav',
        'audio/x-wav',
        'audio/wave',
        'audio/aac',
        'audio/mp4',
        'audio/x-m4a',
        'audio/ogg',
        'audio/opus',
        'audio/webm',
        'audio/flac',
        'audio/x-flac',
      ].contains(mime)) {
    return 'audio';
  }
  if (ext(['.diff', '.patch']) ||
      ['text/x-diff', 'text/x-patch', 'application/x-patch'].contains(mime)) {
    return null;
  }
  if (ext(['.html', '.htm']) || mime == 'text/html') return null;
  final size = metadata['sizeBytes'];
  if (!ext(['.xls', '.xlt', '.xla', '.xlm', '.xlc', '.xlw']) &&
      (ext(['.csv']) ||
          [
            'text/csv',
            'application/csv',
            'application/vnd.ms-excel',
          ].contains(mime)) &&
      (size is! num || size <= 5 * 1024 * 1024)) {
    return 'document';
  }
  if ((ext(['.xlsx']) ||
          mime ==
              'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet') &&
      (size is! num || size <= 10 * 1024 * 1024)) {
    return 'document';
  }
  if (ext(['.md', '.markdown']) ||
      ['text/markdown', 'text/x-markdown'].contains(mime)) {
    return 'document';
  }
  if (mime.startsWith('text/') && !['text/csv', 'text/html'].contains(mime) ||
      [
        'application/json',
        'application/xml',
        'application/yaml',
        'application/x-yaml',
        'application/toml',
        'application/x-sh',
      ].contains(mime) ||
      ext([
        '.txt',
        '.text',
        '.log',
        '.ini',
        '.conf',
        '.cfg',
        '.properties',
        '.toml',
        '.yaml',
        '.yml',
        '.json',
        '.xml',
        '.sql',
        '.gradle',
        '.sh',
        '.bash',
        '.zsh',
        '.kt',
        '.kts',
        '.java',
        '.swift',
        '.ets',
        '.ts',
        '.tsx',
        '.js',
        '.jsx',
        '.mjs',
        '.py',
        '.rb',
        '.go',
        '.rs',
        '.c',
        '.h',
        '.cpp',
        '.hpp',
      ])) {
    return 'document';
  }
  return null;
}

/// Whether a video is HDR (its own colour boxes, never the codec): decides if
/// the viewer needs the native video view. Tests replace it.
typedef HdrDetector = Future<bool> Function(Uri url);

/// Measurements of the last card-to-native-view handoff of an HDR video
/// (milliseconds from the decision to the first frame, and where it started).
class HdrHandoff {
  const HdrHandoff(this.milliseconds, this.start);
  final int milliseconds;
  final Duration start;
}

@visibleForTesting
HdrHandoff? debugLastHdrHandoff;

class AttachmentPreviewDialog extends StatefulWidget {
  const AttachmentPreviewDialog({
    super.key,
    required this.controller,
    required this.metadata,
    this.channelId,
    required this.authorized,
    required this.onClose,
    required this.onDownload,
    this.files,
    this.pdf,
    this.playerFactory,
    this.hdrDetector,
    this.page = false,
  });
  final WorkspaceController controller;
  final Map<String, dynamic> metadata;
  final String? channelId;
  final bool Function() authorized;
  final VoidCallback onClose;
  final Future<void> Function() onDownload;
  final AttachmentPreviewFiles? files;
  final NativePdfRenderer? pdf;
  final AttachmentPlayer Function()? playerFactory;

  /// Replaces the HDR probe (tests).
  final HdrDetector? hdrDetector;
  final bool page;
  @override
  State<AttachmentPreviewDialog> createState() =>
      _AttachmentPreviewDialogState();
}

class _AttachmentPreviewDialogState extends State<AttachmentPreviewDialog>
    with WidgetsBindingObserver {
  final cancel = CancelToken();
  SharedFileLease? lease;

  /// The shared stream (the card's player when one exists) and, for an HDR
  /// video, the viewer's own native-view player ([hdrHold]); [player] is the
  /// one on screen.
  PlayerHold? hold, hdrHold;

  /// The audio viewer's own player (it plays the cached file).
  AttachmentPlayer? ownPlayer;
  AttachmentPlayer? player;
  StreamSubscription<AttachmentPlayback>? playbackEvents;
  AttachmentPlayback playback = const AttachmentPlayback();
  Map<String, dynamic>? document;
  Uint8List? pageBytes;
  int page = 0, pageCount = 0, request = 0;
  bool loading = true, truncated = false, invalidated = false;

  /// The file itself could not be fetched (as opposed to decoded or played):
  /// the download panel offers a retry.
  bool fetchFailed = false;
  String? error;

  /// Bytes of the preview download, throttled; the panel's bar listens.
  final transfer = AttachmentTransfer();
  late final transferView = AttachmentTransferView(transfer);
  Future<void>? cleanup;
  String get kind => attachmentPreviewKind(widget.metadata) ?? 'unsupported';
  bool get current =>
      mounted && !invalidated && !cancel.isCancelled && widget.authorized();
  @override
  void initState() {
    super.initState();
    WidgetsBinding.instance.addObserver(this);
    widget.controller.addListener(scopeChanged);
    load();
  }

  void scopeChanged() {
    if (!widget.authorized()) {
      invalidated = true;
      if (pageBytes != null) unawaited(raftEvictMemoryImage(pageBytes!));
      setState(() {
        document = null;
        pageBytes = null;
      });
      cancel.cancel();
      if (player != null) unawaited(player!.pause().catchError((_) {}));
      unawaited(releaseInputs());
      widget.onClose();
    }
  }

  @override
  void didChangeAppLifecycleState(AppLifecycleState state) {
    if (state != AppLifecycleState.resumed && player != null) {
      unawaited(action((p) => p.pause()));
    }
  }

  /// Fetch again after a failure; the same panel returns to progress.
  void retry() {
    if (!current) return;
    transfer.value = null;
    setState(() {
      loading = true;
      error = null;
      fetchFailed = false;
    });
    load();
  }

  Future<void> load() async {
    final ticket = ++request;
    try {
      if (!current) return;
      if (kind == 'document') {
        // A document opened before on this device shows from the cache: no
        // preview request (it counts against the attachment rate limit).
        final stored = await readStoredDocument();
        if (!current || ticket != request) return;
        if (stored != null) {
          setState(() {
            document = stored.$1;
            truncated = stored.$2;
          });
          return;
        }
        final value = await widget.controller.query(
          '/attachments/${widget.metadata['id']}/preview',
        );
        if (!current || ticket != request) return;
        // Preview refusals are successful HTTP responses, not API errors.
        // Preserve their reason before validating the document payload.
        if (value is Map && value['status'] == 'unsupported') {
          final reason = value['reason'];
          final message = switch (reason) {
            'too_large' => 'This file exceeds the server preview size limit. Download the original file to view it.',
            'unreadable' => 'The file could not be read and may be damaged. Download the original file to view it.',
            'unsupported' => 'This file format is not supported for preview. Download the original file to view it.',
            _ => 'Preview unavailable. Download the original file.',
          };
          // Log bounded reason codes, never arbitrary response text or URLs.
          final code =
              reason is String && RegExp(r'^[a-z_]{1,64}$').hasMatch(reason)
              ? reason
              : 'unknown';
          logUserError(
            FormatException('Preview status=unsupported reason=$code'),
            where: 'attachment preview ${widget.metadata['id']}',
          );
          throw RaftUserError(message);
        }
        if (value is! Map ||
            value['status'] != 'ok' ||
            value['data'] is! Map ||
            ![
              'text',
              'markdown',
              'csv',
              'xlsx',
            ].contains(value['data']['kind'])) {
          throw const FormatException('The file cannot be previewed.');
        }
        setState(() {
          document = Map<String, dynamic>.from(value['data']);
          truncated = value['truncated'] == true;
        });
        unawaited(storeDocument(document!, truncated));
      } else if (kind == 'video') {
        // Streamed, never downloaded first: the viewer takes the card's player
        // over when there is one (same stream, same position), otherwise it
        // opens the attachment's URL itself.
        final owner =
            widget.channelId ?? widget.metadata['channelId'] as String?;
        final acquired = widget.controller.playerSessions.acquire(
          attachmentMediaKey(widget.metadata),
          scope: widget.controller.attachmentImageScope,
          channelId: owner,
          viewer: true,
          create:
              widget.playerFactory ?? () => createAttachmentPlayer(video: true),
          source: () => attachmentStreamSource(
            widget.controller,
            '${widget.metadata['id']}',
            channelId: owner,
          ),
        );
        hold = acquired;
        bindPlayer(acquired.player);
        await acquired.ready;
        if (!current || ticket != request) return;
        unawaited(considerHdr(acquired));
      } else if (kind == 'pdf' || kind == 'audio') {
        final owner =
            widget.channelId ?? widget.metadata['channelId'] as String?;
        Future<String> resolveUrl() => widget.controller.attachmentUrl(
          '${widget.metadata['id']}',
          channelId: owner,
        );
        final files = widget.files ?? AttachmentPreviewFiles();
        final filename = '${widget.metadata['filename'] ?? 'attachment'}';
        // PDF and audio come from the persistent cache when it holds them
        // (no request, not even for the signed URL) and are kept after the
        // first download. Video is streamed (above).
        final prepared = await files.loadCached(
          cache: widget.controller.attachmentDiskCache,
          key: widget.controller.attachmentCacheKey(
            widget.metadata,
            'file',
            channelId: owner ?? '',
          ),
          resolveUrl: resolveUrl,
          refreshUrl: () {
            widget.controller.invalidateAttachmentUrl(
              '${widget.metadata['id']}',
            );
            return widget.controller.attachmentUrl(
              '${widget.metadata['id']}',
              channelId: owner,
              refresh: true,
            );
          },
          filename: filename,
          expectedBytes: (widget.metadata['sizeBytes'] as num?)?.toInt(),
          cancel: cancel,
          authorized: () => current,
          onProgress: transfer.report,
        );
        if (!current || ticket != request || prepared == null) {
          await prepared?.dispose();
          return;
        }
        lease = prepared;
        if (kind == 'pdf') {
          await loadPage(0);
        } else {
          // The audio viewer plays the cached file with a player of its own.
          final own =
              (widget.playerFactory ??
              () => createAttachmentPlayer(video: false))();
          ownPlayer = own;
          bindPlayer(own);
          await own.open(AttachmentSource.file(prepared.file));
        }
      } else {
        throw const FormatException('The file cannot be previewed.');
      }
    } catch (failure) {
      if (current && ticket == request) {
        setState(() {
          // The server's own reason when it sent one (rate limited, not found,
          // too large); otherwise the generic text, with the cause logged.
          error = userErrorText(
            failure,
            fallback: 'Preview unavailable. Download the original file.',
            where: 'attachment preview ${widget.metadata['id']}',
          );
          // Nothing was prepared: the bytes never arrived (or the preview
          // service failed), so a retry can still help.
          fetchFailed =
              (kind == 'pdf' || kind == 'audio') &&
              lease == null &&
              document == null;
        });
      }
    } finally {
      if (current && ticket == request) setState(() => loading = false);
    }
  }

  /// The document preview stored by an earlier open of this attachment.
  Future<(Map<String, dynamic>, bool)?> readStoredDocument() async {
    final cache = widget.controller.attachmentDiskCache;
    if (cache == null) return null;
    try {
      final bytes = await cache.read(documentKey);
      if (bytes == null) return null;
      final value = jsonDecode(utf8.decode(bytes));
      if (value is Map &&
          value['data'] is Map &&
          ['text', 'markdown', 'csv', 'xlsx'].contains(value['data']['kind'])) {
        return (
          Map<String, dynamic>.from(value['data'] as Map),
          value['truncated'] == true,
        );
      }
      await cache.remove(documentKey); // Unreadable: fetched again.
    } catch (_) {
      // A damaged entry is a miss.
    }
    return null;
  }

  Future<void> storeDocument(Map<String, dynamic> data, bool truncated) async {
    final cache = widget.controller.attachmentDiskCache;
    if (cache == null) return;
    try {
      await cache.write(
        documentKey,
        Uint8List.fromList(
          utf8.encode(jsonEncode({'data': data, 'truncated': truncated})),
        ),
      );
    } catch (_) {
      // Caching is best effort.
    }
  }

  AttachmentImageStoreKey get documentKey =>
      widget.controller.attachmentCacheKey(
        widget.metadata,
        'preview',
        channelId:
            widget.channelId ?? widget.metadata['channelId'] as String? ?? '',
      );

  /// Shows [next]: its events drive the controls from now on.
  void bindPlayer(AttachmentPlayer next) {
    final old = playbackEvents;
    playbackEvents = null;
    unawaited(old?.cancel());
    player = next;
    playback = next.playback;
    playbackEvents = next.changes.listen((value) {
      if (!current || !identical(player, next)) return;
      setState(() {
        playback = value;
        if (value.error) {
          error = 'The media could not be played. Download the original file.';
          unawaited(next.pause().catchError((_) {}));
        }
      });
    });
    if (mounted) setState(() {});
  }

  // ---- HDR: the native view needs a player of its own ----------------------

  bool get platformViewsAvailable =>
      widget.hdrDetector != null ||
      (widget.playerFactory == null &&
          attachmentBackendFor(defaultTargetPlatform) ==
              AttachmentBackend.system);

  Future<bool> detectHdr() async {
    final key = attachmentMediaKey(widget.metadata);
    final verdicts = widget.controller.playerSessions.hdrVerdicts;
    final known = verdicts[key];
    if (known != null) return known;
    final owner = widget.channelId ?? widget.metadata['channelId'] as String?;
    final url = AttachmentFiles.attachmentUri(
      await widget.controller.attachmentUrl(
        '${widget.metadata['id']}',
        channelId: owner,
        coalesce: true,
      ),
    );
    final detector = widget.hdrDetector;
    final verdict = detector != null
        ? await detector(url)
        : (await probeMp4Color(
                AttachmentHttp.shared,
                url,
                cancel: cancel,
              ))?.hdr ??
              false;
    if (verdicts.length > 64) verdicts.remove(verdicts.keys.first);
    return verdicts[key] = verdict;
  }

  /// An HDR video needs the native view (`SurfaceView`, `AVPlayerLayer`): a
  /// controller's view type is fixed when it is created, so the viewer opens a
  /// player of its own at the card's position (streaming reads only the rest),
  /// pauses the shared one, and hands the position back on close. SDR videos
  /// never get here: they keep the shared texture player, seamlessly.
  Future<void> considerHdr(PlayerHold shared) async {
    if (!platformViewsAvailable || hdrHold != null) return;
    // Set once the shared player has been paused for the handoff: every path
    // that does not end with the native player bound must undo that pause.
    var pausedPlaying = false;
    Future<void> resumeShared() async {
      if (!pausedPlaying || shared.orphaned) return;
      pausedPlaying = false;
      try {
        if (!shared.player.playback.playing) await shared.player.toggle();
      } on Object {
        // best effort: the shared player may already be gone
      }
    }

    try {
      if (!await detectHdr() || !current || hdrHold != null || exiting) return;
      final started = Stopwatch()..start();
      final base = shared.player.playback;
      await shared.player.pause();
      pausedPlaying = base.playing;
      final owner = widget.channelId ?? widget.metadata['channelId'] as String?;
      final native = widget.controller.playerSessions.acquire(
        '${attachmentMediaKey(widget.metadata)}|hdr',
        scope: widget.controller.attachmentImageScope,
        channelId: owner,
        viewer: true,
        create:
            widget.playerFactory ??
            () => createAttachmentPlayer(video: true, hdr: true),
        source: () => attachmentStreamSource(
          widget.controller,
          '${widget.metadata['id']}',
          channelId: owner,
          start: base.position,
        ),
      );
      hdrHold = native;
      await native.ready;
      if (!current || exiting || !identical(hdrHold, native)) {
        if (identical(hdrHold, native)) hdrHold = null;
        native.release();
        await resumeShared();
        return;
      }
      // The native player starts at full volume: it carries the shared
      // player's level (a muted video stays muted).
      await native.player.volume(base.volume);
      if (base.playing) await native.player.toggle();
      debugLastHdrHandoff = HdrHandoff(
        started.elapsedMilliseconds,
        base.position,
      );
      debugPrint(
        'raft.media: hdr handoff ${started.elapsedMilliseconds} ms at ${base.position.inMilliseconds} ms',
      );
      bindPlayer(native.player);
    } on Object {
      // The shared texture player keeps playing; HDR is a refinement.
      if (hdrHold != null && !identical(player, hdrHold!.player)) {
        hdrHold?.release();
        hdrHold = null;
      }
      // Not handed over (the native player failed before it was bound): the
      // shared player carries on as it was.
      if (hdrHold == null) await resumeShared();
    }
  }

  /// While the shared player catches up with the native view's position the
  /// stage stays black: its texture still shows an old frame until the seek
  /// lands, and that frame flashed during the exit.
  bool swappingBack = false;

  /// Back from the native view to the shared player, at the position (and
  /// play state) the native one reached: the card carries on from there.
  Future<void> endHdr({bool rebind = true}) async {
    final native = hdrHold, shared = hold;
    if (native == null) return;
    hdrHold = null;
    final last = native.player.playback;
    if (rebind && mounted) setState(() => swappingBack = true);
    native.release();
    if (shared != null && !shared.orphaned) {
      try {
        await shared.player.seek(last.position);
        // Volume changed in the viewer belongs to the card as well.
        await shared.player.volume(last.volume);
        if (last.playing) await shared.player.toggle();
      } on Object {
        // best effort
      }
    }
    if (rebind && shared != null && !shared.orphaned && mounted) {
      bindPlayer(shared.player);
      setState(() => swappingBack = false);
    }
  }

  // ---- the route's transition vs the native view ---------------------------

  ModalRoute<dynamic>? route;
  bool entered = false, exiting = false;

  void routeMoved() {
    final r = route;
    if (r == null) return;
    final animation = r.animation;
    final gesture = r.navigator?.userGestureInProgress ?? false;
    final nowExiting =
        animation?.status == AnimationStatus.reverse || gesture || !r.isActive;
    final nowEntered = animation == null || animation.isCompleted;
    if (nowExiting == exiting && nowEntered == entered) return;
    setState(() {
      exiting = nowExiting;
      entered = nowEntered;
    });
    // The first moment of any pop (close button, system back, back gesture,
    // Escape): the native view must be gone before the slide, or it lags the
    // Flutter content. The shared texture player takes the picture over.
    if (nowExiting && hdrHold != null) unawaited(endHdr());
  }

  @override
  void didChangeDependencies() {
    super.didChangeDependencies();
    final next = ModalRoute.of(context);
    if (identical(next, route)) return;
    route?.animation?.removeListener(routeMoved);
    route?.navigator?.userGestureInProgressNotifier.removeListener(routeMoved);
    route = next;
    next?.animation?.addListener(routeMoved);
    next?.navigator?.userGestureInProgressNotifier.addListener(routeMoved);
    entered = next?.animation == null || next!.animation!.isCompleted;
  }

  /// The picture: a native view only while the route rests (it neither slides
  /// in nor out with Flutter content), black stage otherwise.
  Widget videoSurface(AttachmentPlayer p) {
    if (!swappingBack && (!p.usesPlatformView || (entered && !exiting))) {
      return p.video();
    }
    // ds-allow: video stage is black by design (Web VideoPreviewBody)
    return const ColoredBox(color: Color(0xFF000000));
  }

  Future<void> loadPage(int next) async {
    if (!current || lease == null) return;
    setState(() => loading = true);
    try {
      final value = await (widget.pdf ?? NativePdfRenderer()).render(
        lease!.file,
        next,
        cancel: cancel,
        authorized: () => current,
      );
      if (!current) return;
      if (pageBytes != null) unawaited(raftEvictMemoryImage(pageBytes!));
      setState(() {
        page = next;
        pageBytes = value.bytes;
        pageCount = value.pageCount;
      });
    } catch (_) {
      if (current) {
        setState(
          () => error =
              'The PDF could not be opened. Download the original file.',
        );
      }
    } finally {
      if (current) setState(() => loading = false);
    }
  }

  Future<void> action(Future<void> Function(AttachmentPlayer) run) async {
    if (!current || player == null) return;
    try {
      await run(player!);
    } catch (_) {
      if (current) {
        setState(
          () => error =
              'The media could not be played. Download the original file.',
        );
      }
    }
  }

  @override
  void dispose() {
    invalidated = true;
    request++;
    cancel.cancel();
    transfer.dispose();
    widget.controller.removeListener(scopeChanged);
    WidgetsBinding.instance.removeObserver(this);
    if (pageBytes != null) unawaited(raftEvictMemoryImage(pageBytes!));
    pageBytes = null;
    unawaited(releaseInputs());
    super.dispose();
  }

  Future<void> releaseInputs() => cleanup ??= () async {
    final heldLease = lease;
    try {
      route?.animation?.removeListener(routeMoved);
      route?.navigator?.userGestureInProgressNotifier.removeListener(
        routeMoved,
      );
      await playbackEvents?.cancel();
      // The native view's player is the viewer's own: it goes, and the shared
      // player (the card's, when there is one) continues from its position.
      await endHdr(rebind: false);
      // Gives the shared player back: the card keeps it (playing or paused as
      // it was); with no card it is freed (a reopen streams again).
      hold?.release();
      await ownPlayer?.dispose();
    } catch (_) {
      // A failing native player must not keep anything.
    } finally {
      await heldLease?.dispose();
    }
  }();

  /// The text the pane shows when it is copyable (Web `copyText`).
  String? get copyText {
    final data = document;
    if (data == null) return null;
    return switch (data['kind']) {
      'text' => '${data['text'] ?? ''}',
      'markdown' => '${data['markdown'] ?? ''}',
      _ => null,
    };
  }

  /// Initial fetch of the file or its preview data (not a PDF page turn).
  bool get fetching =>
      (kind == 'pdf' || kind == 'audio') &&
      loading &&
      lease == null &&
      document == null;

  /// File head, bar and `1.7 / 2.8 MB · 62%` with Cancel (leaving the
  /// preview cancels the download), or the failed state with Retry.
  Widget transferPanel(BuildContext context) {
    final name = '${widget.metadata['filename'] ?? ''}';
    final size = (widget.metadata['sizeBytes'] as num?)?.toInt();
    final extension = name.contains('.')
        ? name.split('.').last.toUpperCase()
        : '${widget.metadata['mimeType'] ?? ''}';
    return RaftFileTransferPanel(
      filename: name,
      meta: size == null || size <= 0
          ? extension
          : '$extension · ${RaftAttachmentCard.formatSize(size)}',
      progress: transferView,
      fallbackTotal: size,
      failed: fetchFailed,
      onCancel: widget.onClose,
      onRetry: retry,
    );
  }

  Widget pager() => Row(
    mainAxisAlignment: MainAxisAlignment.center,
    children: [
      RaftIconButton(
        glyph: RaftGlyph.chevronLeft,
        tooltip: 'Previous page',
        visualSize: 28,
        minimumTargetSize: 48,
        variant: RaftControlVariant.ghost,
        onPressed: !loading && page > 0 ? () => loadPage(page - 1) : null,
      ),
      Text('${page + 1} / $pageCount'),
      RaftIconButton(
        glyph: RaftGlyph.chevronRight,
        tooltip: 'Next page',
        visualSize: 28,
        minimumTargetSize: 48,
        variant: RaftControlVariant.ghost,
        onPressed: !loading && page + 1 < pageCount
            ? () => loadPage(page + 1)
            : null,
      ),
    ],
  );

  @override
  Widget build(BuildContext context) => RaftAttachmentPreviewShell(
    filename: '${widget.metadata['filename'] ?? ''}',
    page: widget.page,
    onClose: widget.onClose,
    onDownload: current ? widget.onDownload : null,
    copyText: copyText,
    // Web lays text, csv, xlsx and markdown out on the canvas surface.
    canvas: document != null || kind == 'audio',
    // Web VideoPreviewBody: a solid black stage.
    dark: kind == 'video',
    // Web suppresses attachment comments for PDF and image surfaces.
    commentsBuilder: current && kind != 'pdf' && widget.metadata['id'] is String
        ? (_, sheet) => AttachmentCommentsView(
            controller: widget.controller,
            attachmentId: widget.metadata['id'] as String,
            filename: '${widget.metadata['filename'] ?? ''}',
            authorized: () => current,
            sheet: sheet,
          )
        : null,
    child: Column(
      children: [
        Expanded(
          child: !current
              ? const SizedBox.shrink()
              : fetching || fetchFailed
              ? Center(child: transferPanel(context))
              : loading
              ? Center(
                  child: RaftSpinner(
                    inverse: kind == 'video',
                    size: kind == 'video' ? 32 : null,
                  ),
                )
              : error != null
              ? RaftLoadError(error!, onDark: kind == 'video')
              : document != null
              ? RaftDocumentPreview(data: document!, truncated: truncated)
              : kind == 'pdf' && pageBytes != null
              ? InteractiveViewer(
                  child: RaftSizedImage.memory(
                    pageBytes!,
                    fit: BoxFit.contain,
                    // The page stays sharp at the viewer's maximum zoom.
                    zoom: raftZoomableImageScale,
                    evictOnDispose: true,
                  ),
                )
              : kind == 'video' && player != null
              ? RaftVideoViewer(
                  filename: '${widget.metadata['filename'] ?? ''}',
                  video: videoSurface(player!),
                  videoSize: playback.videoSize,
                  playing: playback.playing,
                  buffering: playback.buffering,
                  position: playback.position,
                  duration: playback.duration,
                  volume: playback.volume,
                  onTogglePlay: () => action((p) => p.toggle()),
                  onSeek: (v) => action((p) => p.seek(v)),
                  onVolume: (v) => action((p) => p.volume(v)),
                  onDownload: current ? widget.onDownload : null,
                )
              : kind == 'audio' && player != null
              ? RaftAudioPreview(
                  filename: '${widget.metadata['filename'] ?? ''}',
                  playing: playback.playing,
                  position: playback.position,
                  duration: playback.duration,
                  volume: (playback.volume / 100).clamp(0.0, 1.0),
                  onTogglePlay: () => action((p) => p.toggle()),
                  onSeek: (v) => action((p) => p.seek(v)),
                  onVolume: (v) => action((p) => p.volume(v * 100)),
                )
              : const SizedBox.shrink(),
        ),
        if (current && kind == 'pdf' && pageCount > 0) pager(),
      ],
    ),
  );
}
