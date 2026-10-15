import 'dart:async';

import 'package:raft_client/raft_client.dart';

import 'message_history.dart';
import 'history_read_policy.dart';
import 'message_history_pages.dart';
import 'workspace_controller.dart';
import 'workspace_message_windows.dart';

/// The receiver queue preserves socket order while a previously unseen thread
/// resolves its parent through existing, authorized read routes.
class MessageHistoryEvents {
  static void _saveFailed(WorkspaceController w, Object cause) {
    if (w.error != null) return;
    w.error = MessageHistoryPages.saveError(cause);
    w.notifyListeners();
  }

  static bool Function() _current(
    WorkspaceController w, {
    Iterable<String> channels = const [],
  }) {
    final seal = w.messageHistoryTicket('');
    final tickets = {for (final id in channels) id: w.messageHistoryTicket(id)};
    final generation = w.ledger.generation,
        origin = w.client.origin,
        principal = w.client.user?.id,
        server = w.client.serverId,
        role = w.server?.string('role');
    return () =>
        w.cacheWritable &&
        seal == w.messageHistoryTicket('') &&
        tickets.entries.every(
          (e) => e.value == w.messageHistoryTicket(e.key),
        ) &&
        generation == w.ledger.generation &&
        origin == w.client.origin &&
        principal == w.client.user?.id &&
        server == w.client.serverId &&
        role == w.server?.string('role');
  }

  static MessageHistoryScope _probe(WorkspaceController w) =>
      MessageHistoryScope(
        origin: w.client.origin,
        principal: w.client.user!.id,
        server: w.client.serverId!,
        channel: '',
        parentChannel: '',
        authority: '',
      );

  static void store(
    WorkspaceController w,
    Map<String, dynamic> row, {
    bool updateOnly = false,
  }) {
    final db = MessageHistoryPages.storage(w.cache);
    if (db == null || w.client.user == null || w.client.serverId == null) {
      return;
    }
    final anchor = row['conversationContext'];
    final parent = anchor is Map && anchor['parentChannelId'] is String
        ? anchor['parentChannelId'] as String
        : null;
    final current = _current(w, channels: ['${row['channelId']}', ?parent]);
    final probe = _probe(w);
    final channel = '${row['channelId']}', id = '${row['id']}';
    final unheld = !w.ledger.contains(channel, id);
    final rowReceipt = db.receiver.beginRow(probe, channel, id);
    final receipt = updateOnly ? null : db.receiver.begin(probe, [row]);
    Future<void> apply(Map<String, dynamic> value, int revision) async {
      var success = false, unresolved = false;
      try {
        if (!current()) return;
        success = await _persist(
          w,
          db,
          [value],
          current,
          updateOnly: updateOnly,
          requestedRevision: revision,
          written: () => receipt?.finish(true, resolved: true),
          unresolved: () => unresolved = true,
        );
        if (success &&
            updateOnly &&
            unheld &&
            current() &&
            rowReceipt.current()) {
          final pointer = w.ledger.message(channel, id);
          if (pointer != null) {
            final scope = await db.knownScope(
              probe.origin,
              probe.principal,
              probe.server,
              channel,
            );
            final projection = scope == null
                ? null
                : await db.readContext(scope, id, radius: 0);
            if (current() &&
                rowReceipt.current() &&
                identical(pointer, w.ledger.message(channel, id)) &&
                projection != null) {
              final full = (projection['messages'] as List)
                  .whereType<Map>()
                  .where((m) => m['id'] == id)
                  .firstOrNull;
              if (full != null) {
                final body = Map<String, dynamic>.from(full);
                w.ledger.ingest(
                  [body],
                  expectedGeneration: w.ledger.generation,
                  complete: true,
                );
                w.messageSync.replaceBodies(channel, [body]);
                w.notifyListeners();
              }
            }
          }
        }
      } finally {
        receipt?.finish(
          !current() || success,
          unresolved: unresolved && current(),
        );
      }
    }

    late final Future<void> pending;
    if (updateOnly) {
      pending = db.receiver.coalesceUpdate(
        w,
        probe,
        (
          w,
          probe.origin,
          probe.principal,
          probe.server,
          channel,
          id,
          w.ledger.generation,
          w.messageHistoryTicket(''),
          w.messageHistoryTicket(channel),
          parent,
          w.messageHistoryTicket(parent ?? channel),
          w.server?.string('role'),
        ),
        row,
        () => db.receiverRevision(probe),
        apply,
      );
    } else {
      final revision = db.receiverRevision(probe);
      revision.ignore();
      pending = db.receiver.run(() async => apply(row, await revision));
    }
    unawaited(
      pending.whenComplete(rowReceipt.finish).catchError((Object cause) {
        if (current()) _saveFailed(w, cause);
      }),
    );
  }

  static Future<MessageHistoryScope?> _scope(
    WorkspaceController w,
    MessageHistoryStorage db,
    String id,
    bool Function() current, {
    required Map<String, dynamic> row,
    bool updateOnly = false,
  }) async {
    if (!current() || !w.messageWindows.notRevoked(id)) return null;
    final direct = MessageHistoryPages.scope(
      w,
      id,
      parent: id == w.threadChannelId ? w.threadParentChannelId : id,
      parentMessage: id == w.threadChannelId
          ? w.threadIdentity?.parentMessageId
          : null,
    );
    if (direct != null) return direct;
    final saved = await db.knownScope(
      w.client.origin,
      w.client.user!.id,
      w.client.serverId!,
      id,
    );
    String? parent = saved?.parentChannel, parentMessage = saved?.parentMessage;
    if (!current()) return null;
    final anchor = row['conversationContext'];
    // Shared threads carry the caller's local parent projection on the wire.
    // The canonical parent message can belong to another workspace.
    if (!updateOnly &&
        parent == null &&
        anchor is Map &&
        anchor['channelType'] == 'thread' &&
        anchor['parentChannelId'] is String &&
        anchor['parentMessageId'] is String) {
      parent = anchor['parentChannelId'] as String;
      parentMessage = anchor['parentMessageId'] as String;
    }
    if (parent == null) {
      if (updateOnly) {
        return null; // A partial edit never invents an unseen row.
      }
      final raw = await w.client.get('/channels/$id');
      if (!current() || raw is! Map || raw['id'] != id) return null;
      if (raw['type'] != 'thread') {
        final resolved = RaftChannel(Map<String, dynamic>.from(raw));
        return w.can('viewChannel', resource: resolved)
            ? MessageHistoryPages.scope(w, id, resolved: resolved)
            : null;
      }
      parentMessage = raw['parentMessageId'] as String?;
      if (parentMessage == null) return null;
      // The mounted context endpoint allows a zero-neighbour parent lookup.
      final context = await w.client.get(
        '/messages/context/$parentMessage',
        query: {'before': 0, 'after': 0},
      );
      if (!current() || context is! Map) return null;
      parent = context['channelId'] as String?;
    }
    if (parent == null || !w.messageWindows.notRevoked(parent)) return null;
    var scope = MessageHistoryPages.scope(
      w,
      id,
      parent: parent,
      parentMessage: parentMessage,
    );
    if (scope == null) {
      final raw = await w.client.get('/channels/$parent');
      if (!current() || raw is! Map || raw['id'] != parent) return null;
      final resolved = RaftChannel(Map<String, dynamic>.from(raw));
      if (!w.can('viewChannel', resource: resolved)) return null;
      scope = MessageHistoryPages.scope(
        w,
        id,
        parent: parent,
        parentMessage: parentMessage,
        resolved: resolved,
      );
    }
    return current() && w.messageWindows.notRevoked(id) ? scope : null;
  }

  static Future<bool> _persist(
    WorkspaceController w,
    MessageHistoryStorage db,
    List<Map<String, dynamic>> rows,
    bool Function() current, {
    bool updateOnly = false,
    BigInt? through,
    required int requestedRevision,
    required void Function() written,
    void Function()? unresolved,
  }) async {
    final grouped = <String, List<Map<String, dynamic>>>{};
    for (final row in rows) {
      final id = row['channelId'];
      if (id is String && w.messageWindows.notRevoked(id)) {
        grouped.putIfAbsent(id, () => []).add(row);
      }
    }
    MessageHistoryScope? checkpointScope;
    var highest = BigInt.zero;
    for (final entry in grouped.entries) {
      if (!current()) return false;
      final scope = await _scope(
        w,
        db,
        entry.key,
        current,
        row: entry.value.first,
        updateOnly: updateOnly,
      );
      if (!current()) return false;
      if (scope == null) {
        if (updateOnly) continue;
        unresolved?.call();
        return false;
      }
      if (!await db.acceptEvents(
        scope,
        entry.value,
        updateOnly: updateOnly,
        requestedRevision: requestedRevision,
      )) {
        return false;
      }
      if (!current()) return false;
      checkpointScope ??= scope;
      for (final row in entry.value) {
        final seq = BigInt.tryParse('${row['seq']}');
        if (seq != null && seq > highest) highest = seq;
      }
    }
    // Empty resume responses can still advance an existing receiver checkpoint.
    checkpointScope ??= _probe(w);
    written();
    if (!updateOnly && current()) {
      await db.checkpoint(checkpointScope, through ?? highest);
    }
    if (current() && grouped.isNotEmpty) {
      db.reads.count(HistoryReadReason.incremental);
    }
    return current();
  }

  /// Resume uses committed progress, so a device write failure cannot cause a
  /// later reconnect to skip messages that exist only in memory.
  static Future<void> resume(WorkspaceController w) async {
    if (w.client.user == null || w.client.serverId == null) return;
    final db = MessageHistoryPages.storage(w.cache), current = _current(w);
    if (db == null) {
      if (w.ledger.watermark > BigInt.zero) w.client.resume(w.ledger.watermark);
      return;
    }
    try {
      await db.receiver.settled;
      if (!current()) return;
      final scope = _probe(w);
      final committed = await db.resumeSequence(scope);
      // First-upgrade sessions can already hold accepted legacy rows without
      // a durable cursor. Keep that reconnect path, bounded by pending writes.
      final seq = committed > BigInt.zero
          ? committed
          : db.receiver.checkpointLimit(scope, w.ledger.watermark);
      if (current() && seq > BigInt.zero) w.client.resume(seq);
    } catch (cause) {
      if (current()) {
        _saveFailed(w, cause);
      }
    }
  }

  static Future<void> resumeResponse(
    WorkspaceController w,
    Map payload, {
    required void Function() refreshUnread,
  }) async {
    if (w.client.user == null || w.client.serverId == null) return;
    final db = MessageHistoryPages.storage(w.cache);
    final rows = (payload['messages'] as List? ?? const [])
        .whereType<Map>()
        .map((m) => Map<String, dynamic>.from(m))
        .where((m) => w.messageWindows.notRevoked('${m['channelId']}'))
        .toList();
    final current = _current(
      w,
      channels: [
        for (final row in rows) ...[
          '${row['channelId']}',
          if (row['conversationContext'] case {
            'parentChannelId': final String parent,
          })
            parent,
        ],
      ],
    );
    final revision = db?.receiverRevision(_probe(w));
    revision?.ignore();
    final receipt = db?.receiver.begin(_probe(w), rows);
    final initial = {
      for (final row in rows)
        row['id']: w.ledger.message('${row['channelId']}', '${row['id']}'),
    };
    final cursor = BigInt.tryParse('${payload['currentSeq']}');
    Future<void> apply() async {
      var committed = db == null, unresolved = false;
      try {
        if (db != null && current()) {
          committed = await _persist(
            w,
            db,
            rows,
            current,
            through: cursor,
            requestedRevision: await revision!,
            written: () {
              receipt?.finish(true, replayedThrough: cursor, resolved: true);
              db.receiver.confirmReplay(_probe(w), cursor);
            },
            unresolved: () => unresolved = true,
          );
        }
      } catch (cause) {
        if (current()) {
          _saveFailed(w, cause);
        }
      } finally {
        receipt?.finish(
          !current() || committed,
          unresolved: unresolved && current(),
        );
      }
      if (!current()) return;
      final accepted = rows
          .where(
            (row) => identical(
              initial[row['id']],
              w.ledger.message('${row['channelId']}', '${row['id']}'),
            ),
          )
          .toList();
      w.ledger.ingest(accepted, expectedGeneration: w.ledger.generation);
      for (final row in accepted) {
        final id = row['channelId'];
        if (id == w.channel?.id && !w.hasNewer ||
            id == w.threadChannelId &&
                !w.messageWindows.contextHasNewer(id as String) ||
            id is String && w.messageWindows.retainsLiveTail(id)) {
          w.messageWindows.reveal(liveWindowWrite, id, [row['id']]);
        }
      }
      if (w.channel != null && !w.hasNewer) {
        unawaited(w.markRead(w.channel!.id));
      }
      if (w.threadChannelId != null) unawaited(w.markRead(w.threadChannelId!));
      if (committed &&
          payload['hasMore'] == true &&
          cursor != null &&
          cursor > BigInt.zero) {
        w.client.resume(cursor);
      }
      refreshUnread();
      w.notifyListeners();
    }

    if (db == null) return apply();
    await db.receiver.run(apply);
  }
}
