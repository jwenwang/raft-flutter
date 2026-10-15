import 'package:drift/drift.dart';

import '../data/message_history.dart';
import '../data/workspace_cache_codec.dart';

/// Updates existing rollback copies from accepted canonical rows. Call this
/// inside the message transaction; a failed shadow write must roll it back.
class HistoryShadows {
  HistoryShadows(this.db, this.readLegacy, this.writeLegacy);
  final GeneratedDatabase db;
  final Future<dynamic> Function(String, String, String, String, String)
  readLegacy;
  final Future<void> Function(String, String, String, String, String, dynamic)
  writeLegacy;

  Future<void> refresh(MessageHistoryScope scope) async {
    final marker = await readLegacy(
      scope.origin,
      scope.principal,
      scope.server,
      'message-history',
      scope.channel,
    );
    final parent =
        scope.parentMessage ??
        (marker is Map && marker['parentMessageId'] is String
            ? marker['parentMessageId'] as String
            : null);
    // These are at most two primary-key lookups, not a scan of saved windows.
    for (final (kind, id) in [
      ('window', scope.channel),
      if (parent != null) ('thread-window', parent),
    ]) {
      dynamic raw;
      try {
        raw = await readLegacy(
          scope.origin,
          scope.principal,
          scope.server,
          kind,
          id,
        );
      } on FormatException {
        continue;
      }
      if (raw is! Map ||
          raw['version'] != 1 ||
          raw['windowKind'] != 'tail' ||
          raw['authority'] != scope.authority ||
          raw['parentChannelId'] != scope.parentChannel ||
          kind == 'thread-window' && raw['threadChannelId'] != scope.channel) {
        continue;
      }
      final old = raw['messages'];
      if (old is! List ||
          old.length > 500 ||
          old.any((row) => row is! Map || row['id'] is! String)) {
        continue;
      }
      final ids = [for (final row in old) row['id'] as String];
      final stored = <String, QueryRow>{};
      for (var n = 0; n < ids.length; n += 200) {
        final batch = ids.skip(n).take(200).toList();
        final rows = await db
            .customSelect(
              'SELECT message_id,tombstone,payload FROM raft_history_rows WHERE origin=? AND principal=? AND server=? AND channel=? '
              'AND message_id IN (${List.filled(batch.length, '?').join(',')})',
              variables: [
                for (final value in [...scope.key, ...batch]) Variable(value),
              ],
            )
            .get();
        for (final row in rows) {
          stored[row.read<String>('message_id')] = row;
        }
      }
      final live = stored.values
          .where((row) => row.read<int>('tombstone') == 0)
          .toList();
      final decoded = await decodeWorkspaceTranslations([
        for (final row in live) row.read<String>('payload'),
      ]);
      final bodies = {
        for (var n = 0; n < live.length; n++)
          live[n].read<String>('message_id'): decoded[n],
      };
      final replacement = {
        ...Map<String, dynamic>.from(raw),
        'messages': [
          for (final row in old)
            if (!stored.containsKey(row['id']) &&
                (marker is! Map ||
                    marker['evictionSequence'] is! String ||
                    historySequenceKey(row['seq'])
                            .compareTo(marker['evictionSequence'] as String) >
                        0))
              row
            else if (bodies.containsKey(row['id']))
              bodies[row['id']],
        ],
      };
      if (await encodeWorkspaceCache(raw) ==
          await encodeWorkspaceCache(replacement)) {
        continue;
      }
      final guards = await db
          .customSelect(
            'SELECT 1 FROM raft_history_compat_write WHERE origin=? AND principal=? AND server=?',
            variables: [for (final value in scope.key.take(3)) Variable(value)],
          )
          .get();
      if (guards.isEmpty) {
        await db.customStatement(
          'INSERT INTO raft_history_compat_write VALUES(?,?,?)',
          scope.key.take(3).toList(),
        );
      }
      await writeLegacy(
        scope.origin,
        scope.principal,
        scope.server,
        kind,
        id,
        replacement,
      );
      if (guards.isEmpty) {
        await db.customStatement(
          'DELETE FROM raft_history_compat_write WHERE origin=? AND principal=? AND server=?',
          scope.key.take(3).toList(),
        );
      }
    }
  }
}
