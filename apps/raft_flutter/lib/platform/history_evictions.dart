import 'package:drift/drift.dart';

import '../data/history_capacity.dart';
import '../data/message_history.dart';
import '../data/workspace_cache_codec.dart';
import 'message_history.dart' show HistoryOperationOrder;

/// Retains exact recent eviction identities and a conservative fence for the
/// older identities. The fence never expires while its conversation exists.
class HistoryEvictions {
  HistoryEvictions(this.db);
  final GeneratedDatabase db;
  static const retainedIds = 256;
  static const _where = 'origin=? AND principal=? AND server=? AND channel=?';
  Future<List<QueryRow>> _query(String sql, Iterable<Object> values) => db
      .customSelect(
        sql,
        variables: [
          for (final v in values) v is int ? Variable(v) : Variable('$v'),
        ],
      )
      .get();

  /// Call inside the caller's transaction. At most 200 identities are removed.
  Future<bool> trimNow(MessageHistoryScope scope) async {
    final rows = await _query(
      'SELECT message_id,seq_key,revision FROM raft_history_rows WHERE $_where AND tombstone=2 ORDER BY revision DESC,message_id DESC LIMIT 200 OFFSET $retainedIds',
      scope.key,
    );
    if (rows.isEmpty) return false;
    final records = await _query(
      "SELECT payload FROM workspace_cache WHERE origin=? AND principal=? AND server=? AND kind='message-history' AND id=?",
      scope.key,
    );
    if (records.isEmpty) return false;
    final marker = Map<String, dynamic>.from(
      await decodeWorkspaceCache(records.single.read<String>('payload')) as Map,
    );
    var revision = marker['evictionRevision'] as int? ?? 0;
    var sequence =
        marker['evictionSequence'] as String? ??
        historySequenceKey(BigInt.zero);
    for (final row in rows) {
      if (row.read<int>('revision') > revision) {
        revision = row.read<int>('revision');
      }
      if (row.read<String>('seq_key').compareTo(sequence) > 0) {
        sequence = row.read<String>('seq_key');
      }
    }
    await db.customStatement(
      "UPDATE workspace_cache SET payload=? WHERE origin=? AND principal=? AND server=? AND kind='message-history' AND id=?",
      [
        await encodeWorkspaceCache({
          ...marker,
          'evictionRevision': revision,
          'evictionSequence': sequence,
        }),
        ...scope.key,
      ],
    );
    await db.customStatement(
      'DELETE FROM raft_history_rows WHERE $_where AND tombstone=2 AND message_id IN (${List.filled(rows.length, '?').join(',')})',
      [...scope.key, for (final row in rows) row.read<String>('message_id')],
    );
    return rows.length == 200;
  }

  /// Handles receipts left by an older installation, even below its quota.
  /// Each transaction visits one marker / 200 receipts, then frees the queue.
  Future<void> cleanAccount(
    HistoryAccount a,
    HistoryOperationOrder order,
    bool Function() current,
  ) async {
    final ceiling = await order(
      () async => (await _query(
        "SELECT coalesce(max(rowid),0) AS last FROM workspace_cache WHERE origin=? AND principal=? AND kind='message-history'",
        [a.origin, a.principal],
      )).single.read<int>('last'),
    );
    var cursor = 0;
    while (current() && cursor < ceiling) {
      final next = await order(
        () => db.transaction(() async {
          if (!current()) return ceiling;
          final records = await _query(
            "SELECT rowid,server,id,payload FROM workspace_cache WHERE rowid>? AND rowid<=? AND origin=? AND principal=? AND kind='message-history' ORDER BY rowid LIMIT 1",
            [cursor, ceiling, a.origin, a.principal],
          );
          if (records.isEmpty) return ceiling;
          final record = records.single;
          final marker = Map<String, dynamic>.from(
            await decodeWorkspaceCache(record.read<String>('payload')) as Map,
          );
          final scope = MessageHistoryScope(
            origin: a.origin,
            principal: a.principal,
            server: record.read<String>('server'),
            channel: record.read<String>('id'),
            parentChannel: marker['parentChannelId'] as String,
            authority: marker['authority'] as String,
          );
          return await trimNow(scope) ? cursor : record.read<int>('rowid');
        }),
      );
      cursor = next;
      if (cursor < ceiling) {
        await Future<void>.delayed(const Duration(milliseconds: 10));
      }
    }
  }
}
