import 'package:raft_client/raft_client.dart';
import 'package:raft_sync/raft_sync.dart' show mergeMessageProjection;

import 'message_history.dart';
import 'history_capacity.dart';
import 'history_read_policy.dart';
import 'message_history_verification.dart';
import 'message_history_memory.dart';
import 'history_snapshot_writes.dart';
import 'message_window_snapshot.dart';
import 'workspace_cache.dart';
import 'workspace_controller.dart';
import 'workspace_message_windows.dart';

/// Transport and local paging use the same admitted projections. Request
/// metadata is held in a Dart object and never added to the server protocol.
class MessageHistoryPages {
  static MessageHistoryPage savedContext(
    WorkspaceController w, MessageHistoryScope scope, Map<String, dynamic> local,
    Map<String, Map<String, dynamic>> initial,
  ) => MessageHistoryPage(
    _live(local, initial, w.ledger.messages(scope.channel), focused: true,
      pending: (id) => storage(w.cache)!.receiver.hasRow(scope,id)),
    scope: scope,
    revision: local is MessageHistoryPage ? local.revision : 0,
    before: null, after: null, local: true, focused: true,
    target: local['targetMessageId'] as String?,
    verifiedAt: local is MessageHistoryPage ? local.verifiedAt : null,
  );
  static String saveError(Object cause) => cause is HistoryReadRetired
      ? 'Local history changed while loading. Try again.'
      : cause is HistoryCapacityExceeded
      ? 'Device message storage is full. Increase the message history limit or stop keeping a conversation in Storage settings.'
      : 'Local changes could not be saved on this device.';
  static void attach(WorkspaceController w) {
    final device = storage(w.cache);
    if (device == null || w.presentationSource != null) return;
    HistorySnapshotWrites.attach(w);
    MessageHistoryMemory.attach(w, device);
    device.capacity.protect(w, () sync* {
      final user = w.client.user?.id, server = w.client.serverId;
      if (user == null || server == null || !w.cacheWritable) return;
      final account = HistoryAccount(w.client.origin, user);
      for (final id in {w.channel?.id, w.threadChannelId}.nonNulls) {
        yield HistoryProtectedRows(account, server, id,
          w.visibleIds[id] ?? const <String>{},
          wholeChannel: w.loadingOlder || (id == w.channel?.id ? w.channelLoading : w.threadLoading));
      }
      for (final sends in w.composer.sends.pending.values) {
        for (final send in sends) {
          final id = send.channelId ?? send.row.channelId;
          yield HistoryProtectedRows(account, server, id, [send.row.id]);
        }
      }
    });
  }

  static void detach(WorkspaceController w) {
    HistorySnapshotWrites.cancel(w);
    final device = storage(w.cache);
    device?.receiver.cancelUpdates(w);
    device?.reads.forget(w);
    device?.capacity.release(w);
    device?.memory.unregister(w);
  }
  static void recheckContext(
    WorkspaceController w,
    MessageWindowRequest request,
    String id,
    String target,
  ) => MessageHistoryVerification.recheckContext(w, request, id, target);

  static Future<Map<String, dynamic>> context(
    WorkspaceController w,
    MessageWindowRequest request,
    String id,
    String target, {
    bool thread = false,
  }) => load(
    cache: w.cache,
    scope: scope(
      w,
      id,
      parent: thread ? w.threadParentChannelId : id,
      parentMessage: thread ? w.threadIdentity?.parentMessageId : null,
    ),
    current: () => request.current && w.cacheWritable,
    held: () => w.ledger.messages(id),
    ticket: w.messageHistoryTicket(thread ? w.threadParentChannelId ?? id : id),
    focused: true,
    target: target,
    remote: () async => Map<String, dynamic>.from(
      await w.client.get('/messages/context/$target', query: {'channelId': id}),
    ),
  );
  static Future<Map<String, dynamic>> read(
    WorkspaceController w,
    MessageWindowRequest request,
    String id, {
    bool thread = false,
    BigInt? before,
    BigInt? after,
    int limit = 50,
    bool localFirst = true,
    bool gap = false,
    Future<Map<String, dynamic>> Function()? remote,
  }) {
    return load(
      cache: w.cache,
      scope: scope(
        w,
        id,
        parent: thread ? w.threadParentChannelId : id,
        parentMessage: thread ? w.threadIdentity?.parentMessageId : null,
      ),
      current: () => request.current && w.cacheWritable,
      held: () => w.ledger.messages(id),
      remote:
          remote ??
          () => w.client.messagePage(
            id,
            before: before,
            after: after,
            limit: limit,
          ),
      before: before,
      after: after,
      limit: limit,
      localFirst: localFirst,
      ticket: w.messageHistoryTicket(
        thread ? w.threadParentChannelId ?? id : id,
      ),
      reason: gap || remote != null ? HistoryReadReason.gap : null,
    );
  }

  static MessageHistoryScope? scope(
    WorkspaceController w,
    String id, {
    String? parent,
    String? parentMessage,
    RaftChannel? resolved,
  }) {
    parent ??= id;
    final resource = [
      ?resolved,
      if (w.channel != null) w.channel!,
      ...w.channels,
      ...w.dms,
    ].where((c) => c.id == parent).firstOrNull;
    return w.client.user == null ||
            w.client.serverId == null ||
            resource == null ||
            !w.can('viewChannel', resource: resource)
        ? null
        : MessageHistoryScope(
            origin: w.client.origin,
            principal: w.client.user!.id,
            server: w.client.serverId!,
            channel: id,
            parentChannel: parent,
            parentMessage: parentMessage,
            authority: messageWindowAuthority(
              w.server?.string('role'),
              resource.json,
            ),
          );
  }

  static Future<bool> record(
    WorkspaceController w,
    MessageWindowRequest request,
    Map<String, dynamic> page,
  ) async {
    if (page is! MessageHistoryPage) return true;
    if (!request.current) return false;
    final device = storage(w.cache);
    if (device == null) return true;
    final id = page.scope.channel;
    final initial = {for (final row in w.ledger.messages(id)) row['id']: row};
    try {
      if (!page.local) await device.acceptPage(page);
      if (!request.current || !w.cacheWritable) return false;
      // A stored edit can arrive after load reconciled but before admission.
      // Publish that projection only while no later in-memory event owns it.
      final merged = await device.reconcilePage(
        page.scope,
        page,
        requestedRevision: page.revision,
        before: page.before,
        after: page.after,
        focused: page.focused,
      );
      if (!request.current || !w.cacheWritable) return false;
      final current = {for (final row in w.ledger.messages(id)) row['id']: row};
      final accepted = <Map<String, dynamic>>[];
      final ids = <String>{};
      for (final row in merged['messages'] as List) {
        final key = row['id'] as String;
        ids.add(key);
        final pending = device.receiver.pendingUpdate(page.scope, key);
        if (identical(initial[key], current[key]) &&
            (!device.receiver.hasRow(page.scope, key) || pending != null)) {
          accepted.add(pending == null ? Map<String, dynamic>.from(row as Map) : mergeMessageProjection(Map<String, dynamic>.from(row as Map), pending));
        }
      }
      for (final row in page['messages'] as List) {
        final key = row['id'] as String;
        if (!ids.contains(key) &&
            identical(initial[key], current[key]) &&
            !device.receiver.hasRow(page.scope, key)) {
          w.ledger.remove(id, key);
          w.visibleIds[id]?.remove(key);
        }
      }
      w.messageWindows.acceptRows(request, id, accepted, authoritative: true);
      w.messageSync.replaceBodies(id, accepted);
      if (!page.local &&
          !page.focused &&
          page.before == null &&
          page.after == null) {
        final seq = (page.remotePage?['messages'] as List? ?? const []).fold(
          BigInt.zero,
          (BigInt latest, dynamic row) {
            final seq = BigInt.tryParse('${row['seq']}') ?? BigInt.zero;
            return seq > latest ? seq : latest;
          },
        );
        await device.checkpoint(page.scope, seq, initializeOnly: true);
        MessageHistoryVerification.retained(w, request, page);
      }
      if (page.local) MessageHistoryVerification.schedule(w, request, page);
      return true;
    } catch (cause) {
      if (request.current && w.cacheWritable) {
        if (cause is HistoryReadRetired) {
          // The receiver can have admitted this page before its write fence.
          // Remove only the unchanged page projections; later live edits own
          // their bodies and must survive this retired response.
          for (final row in page['messages'] as List) {
            final key = row['id'] as String;
            if (identical(initial[key], w.ledger.message(id, key)) &&
                !device.receiver.hasRow(page.scope, key)) {
              w.ledger.remove(id, key);
              w.visibleIds[id]?.remove(key);
            }
          }
        }
        w.error ??= saveError(cause);
      }
      return false;
    }
  }

  static MessageHistoryStorage? storage(WorkspaceCache? cache) =>
      cache is MessageHistoryProvider &&
          (cache as MessageHistoryProvider).messageHistoryEnabled
      ? (cache as MessageHistoryProvider).messageHistory
      : null;

  static List<Map<String, dynamic>> snapshotRows(
    WorkspaceCache? cache,
    List<RaftMessage> rows,
  ) => [
    for (final row
        in storage(cache) != null
            ? rows
            : rows.skip(
                (rows.length - messageWindowLimit).clamp(0, rows.length),
              ))
      row.json,
  ];

  static Future<Map<String, dynamic>> load({
    required WorkspaceCache? cache,
    required MessageHistoryScope? scope,
    required bool Function() current,
    required List<Map<String, dynamic>> Function() held,
    required Future<Map<String, dynamic>> Function() remote,
    BigInt? before,
    BigInt? after,
    int limit = 50,
    bool localFirst = true,
    bool focused = false,
    String? target,
    Object? ticket,
    HistoryReadReason? reason,
    int retiredAttempts = 0,
  }) async {
    final device = storage(cache);
    if (device == null || scope == null) return remote();
    final queryKey = historyQueryKey(
      before: before,
      after: after,
      limit: limit,
      target: target,
    );
    Future<Map<String, dynamic>> wire() => device.reads.fetch(
      historyReadKey(scope, ticket ?? scope.authority, queryKey),
      reason ??
          (before != null || after != null || focused
              ? HistoryReadReason.missing
              : HistoryReadReason.tail),
      remote,
    );
    Future<DateTime?> verified(DateTime? fallback) async {
      try {
        return await device.lastVerified(scope, queryKey) ?? fallback;
      } catch (_) {
        return fallback;
      }
    }

    final initial = {for (final row in held()) row['id']: row};
    bool pending(String id) => device.receiver.hasRow(scope, id);
    var revision = 0;
    try {
      revision = await device.revision(scope);
      if (localFirst && focused && target != null) {
        final local = await device.readContext(scope, target);
        if (local != null) {
          device.reads.count(HistoryReadReason.local);
          return MessageHistoryPage(
            _live(local, initial, held(), focused: true, pending: pending),
            scope: scope,
            revision: revision,
            before: null,
            after: null,
            local: true,
            focused: true,
            target: target,
            verifiedAt: await verified(
              local is MessageHistoryPage ? local.verifiedAt : null,
            ),
          );
        }
      }
      if (localFirst && (before != null || after != null)) {
        final local = await device.readPage(
          scope,
          before: before,
          after: after,
          limit: limit,
        );
        if (!current()) return local?.page ?? const {'messages': <dynamic>[]};
        if (local != null) {
          device.reads.count(HistoryReadReason.local);
          return MessageHistoryPage(
            _live(
              local.page,
              initial,
              held(),
              before: before,
              after: after,
              pending: pending,
            ),
            scope: scope,
            revision: revision,
            before: before,
            after: after,
            local: true,
            limit: limit,
            verifiedAt: await verified(local.verifiedAt),
          );
        }
      }
      if (localFirst && reason == null &&
          await device.wasEvicted(scope, before: before, after: after, target: target)) {
        reason = HistoryReadReason.evicted;
      }
    } catch (_) {
      // A full/unavailable device never prevents a successful remote read.
      return current() ? wire() : const {'messages': <dynamic>[]};
    }
    if (!current()) return const {'messages': <dynamic>[]};
    final raw = await wire();
    if (!current()) return raw;
    final input = raw['messages'];
    if (input is! List ||
        acceptedWindowRows(input, scope.channel).length != input.length) {
      return raw;
    }
    Map<String, dynamic> merged;
    try {
      merged = await device.reconcilePage(
        scope,
        raw,
        requestedRevision: revision,
        before: before,
        after: after,
        focused: focused,
      );
    } on HistoryReadRetired {
      if (retiredAttempts >= 2 || !current()) rethrow;
      return load(cache: cache, scope: scope, current: current, held: held,
        remote: remote, before: before, after: after, limit: limit,
        localFirst: false, focused: focused, target: target, ticket: ticket,
        reason: HistoryReadReason.evicted, retiredAttempts: retiredAttempts + 1);
    } catch (_) {
      merged = raw;
    }
    return MessageHistoryPage(
      _live(
        merged,
        initial,
        held(),
        before: before,
        after: after,
        focused: focused,
        pending: pending,
      ),
      scope: scope,
      revision: revision,
      before: before,
      after: after,
      local: false,
      remotePage: raw,
      limit: limit,
      focused: focused,
      target: target,
    );
  }

  static Map<String, dynamic> _live(
    Map<String, dynamic> page,
    Map<dynamic, Map<String, dynamic>> initial,
    List<Map<String, dynamic>> held, {
    BigInt? before,
    BigInt? after,
    bool focused = false,
    bool Function(String id)? pending,
  }) {
    final current = {for (final row in held) row['id']: row};
    final fresh = <String, Map<String, dynamic>>{
      for (final row in page['messages'] as List)
        row['id'] as String: Map<String, dynamic>.from(row as Map),
    };
    final seqs = fresh.values.map((m) => BigInt.parse('${m['seq']}')).toList()
      ..sort();
    final lower = after == null
        ? (seqs.isEmpty ? BigInt.zero : seqs.first)
        : after + BigInt.one;
    final upper = before != null
        ? before - BigInt.one
        : focused
        ? seqs.isEmpty
              ? BigInt.zero
              : seqs.last
        : historyMaxSequence;
    for (final row in held) {
      if (identical(initial[row['id']], row) &&
          pending?.call(row['id'] as String) != true) {
        continue;
      }
      final seq = BigInt.tryParse('${row['seq']}');
      if (seq != null && seq >= lower && seq <= upper) {
        fresh[row['id'] as String] = row;
      }
    }
    for (final id in initial.keys) {
      if (!current.containsKey(id)) fresh.remove(id);
    }
    final rows = fresh.values.toList()
      ..sort(
        (a, b) =>
            BigInt.parse('${a['seq']}').compareTo(BigInt.parse('${b['seq']}')),
      );
    return {...page, 'messages': rows};
  }

  static Future<void> commit(
    WorkspaceCache? cache,
    Map<String, dynamic> page, {
    required bool Function() current,
    required void Function() failed,
  }) async {
    if (page is! MessageHistoryPage || page.local || !current()) return;
    try {
      await storage(cache)?.acceptPage(page);
    } catch (_) {
      if (current()) failed();
    }
  }
}
