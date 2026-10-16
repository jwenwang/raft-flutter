import 'history_memory.dart';
import 'message_history.dart';
import 'message_history_pages.dart';
import 'workspace_controller.dart';
import 'workspace_message_windows.dart';

/// Adapter for existing body owners. The pool stores no message authority.
class MessageHistoryMemory {
  static void sample(
    WorkspaceController w,
    Object owner,
    void Function() capture,
  ) =>
      MessageHistoryPages.storage(w.cache)?.memory
          .samplePosition(owner, capture);
  static void cancelSample(WorkspaceController w, Object owner) =>
      MessageHistoryPages.storage(w.cache)?.memory.cancelSample(owner);
  static void attach(WorkspaceController w, MessageHistoryStorage storage) {
    if (w.presentationSource != null) return;
    storage.memory.register(
      w,
      HistoryMemoryPort(
        changes: w,
        selected: () => w.foreground
            ? {w.channel?.id, w.threadChannelId}.nonNulls
            : const [],
        stamp: () => (
          w.ledger.revision,
          w.client.origin,
          w.client.user?.id,
          w.client.serverId,
          w.channel?.id,
          w.threadChannelId,
          w.foreground,
          w.loadingOlder,
          w.channelLoading,
          w.threadLoading,
          w.messageWindows.accepted[w.channel?.id],
          w.messageWindows.accepted[w.threadChannelId],
          w.cacheWritable,
          w.highlightedMessageId,
        ),
        capture: () => capture(w, storage),
      ),
    );
  }

  static Future<List<HistoryMemoryWindow>> capture(
    WorkspaceController w,
    MessageHistoryStorage storage,
  ) async {
    final result = <HistoryMemoryWindow>[];
    final generation = w.ledger.generation,
        origin = w.client.origin,
        principal = w.client.user?.id,
        server = w.client.serverId,
        seal = w.messageHistoryTicket(''),
        main = w.channel?.id,
        thread = w.threadChannelId;
    bool current() =>
        w.cacheWritable &&
        generation == w.ledger.generation &&
        origin == w.client.origin &&
        principal == w.client.user?.id &&
        server == w.client.serverId &&
        seal == w.messageHistoryTicket('') &&
        main == w.channel?.id &&
        thread == w.threadChannelId;
    if (principal == null || server == null || !current()) return result;
    for (final id in {
      ...w.ledger.channelIds,
      for (final snapshot in w.messageWindows.retainedSnapshots) ...[
        ?snapshot.channelId,
        ?snapshot.parent?.channelId,
      ],
      for (final thread in w.messageWindows.threads.values)
        thread.parentChannelId,
      ...w.messageSync.channelIds,
    }) {
      if (!current()) break;
      if (!w.messageWindows.notRevoked(id)) continue;
      final heldThread = w.messageWindows.threads.values
          .where((held) => held.threadChannelId == id)
          .firstOrNull;
      var scope = MessageHistoryPages.scope(
        w,
        id,
        parent: id == thread
            ? w.threadParentChannelId
            : heldThread?.parentChannelId ?? id,
        parentMessage: id == thread ? w.threadParentMessageId : null,
      );
      if (scope == null) {
        final known = await storage.knownScope(origin, principal, server, id);
        if (!current()) return [];
        if (known != null) {
          scope = MessageHistoryPages.scope(
            w,
            id,
            parent: known.parentChannel,
            parentMessage: known.parentMessage,
          );
        }
      }
      if (scope == null || !current()) continue;
      final ticket = w.messageHistoryTicket(scope.parentChannel);
      final versions = _versions(w, id);
      if (versions.isEmpty) continue;
      final active = w.foreground && (id == main || id == thread);
      final anchor = w.messageWindows.readingAnchor(id);
      var anchorId = anchor == null
          ? null
          : versions.entries
                .where(
                  (entry) =>
                      w.messageKey(
                        w.messageWindows.presented(entry.value.first),
                      ) ==
                      anchor.messageKey,
                )
                .firstOrNull
                ?.key;
      final membership = w.messageWindows.accepted[id];
      final displayed = w.messageWindows.rows(id);
      // Restored contexts may coexist with a cached tail before a view reports
      // geometry. An absent/stale view anchor cannot select that hidden tail.
      if (active &&
          displayed.isNotEmpty &&
          displayed.length <= storage.memory.activeLimit &&
          !displayed.any((row) => row.id == anchorId)) {
        anchorId = displayed[displayed.length ~/ 2].id;
      }
      final highlighted = w.highlightedMessageId;
      final protected = <String>{
        ?highlighted,
        if (w.threadParent?.channelId == id) w.threadParent!.id,
        for (final sends in w.composer.sends.pending.values)
          for (final send in sends)
            if ((send.channelId ?? send.row.channelId) == id) send.row.id,
      };
      bool admitted() =>
          current() &&
          w.messageWindows.notRevoked(id) &&
          identical(anchor, w.messageWindows.readingAnchor(id)) &&
          identical(membership, w.messageWindows.accepted[id]) &&
          ticket == w.messageHistoryTicket(scope!.parentChannel);
      result.add(
        HistoryMemoryWindow(
          scope: scope,
          versions: versions,
          active: active,
          paused:
              active &&
              (w.loadingOlder ||
                  (id == main ? w.channelLoading : w.threadLoading)),
          anchor: anchorId,
          protectedIds: protected,
          current: admitted,
          release: (ids) {
            if (!admitted()) return {};
            final fresh = _versions(w, id);
            final released = ids
                .where(
                  (key) =>
                      fresh[key]?.every(
                        (row) =>
                            versions[key]?.any(
                              (held) => identical(held, row),
                            ) ==
                            true,
                      ) ==
                      true,
                )
                .toSet();
            if (released.isEmpty) return released;
            final boundary = _releaseBodies(w, id, released);
            w.messageSync.releaseBodies(id, released);
            if (id == main) {
              w.hasMore = w.hasMore || boundary.older;
              w.hasNewer = w.hasNewer || boundary.newer;
            }
            if (id == thread) {
              w.threadHasMore = w.threadHasMore || boundary.older;
            }
            w.notifyListeners();
            return released;
          },
        ),
      );
    }
    return result;
  }

  static Map<String, List<Map<String, dynamic>>> _versions(
    WorkspaceController w,
    String id,
  ) {
    final result = <String, List<Map<String, dynamic>>>{};
    void add(Map<String, dynamic> row) {
      final key = row['id'];
      if (row['channelId'] != id || key is! String) return;
      final copies = result.putIfAbsent(key, () => []);
      if (!copies.any((copy) => identical(copy, row))) copies.add(row);
    }

    for (final row in w.ledger.messages(id)) {
      add(row);
    }
    for (final snapshot in w.messageWindows.retainedSnapshots) {
      for (final row in [...snapshot.rows, ...snapshot.timeline]) {
        add(row.json);
      }
    }
    for (final thread in w.messageWindows.threads.values) {
      if (thread.parent case final parent?) add(parent.json);
    }
    for (final row in w.messageSync.messages(id)) {
      final copies = result.putIfAbsent(row['id'] as String, () => []);
      if (!copies.any((copy) => identical(copy, row))) copies.add(row);
    }
    return result;
  }

  static ({bool older, bool newer}) _releaseBodies(
    WorkspaceController w,
    String id,
    Set<String> ids,
  ) {
    final owner = w.messageWindows, visible = w.messageWindows.rows(id);
    final kept = visible.where((row) => !ids.contains(row.id)).toList();
    final older = visible.any(
      (row) =>
          ids.contains(row.id) && (kept.isEmpty || row.seq < kept.first.seq),
    );
    final newer = visible.any(
      (row) =>
          ids.contains(row.id) && (kept.isEmpty || row.seq > kept.last.seq),
    );
    final accepted = owner.accepted[id];
    if (accepted != null) {
      owner.rememberWindow(
        liveWindowWrite,
        id,
        accepted.authority,
        older || accepted.hasMore,
        accepted.token,
        historyLimited: accepted.historyLimited,
      );
      owner.setContextMode(
        liveWindowWrite,
        id,
        newer ? true : accepted.contextNewer,
      );
    }
    w.ledger.releaseBodies(id, ids);
    owner.visibleIds[id]?.removeAll(ids);
    for (final entry in owner.threads.entries.toList()) {
      final held = entry.value;
      var next = held;
      if (held.parentChannelId == id && ids.contains(held.parent?.id)) {
        next = next.withParent(null);
      }
      if (held.threadChannelId == id && (older || newer)) {
        next = next.withBoundary(
          more: older || held.hasMore,
          context: newer || held.contextWindow,
        );
      }
      if (!identical(held, next)) {
        owner.cacheThread(liveWindowWrite, entry.key, next);
      }
    }
    return (older: older, newer: newer);
  }
}
