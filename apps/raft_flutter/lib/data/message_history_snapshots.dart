import 'message_history_pages.dart';
import 'history_snapshot_writes.dart';
import 'message_window_snapshot.dart';
import 'workspace_controller.dart';
import 'workspace_message_windows.dart';

/// Compatibility snapshot adapter. Durable rows/ranges are owned separately;
/// this keeps the previous version's rollback format and window boundaries.
class MessageHistorySnapshots {
  static Future<void> flush(WorkspaceController w) {
    while (w.presentationSource != null) {
      w = w.presentationSource!;
    }
    return HistorySnapshotWrites.flush(w);
  }

  static bool restore(
    WorkspaceController w,
    String id,
    dynamic page,
    MessageWindowRequest request, {
    required bool replace,
    required bool Function() allowed,
  }) {
    final authority = messageWindowAuthority(
      w.server?.string('role'),
      w.channel?.json ?? const {},
    );
    if (!request.current ||
        page is! Map ||
        page['version'] != 1 ||
        page['windowKind'] != 'tail' ||
        page['hasNewer'] == true ||
        page['authority'] != authority ||
        !w.can('viewChannel', resource: w.channel) ||
        !w.messageWindows.notRevoked(id) ||
        !allowed() ||
        page['parentChannelId'] != w.channel?.id) {
      return false;
    }
    w.hasMore = page['hasMore'] == true;
    w.historyLimited = page['historyLimited'] == true;
    if (replace) w.hasNewer = page['hasNewer'] == true;
    w.messageWindows.rememberWindow(
      request,
      id,
      authority,
      w.hasMore,
      w.messageHistoryTicket(id) as String,
      historyLimited: w.historyLimited,
      rows: acceptedWindowRows(page['messages'], id),
      replace: replace,
    );
    w.messageWindows.setDiskOnly(request, id, true);
    w.messageWindows.setContextMode(request, id, null);
    return true;
  }

  static Future<void> save(
    WorkspaceController w,
    String id, {
    required bool thread,
    bool coalesce = false,
    required WorkspaceThreadIdentity? identity,
    required String Function(String?) replyToken,
    required Future<void> Function(String, String, dynamic) write,
  }) {
    if (!w.cacheWritable ||
        w.channelLoading && w.messageWindows.isContext(id) ||
        !thread && w.channel == null ||
        (thread ? id != w.threadChannelId : id != w.channel!.id) ||
        !w.messageWindows.notRevoked(id) ||
        (thread ? w.messageWindows.contextHasNewer(id) : w.hasNewer) ||
        w.highlightedMessageId != null) {
      return Future<void>.value();
    }
    final resource = thread ? w.threadSourceChannel : w.channel;
    if (resource == null || !w.can('viewChannel', resource: resource)) {
      return Future<void>.value();
    }
    final authority = messageWindowAuthority(
      w.server?.string('role'),
      resource.json,
    );
    w.messageWindows.rememberWindow(
      liveWindowWrite,
      id,
      authority,
      thread ? w.threadHasMore : w.hasMore,
      replyToken(thread ? w.threadParentChannelId : id),
      historyLimited: thread ? w.threadHistoryLimited : w.historyLimited,
    );
    w.messageWindows.setContextMode(liveWindowWrite, id, null);
    final rows = thread ? w.replies : w.messages;
    final payload = {
      'version': 1,
      'windowKind': 'tail',
      'authority': authority,
      'parentChannelId': thread ? w.threadParentChannelId : id,
      'messages': MessageHistoryPages.snapshotRows(w.cache, rows),
      'hasMore': thread ? w.threadHasMore : w.hasMore,
      'historyLimited': thread ? w.threadHistoryLimited : w.historyLimited,
    };
    final window = {
      ...payload,
      'hasNewer': false,
      'threadSummaries': thread
          ? {}
          : Map<String, dynamic>.from(w.threadSummaries),
    };
    final parent = w.threadParent;
    final threadPayload =
        thread &&
            identity != null &&
            identity.focusedMessageId == null &&
            w.messageWindows.notRevoked(identity.parentChannelId) &&
            w.messageWindows.notRevoked(id)
        ? {
            ...payload,
            'parentChannelId': identity.parentChannelId,
            'threadChannelId': id,
            if (parent != null &&
                parent.id == identity.parentMessageId &&
                parent.channelId == identity.parentChannelId)
              'parent': parent.json,
          }
        : null;
    final parentId = thread ? w.threadParentChannelId ?? id : id;
    final scope = (
      w.client.origin,
      w.client.user?.id,
      w.client.serverId,
      w.ledger.generation,
      replyToken(parentId),
    );
    bool current() =>
        w.cacheWritable &&
        scope ==
            (
              w.client.origin,
              w.client.user?.id,
              w.client.serverId,
              w.ledger.generation,
              replyToken(parentId),
            ) &&
        w.messageWindows.notRevoked(id) &&
        w.messageWindows.notRevoked(parentId);
    Future<void> writeCopies() async {
      if (!current()) return;
      final saved = write('window', id, window);
      if (threadPayload != null) {
        await write('thread-window', identity!.parentMessageId, threadPayload);
      }
      await saved;
    }

    return coalesce && MessageHistoryPages.storage(w.cache) != null
        ? HistorySnapshotWrites.schedule(w, (scope, id), writeCopies)
        : HistorySnapshotWrites.writeNow(w, (scope, id), writeCopies);
  }
}
