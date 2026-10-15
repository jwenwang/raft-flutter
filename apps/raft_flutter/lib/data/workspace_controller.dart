import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:raft_client/raft_client.dart';
import 'package:raft_sync/raft_sync.dart';

import 'burst_reads.dart';
import 'announcement_store.dart';
import 'cause_notice.dart';
import 'message_task_cache.dart';
import 'message_translation_store.dart';
import 'thread_agent_followers.dart';
import 'thread_search.dart';
import 'panel_caches.dart';
import 'source_channel_files_store.dart';
import 'source_activity_unread_store.dart';
import 'reaction_toggle.dart';
import 'workspace_cache.dart';
import 'workspace_composer.dart';
import 'workspace_uploads.dart';
import 'workspace_sends.dart';
import 'conversation_window.dart';
import 'workspace_message_windows.dart';

export 'workspace_uploads.dart' show UploadDraft;
export 'conversation_window.dart' show MessageContextLoadError;
export 'workspace_message_windows.dart' show ReadingAnchor;
import 'resource_snapshot_cache.dart';
import 'user_activity.dart';
import 'user_error.dart';
import 'workspace_entity_directory.dart';
import 'followed_threads_store.dart';
import 'raft_location.dart';
import 'raft_navigation_history.dart';
import 'navigation_registry.dart';
import 'workspace_navigation.dart';
import 'shared_draft_attachment.dart';
import 'message_window_snapshot.dart';
import 'message_history_pages.dart';
import 'message_history_events.dart';
import 'message_history_snapshots.dart';
import 'message_history_navigation.dart';
import 'message_history_positions.dart';
import 'thread_window_snapshot.dart';
import 'attachment_image_repository.dart';
import 'cache_clear.dart';
import 'attachment_url_cache.dart';
import '../platform/memory_budgets.dart';
import '../platform/memory_release.dart';
import '../platform/attachment_player_sessions.dart';
import 'upload_mime.dart';
import 'feature_flag_client_facts.dart';

class WorkspaceAttachmentImageLease {
  WorkspaceAttachmentImageLease(this.lease, this._releaseAuthority);
  final AttachmentImageLease lease;
  final VoidCallback _releaseAuthority;
  bool _released = false;
  void release({bool discard = false}) {
    if (_released) return;
    _released = true;
    discard ? lease.discard() : lease.release();
    _releaseAuthority();
  }
}

/// Real URI identity, independent of the asynchronously accepted parent record.
@immutable
class WorkspaceThreadIdentity {
  const WorkspaceThreadIdentity({
    required this.parentChannelId,
    required this.parentMessageId,
    this.focusedMessageId,
  });
  final String parentChannelId, parentMessageId;
  final String? focusedMessageId;
}

/// Web `isThreadAbsentLookupError` (threadStore.ts): only the route's explicit
/// "no thread for this message" 404 means an empty thread. A 404 carrying a
/// `code` is absent only for `THREAD_NOT_FOUND`; without a `code` the legacy
/// error text decides. Every other failure (a lost parent channel is also a
/// 404) is a load error with Retry.
bool isThreadAbsentLookupError(Object? error) {
  if (error is! RaftApiException || error.status != 404) return false;
  if (error.details.containsKey('code')) {
    return error.details['code'] == 'THREAD_NOT_FOUND';
  }
  return error.message == 'No thread found for this message';
}

class WorkspaceController extends ChangeNotifier {
  void synchronizePagePresentation(
    WorkspaceController source, {
    required bool active,
  }) {
    final reactivated = active && !presentationActive;
    presentationActive = active;
    if (reactivated) {
      if (_presentationOwner case final owner?) {
        source.setConversationPresentation(
          owner,
          main: _mainPresented,
          thread: _threadPresented,
        );
      }
      if (_tabPresentationOwner case final owner?) {
        source.setChatTabPresentation(owner, _chatTabPresented);
      }
    }
    server = source.server;
    servers = source.servers;
    channels = source.channels;
    dms = source.dms;
    unread = source.unread;
    mentions = source.mentions;
    newFlags = source.newFlags;
    sidebarOrder = source.sidebarOrder;
    connected = source.connected;
    foreground = active && source.foreground;
    syncCoreMessagesEnabled = source.syncCoreMessagesEnabled;
    remoteComputerUpgradeEnabled = source.remoteComputerUpgradeEnabled;
    agentMigrationEnabled = source.agentMigrationEnabled;
    runtimeFormV2WebEnabled = source.runtimeFormV2WebEnabled;
    savedRevision = source.savedRevision;
    planRevision = source.planRevision;
    syncCoreNotificationPrefsEnabled = source.syncCoreNotificationPrefsEnabled;
    _revokedChannels
      ..clear()
      ..addAll(source._revokedChannels);
    _revokedServers
      ..clear()
      ..addAll(source._revokedServers);
    if (!active) return;
    _missingConversationChannelId = source._missingConversationChannelId;
    // Back reveals the retained page before the source has hydrated its new
    // selection. Do not replace that page's channel with the outgoing one.
    final target = location;
    final requestedChannel = switch (target.route) {
      RaftRoute.channel || RaftRoute.dm => target.entityId,
      _ => null,
    };
    if (requestedChannel != null && source.channel?.id != requestedChannel) {
      if (_missingConversationChannelId == requestedChannel ||
          _revokedChannels.contains(requestedChannel)) {
        channel = null;
        channelLoading = false;
        return;
      }
      if (channel?.id != requestedChannel) {
        channel = [
          ...source.channels,
          ...source.dms,
        ].where((record) => record.id == requestedChannel).firstOrNull;
        channelLoading =
            source.visibleIds[requestedChannel]?.isNotEmpty != true;
      }
      return;
    }
    channel = source.channel;
    hasMore = source.hasMore;
    hasNewer = source.hasNewer;
    historyLimited = source.historyLimited;
    highlightedMessageId = source.highlightedMessageId;
    contextLoadError = source.contextLoadError;
    channelLoading = source.channelLoading;
    loadingOlder = source.loadingOlder;
    loading = source.loading;
    error = source.error;
    threadParent = source.threadParent;
    threadChannelId = source.threadChannelId;
    threadHasMore = source.threadHasMore;
    threadHistoryLimited = source.threadHistoryLimited;
    threadLoading = source.threadLoading;
    _threadIdentity = source._threadIdentity;
    _threadVisit = source._threadVisit;
    _threadResolutionLoading = source._threadResolutionLoading;
    _threadParentLoading = source._threadParentLoading;
    _threadResolutionError = source._threadResolutionError;
  }

  /// High-frequency socket events that never change workspace state.
  static const presenceEvents = {
    'agent:activity',
    'agent:seen',
    'agent:session',
    'machine:status',
    'machine:capabilities',
  };

  WorkspaceController(
    this.client, {
    this.cache,
    this.presentationSource,
    this.mobileNavigation = false,
    this.ownsClient = true,
    this.restoreSharedAttachment,
    this.discardSharedScope,
    WorkspaceEntityDirectory? entityDirectory,
    FollowedThreadsStore? followedThreads,
    AttachmentImageByteStore? attachmentImageStore,
    AttachmentUrlCache? attachmentUrls,
    WorkspaceComposer? composer,
  }) : _attachmentUrls = attachmentUrls ?? AttachmentUrlCache(),
       _presentationAuthority = presentationSource?.navigationAuthority,
       attachmentImageStore =
           attachmentImageStore ?? AttachmentDiskCache.installed {
    this.composer =
        composer ??
        presentationSource?.composer ??
        WorkspaceComposer(
          owner: this,
          read: _cached,
          write: _save,
          authority: () => (
            client.origin,
            client.generation,
            client.user?.id,
            client.serverId,
            server?.string('role'),
            jsonEncode(server?.json['permissions']),
          ),
        );
    if (presentationSource == null) {
      _removePlayerTrim = _playerSessions.installMemoryTrim();
    }
    MessageHistoryPages.attach(this);
    _ownsFollowedThreads = followedThreads == null;
    this.followedThreads =
        followedThreads ??
        FollowedThreadsStore(
          scope: _entityScope,
          query: (path) => query(path),
          command: (path, data) => command('POST', path, data: data),
          authority: this,
          events: client.events,
          isOpen: (id) => threadChannelId == id,
        );
    _ownsEntityDirectory = entityDirectory == null;
    this.entityDirectory =
        entityDirectory ??
        WorkspaceEntityDirectory(
          scope: _entityScope,
          query: (path) => query(path),
          events: client.events,
          readDevice: cache == null ? null : _readEntities,
          writeDevice: cache == null ? null : _writeEntities,
        );
    // Directory changes are not forwarded to this notifier: agent heartbeats,
    // last-seen and machine status patches would rebuild every workspace
    // listener. Surfaces that render entities listen to [entityDirectory]
    // (or a [WorkspaceEntitySelection] of it) directly.
    if (_ownsEntityDirectory) addListener(_entityAuthorityChanged);
    if (presentationSource == null) {
      this.composer.sends.attach(this, _sendChanged);
    }
    subscription = presentationSource == null
        ? client.events.listen(_event)
        : const Stream<RaftEvent>.empty().listen((_) {});
    // A route-local presentation shares its source's layers, which register.
    if (presentationSource == null) {
      _removeCacheClear = CacheClear.instance.register(
        'workspace-memory',
        clearRebuildableMemory,
      );
    }
  }

  VoidCallback? _removeCacheClear;
  late final WorkspaceComposer composer;
  late final WorkspaceMessageWindows messageWindows =
      presentationSource?.messageWindows ??
      WorkspaceMessageWindows(
        authority: _messageWindowAuthority,
        presentation: _messageWindowPresentation,
        layout: _messageWindowLayout,
        sends: composer.sends,
        notRevoked: (id) => !_revokedChannels.contains(id),
      );

  WorkspaceController get _windowReceiver =>
      presentationSource?._windowReceiver ?? this;

  ConversationWindow? get messageWindow => messageWindows.channel(channel?.id);
  ConversationWindow? get threadWindow =>
      messageWindows.thread(threadParentChannelId, threadParentMessageId);

  MessageWindowAuthority? _messageWindowAuthority(
    ConversationWindowScope scope,
  ) {
    final resource = _threadChannelResource(scope.channelId);
    if (_disposed ||
        client.user == null ||
        server == null ||
        server!.id != client.serverId ||
        _revokedServers.contains(server!.id) ||
        _revokedChannels.contains(scope.channelId) ||
        resource == null ||
        !can('viewChannel', resource: resource)) {
      return null;
    }
    final accepted = messageWindows.accepted[scope.channelId];
    final authority = messageWindowAuthority(
      server!.string('role'),
      resource.json,
    );
    if (accepted != null &&
        !accepted.matches(authority, _replyToken(scope.channelId))) {
      return null;
    }
    return (
      navigationAuthority,
      ledger.generation,
      _replyToken(scope.channelId),
      authority,
      jsonEncode(server!.json['permissions']),
    );
  }

  (bool, bool) _messageWindowLayout(ConversationWindowScope scope) {
    final selected = scope.parentMessageId == null
        ? channel?.id == scope.channelId
        : threadParentChannelId == scope.channelId &&
              threadParentMessageId == scope.parentMessageId;
    return (
      selected,
      selected &&
          foreground &&
          _chatTabPresented &&
          (scope.parentMessageId == null ? _mainPresented : _threadPresented),
    );
  }

  ConversationWindowSnapshot _messageWindowPresentation(bool thread) {
    final rows = thread ? replies : messages;
    return ConversationWindowSnapshot(
      channelId: thread ? threadChannelId : channel?.id,
      rows: rows,
      timeline: timeline(thread: thread, rows: rows),
      parent: thread ? presentedThreadParent : null,
      loading: thread ? threadLoading : channelLoading,
      loadingOlder: loadingOlder,
      parentLoading: thread && threadParentLoading,
      resolving: thread && threadResolutionLoading,
      error: thread ? threadResolutionError : error,
      hasMore: thread ? threadHasMore : hasMore,
      hasNewer: thread ? messageWindows.contextHasNewer(threadChannelId) : hasNewer,
      historyLimited: thread ? threadHistoryLimited : historyLimited,
      highlightedMessageId: highlightedMessageId,
      contextLoadError: thread ? null : contextLoadError,
    );
  }

  /// "Clear cache": lets go of what this workspace keeps in memory and can
  /// rebuild: decoded attachment pictures nobody shows, signed attachment URLs
  /// already resolved, and the kept still pictures of videos. Pictures a
  /// widget holds, requests in flight and playing or held players stay.
  @visibleForTesting
  void clearRebuildableMemory() {
    _attachmentImages?.trimIdle();
    _attachmentUrls.dropResolved();
    _playerSessions.trimStills();
    _playerSessions.forgetFrames();
  }

  /// Borrowed editor controllers project messages but never own the session,
  /// membership recovery, server selection, socket resume, or client teardown.
  final bool ownsClient;
  final RaftClient client;
  final WorkspaceCache? cache;

  /// Route-local presentation borrows the session and accepted stores. Only
  /// the source reduces socket events and owns server/bootstrap lifecycle.
  final WorkspaceController? presentationSource;
  final String? _presentationAuthority;
  bool presentationActive = true;
  final RestoreSharedDraftAttachment? restoreSharedAttachment;
  final Future<void> Function(
    String origin,
    String principal,
    String server,
    String? channel,
  )?
  discardSharedScope;
  late final WorkspaceEntityDirectory entityDirectory;
  late final bool _ownsEntityDirectory;

  /// Server-scoped followed threads, shared with borrowed controllers.
  late final FollowedThreadsStore followedThreads;
  late final bool _ownsFollowedThreads;

  /// Web `useTranslationStore`: translation settings and the in-memory
  /// translation cache, created on first use.
  MessageTranslationStore get translations =>
      presentationSource?.translations ??
      (_translations ??= MessageTranslationStore(this));
  MessageTranslationStore? _translations;

  /// Web ThreadPanel in-thread search state (bar, query, matches), created on
  /// first use; a presentation mirror shares its source's.
  ThreadSearch get threadSearch =>
      presentationSource?.threadSearch ??
      (_threadSearch ??= ThreadSearch(this));
  ThreadSearch? _threadSearch;

  /// Web `useThreadAgentFollowerStore`: the agents following each thread.
  ThreadAgentFollowers get threadFollowers =>
      presentationSource?.threadFollowers ??
      (_threadFollowers ??= ThreadAgentFollowers(this));
  ThreadAgentFollowers? _threadFollowers;

  /// Web `useAnnouncementStore`: the account-level announcement queue,
  /// living as long as this account session.
  AnnouncementStore get announcements =>
      presentationSource?.announcements ??
      (_announcements ??= AnnouncementStore(
        get: (path, {query}) => client.get(path, query: query),
        post: (path) => client.post(path),
        principal: () => client.user?.id,
      ));
  AnnouncementStore? _announcements;

  /// Web `useOnboardingAnnouncementGateStore` (session memory per account).
  AnnouncementOnboardingGate get announcementGate =>
      presentationSource?.announcementGate ??
      (_announcementGate ??= AnnouncementOnboardingGate());
  AnnouncementOnboardingGate? _announcementGate;
  WorkspaceEntityScope? _entityScope() {
    final principal = client.user?.id;
    final serverId = client.serverId;
    if (_disposed ||
        principal == null ||
        serverId == null ||
        server?.id != serverId) {
      return null;
    }
    return WorkspaceEntityScope(
      origin: client.origin,
      principal: principal,
      serverId: serverId,
      generation: client.generation,
      role: server?.string('role') ?? '',
      capabilities: {
        if (can('viewAgents')) WorkspaceEntityKind.agents,
        if (can('viewMachines')) WorkspaceEntityKind.computers,
        if (can('viewMembers')) WorkspaceEntityKind.members,
      },
    );
  }

  /// Device records of the server-scoped entity directory: kind `entities`,
  /// id = [WorkspaceEntityKind.name], under the scope's origin, principal and
  /// server (so logout, account and server revocation clear them with the
  /// rest of the account/server data). The directory checks role and
  /// capabilities before painting one.
  Future<dynamic> _readEntities(
    WorkspaceEntityScope scope,
    WorkspaceEntityKind kind,
  ) async {
    final store = cache;
    if (store == null || _disposed || _cacheRetired) return null;
    await _cacheWrites;
    if (_disposed || _cacheRetired) return null;
    return store.read(
      scope.origin,
      scope.principal,
      scope.serverId,
      'entities',
      kind.name,
    );
  }

  void _writeEntities(
    WorkspaceEntityScope scope,
    WorkspaceEntityKind kind,
    Map<String, dynamic>? record,
  ) {
    final store = cache;
    if (store == null ||
        _cacheRetired ||
        (record != null && _revokedServers.contains(scope.serverId))) {
      return;
    }
    _cacheWrites = _cacheWrites.then((_) async {
      try {
        await store.write(
          scope.origin,
          scope.principal,
          scope.serverId,
          'entities',
          kind.name,
          record,
        );
      } catch (_) {
        // Best effort: the directory is revalidated on every start anyway.
      }
    });
  }

  /// Saved message translations of [serverId] under the current account
  /// (see [MessageTranslationStore.restore]); empty without a device cache.
  Future<List<Map<String, dynamic>>> readDeviceTranslations(
    String serverId,
  ) async {
    final store = cache, principal = client.user?.id;
    if (store == null || principal == null || _disposed || _cacheRetired) {
      return const [];
    }
    final origin = client.origin;
    await _cacheWrites;
    if (_disposed || _cacheRetired) return const [];
    try {
      return await store.readTranslations(origin, principal, serverId);
    } catch (_) {
      // Best effort: missing entries are requested again.
      return const [];
    }
  }

  /// Queues a device write of translation [entries] of [serverId] under the
  /// current account. Nothing is written once the cache is retired, for a
  /// revoked server, or for a channel revoked since the entry was made.
  void writeDeviceTranslations(
    String serverId,
    List<Map<String, dynamic>> entries,
  ) {
    final store = cache, principal = client.user?.id;
    if (store == null ||
        principal == null ||
        _cacheRetired ||
        _revokedServers.contains(serverId)) {
      return;
    }
    final rows = [
      for (final entry in entries)
        if (client.serverId != serverId ||
            !_revokedChannels.contains(entry['channelId']))
          entry,
    ];
    if (rows.isEmpty) return;
    final origin = client.origin;
    _cacheWrites = _cacheWrites.then((_) async {
      if (_cacheRetired) return;
      try {
        await store.writeTranslations(origin, principal, serverId, rows);
      } catch (_) {
        // Best effort: unsaved translations are requested again.
      }
    });
  }

  /// The server translation gate saved for [serverId] (kind
  /// `translation-settings`), or null.
  Future<dynamic> readTranslationGate(String serverId) => _cacheRetired
      ? Future.value()
      : _cached('translation-settings', '', server: serverId);

  /// Saves (or with null deletes) the translation gate of [serverId].
  void saveTranslationGate(String serverId, Map<String, dynamic>? gate) {
    if (gate != null && _revokedServers.contains(serverId)) return;
    unawaited(_save('translation-settings', '', gate, server: serverId));
  }

  void _entityAuthorityChanged() {
    if (_disposed || !_ownsEntityDirectory) return;
    if (entityDirectory.synchronize() && entityDirectory.started) {
      // ignore: raft_lints/avoid_unreviewed_idle_work -- One-shot deferral out of a build or notification; the callback does not schedule another microtask itself.
      scheduleMicrotask(() {
        if (!_disposed) unawaited(entityDirectory.preload());
      });
    }
  }

  AttachmentImageRepository? _attachmentImages;

  /// Persistent bytes for inline/list image previews (null: memory only).
  final AttachmentImageByteStore? attachmentImageStore;
  int get retainedImageDecodedBytes => _attachmentImages?.decodedByteCount ?? 0;
  int get retainedImageCount => _attachmentImages?.entryCount ?? 0;
  int get retainedImageEncodedBytes => _attachmentImages?.encodedByteCount ?? 0;
  final _imageAuthorities =
      <AttachmentImageKey, Map<Object, bool Function()>>{};
  AttachmentImageScope get attachmentImageScope => AttachmentImageScope(
    origin: client.origin,
    principal: client.user?.id ?? '',
    server: client.serverId ?? '',
    generation: client.generation,
    role: server?.string('role') ?? '',
  );

  /// Server-identity key for panel caches (account, origin, server, role and
  /// authentication generation). Channel switches, threads and tab changes do
  /// not change it; null while no signed-in server is selected.
  String? _panelCacheIdentity() {
    final principal = client.user?.id;
    final serverId = client.serverId;
    if (_disposed ||
        principal == null ||
        serverId == null ||
        server?.id != serverId) {
      return null;
    }
    return jsonEncode([
      client.origin,
      principal,
      serverId,
      client.generation,
      server?.string('role'),
    ]);
  }

  ServerBoundCache<String, Object> get agentTabCache =>
      presentationSource?.agentTabCache ?? _agentTabCache;
  late final _agentTabCache = ServerBoundCache<String, Object>(
    _panelCacheIdentity,
  );
  ServerBoundCache<String, TaskHistorySnapshot> get taskHistoryCache =>
      presentationSource?.taskHistoryCache ?? _taskHistoryCache;
  late final _taskHistoryCache = ServerBoundCache<String, TaskHistorySnapshot>(
    _panelCacheIdentity,
    limit: 128,
  );
  late final _channelFilesStores =
      ServerBoundCache<String, SourceChannelFilesStore>(
        _panelCacheIdentity,
        limit: 24,
        onEvict: (store) => store.retire(),
      );

  /// Channel access behind a cached Files list: the server identity plus the
  /// member's current capabilities for that channel. null (nothing shown, list
  /// dropped on the next synchronization) once the channel is gone, revoked or
  /// no longer viewable. Channel switches and thread windows are not part of it.
  String? _channelFilesAuthority(String channelId) {
    final identity = _panelCacheIdentity();
    if (identity == null || _revokedChannels.contains(channelId)) return null;
    final current = channel?.id == channelId ? channel : null;
    final row =
        current ??
        [...channels, ...dms].where((c) => c.id == channelId).firstOrNull;
    if (row == null || !can('viewChannel', resource: row)) return null;
    return jsonEncode([identity, row.id, row.json['channelCapabilities']]);
  }

  /// The controller-owned Files projection of [channelId]. The same store is
  /// returned on every visit, so a revisit paints the last list immediately
  /// and a refresh replaces it in place.
  SourceChannelFilesStore channelFilesStore(String channelId) {
    final source = presentationSource;
    if (source != null) return source.channelFilesStore(channelId);
    final store = _channelFilesStores.putIfAbsent(
      channelId,
      () => SourceChannelFilesStore(
        channelId: channelId,
        authority: () => _channelFilesAuthority(channelId),
        get: (path, {query}) async {
          try {
            return await client.get(path, query: query);
          } on RaftApiException catch (error) {
            throw ChannelFilesRequestFailure(
              [401, 403].contains(error.status)
                  ? ChannelFilesFailure.unauthorized
                  : ChannelFilesFailure.unavailable,
            );
          }
        },
      ),
    );
    store.syncAuthority();
    return store;
  }

  bool _retainsAttachment(AttachmentImageKey key) {
    if (_disposed ||
        key.scope != attachmentImageScope ||
        client.user == null ||
        server?.id != client.serverId) {
      return false;
    }
    // A freshly accepted Files row is an independent permission projection.
    // Its active owner never grants authority through a cached URL or count.
    if (_imageAuthorities[key]?.values.any((admitted) => admitted()) == true) {
      return true;
    }
    final candidates =
        _retainedAttachmentIndex()[key.channelId]?[key.attachmentId] ??
        const <Map<String, dynamic>>[];
    for (final metadata in candidates) {
      final canonical = AttachmentImageKey.fromMetadata(
        scope: key.scope,
        channelId: key.channelId,
        metadata: metadata,
        rendition: key.rendition,
        target: key.target,
      );
      if (canonical == key) return true;
    }
    return false;
  }

  // Attachments in currently admitted channels, keyed by channel then file.
  // Notifications recheck admission; history is scanned only when the ledger,
  // image scope or admitted channel ids change.
  Object? _retainedIndexStamp;
  Map<String, Map<String, List<Map<String, dynamic>>>> _retainedIndex = {};
  Object? _retainedAdmissionStamp;
  List<String> _retainedChannelIds = const [];
  int _retainedIndexBuilds = 0;

  /// Number of complete history scans used for attachment retention.
  @visibleForTesting
  int get debugRetainedAttachmentIndexBuilds => _retainedIndexBuilds;

  List<String> _admittedAttachmentChannels() {
    // Keep every former index invalidation boundary. In particular a normal
    // notification still reevaluates can() and revocations before reusing any
    // metadata, even when the channel list has the same identity and length.
    final stamp = (
      ledger.revision,
      _stateStamp,
      attachmentImageScope,
      identityHashCode(channels),
      channels.length,
      identityHashCode(dms),
      dms.length,
      _revokedChannels.length,
    );
    if (stamp == _retainedAdmissionStamp) return _retainedChannelIds;
    final admitted = [
      for (final channel in [...channels, ...dms])
        if (!_revokedChannels.contains(channel.id) &&
            can('viewChannel', resource: channel))
          channel.id,
    ];
    if (!listEquals(admitted, _retainedChannelIds)) {
      _retainedChannelIds = List.unmodifiable(admitted);
    }
    _retainedAdmissionStamp = stamp;
    return _retainedChannelIds;
  }

  Map<String, Map<String, List<Map<String, dynamic>>>>
  _retainedAttachmentIndex() {
    final admitted = _admittedAttachmentChannels();
    final stamp = (ledger.revision, attachmentImageScope, admitted);
    if (stamp == _retainedIndexStamp) return _retainedIndex;
    ++_retainedIndexBuilds;
    final index = <String, Map<String, List<Map<String, dynamic>>>>{};
    for (final id in admitted) {
      final channelIds = <String>{id};
      for (final message in ledger.messages(id)) {
        final thread = RaftMessage(message).threadId;
        if (thread != null) channelIds.add(thread);
      }
      for (final channelId in channelIds) {
        if (index.containsKey(channelId)) continue;
        final byAttachment = index[channelId] = {};
        for (final message in ledger.messages(channelId)) {
          for (final metadata in RaftMessage(message).attachments) {
            final id = metadata['id'];
            if (id is String) (byAttachment[id] ??= []).add(metadata);
          }
        }
      }
    }
    _retainedIndexStamp = stamp;
    return _retainedIndex = index;
  }

  VoidCallback _registerImageAuthority(
    AttachmentImageKey key,
    bool Function() authorized,
  ) {
    final owner = Object();
    _imageAuthorities.putIfAbsent(key, () => {})[owner] = authorized;
    return () {
      final owners = _imageAuthorities[key];
      owners?.remove(owner);
      if (owners?.isEmpty == true) _imageAuthorities.remove(key);
      _attachmentImages?.synchronize(attachmentImageScope);
    };
  }

  AttachmentImageRepository _newImageRepository() {
    final budgets = RaftMemoryBudgets.current;
    final repository = AttachmentImageRepository(
      scope: attachmentImageScope,
      retainedAuthority: _retainsAttachment,
      store: attachmentImageStore,
      maxEntries: budgets.attachmentImageEntries,
      maxDecodedBytes: budgets.attachmentImageDecodedBytes,
      maxActiveDecodedBytes: budgets.attachmentImageActiveDecodedBytes,
      maxEncodedBytes: budgets.attachmentImageEncodedBytes,
    );
    // Decoded bitmaps are rebuildable from the byte store: let go of the
    // unleased ones under memory pressure and while the app is hidden.
    _removeImageReporter = RaftMemoryRelease.instance.addReporter(
      'attachmentImages',
      () =>
          '${repository.entryCount}/${repository.maxEntries} entries '
          '${repository.decodedByteCount}/${repository.maxDecodedBytes} B decoded '
          '${repository.encodedByteCount} B encoded',
    );
    _removeImageTrimmer = RaftMemoryRelease.instance.addTrimmer(
      (reason) => repository.trimIdle(
        pressure: reason == RaftMemoryTrim.pressure,
        reason: reason.name,
      ),
    );
    return repository;
  }

  VoidCallback? _removeImageTrimmer, _removeImageReporter;

  WorkspaceAttachmentImageLease acquireAttachmentImage(
    AttachmentImageKey key, {
    required AttachmentImageLoader load,
    required bool Function() authorized,
    bool foreground = false,
    bool bestAvailable = false,
  }) {
    if (presentationSource != null) {
      return presentationSource!.acquireAttachmentImage(
        key,
        load: load,
        authorized: authorized,
        foreground: foreground,
        bestAvailable: bestAvailable,
      );
    }
    final releaseAuthority = _registerImageAuthority(key, authorized);
    try {
      final repository = _attachmentImages ??= _newImageRepository();
      repository.synchronize(attachmentImageScope);
      return WorkspaceAttachmentImageLease(
        repository.acquire(
          key,
          load: load,
          authorized: authorized,
          foreground: foreground,
          bestAvailable: bestAvailable,
        ),
        releaseAuthority,
      );
    } catch (_) {
      releaseAuthority();
      rethrow;
    }
  }

  /// Synchronous lookup: a lease on the already decoded image for [key], or
  /// null when it is not retained (or no longer authorized). Never loads.
  WorkspaceAttachmentImageLease? peekAttachmentImage(
    AttachmentImageKey key, {
    required bool Function() authorized,
  }) {
    if (presentationSource != null) {
      return presentationSource!.peekAttachmentImage(
        key,
        authorized: authorized,
      );
    }
    final repository = _attachmentImages;
    if (repository == null || _disposed) return null;
    repository.synchronize(attachmentImageScope);
    final releaseAuthority = _registerImageAuthority(key, authorized);
    final lease = repository.acquireReady(key, authorized: authorized);
    if (lease == null) {
      releaseAuthority();
      return null;
    }
    return WorkspaceAttachmentImageLease(lease, releaseAuthority);
  }

  /// The persistent attachment cache, when this controller has one (the
  /// installed app cache by default; tests with a fake byte store have none).
  AttachmentDiskCache? get attachmentDiskCache {
    final source = presentationSource;
    if (source != null) return source.attachmentDiskCache;
    final store = attachmentImageStore;
    return store is AttachmentDiskCache ? store : null;
  }

  /// Persistent key of a derived entry (`file`, `preview`, `html`) of
  /// [metadata]'s attachment for the current account and server.
  AttachmentImageStoreKey attachmentCacheKey(
    Map<String, dynamic> metadata,
    String kind, {
    required String channelId,
  }) => AttachmentImageKey.storeKeyOf(
    scope: attachmentImageScope,
    channelId: channelId,
    metadata: metadata,
    kind: kind,
  );

  /// True when [key]'s source bytes are already stored on this device, so
  /// nothing needs to be signed or fetched for them.
  Future<bool> attachmentStored(AttachmentImageKey key) async {
    final disk = attachmentDiskCache;
    if (disk == null) return false;
    try {
      return await disk.contains(key.source.storeKey);
    } catch (_) {
      return false;
    }
  }

  /// Set by the host from its actual logical width before bootstrap. Home
  /// retains selection/drafts, but is not an open conversation/read surface.
  bool mobileNavigation;
  final messageSync = MessageSync();
  final threadRepliesSync = ThreadRepliesSync();
  final notificationPrefsSync = NotificationPrefsSync();
  bool syncCoreNotificationPrefsEnabled = false;
  String? _replyIdentity;

  /// Fail-closed fallback for an authority event without a channel id.
  int _replyAuthorityEpoch = 0;

  /// Per-channel authority epochs. A membership/authority change for one
  /// channel retires only that channel's windows, thread windows, reply
  /// scopes and in-flight loads; every other channel keeps its cache.
  final Map<String, int> _channelReplyEpochs = {};

  String _syncIdentity() => jsonEncode([
    client.origin,
    client.generation,
    client.serverId,
    client.user?.id,
    server?.string('role'),
  ]);

  /// Authority token for data owned by [channelId] (its message window, its
  /// thread windows and reply summaries).
  String _replyToken(String? channelId) => jsonEncode([
    _syncIdentity(),
    _replyAuthorityEpoch,
    channelId,
    _channelReplyEpochs[channelId] ?? 0,
  ]);
  void _ensureSyncIdentity() {
    final next = _syncIdentity();
    if (_replyIdentity == next) return;
    final hadIdentity = _replyIdentity != null;
    _messageFlagRequest++;
    _replyIdentity = next;
    threadRepliesSync.reset();
    notificationPrefsSync.reset();
    syncCoreNotificationPrefsEnabled = false;
    if (hadIdentity) _summariesByChannel.clear();
  }

  /// Retires one channel's reply authority (Source refreshes only the
  /// affected channel row on a membership event). Its reply scopes and
  /// thread windows go now; its message window is dropped on next use by the
  /// token mismatch. The open channel keeps its displayed summaries until its
  /// own refresh replaces them; a real revocation clears them in
  /// [_revokeChannel].
  void _invalidateChannelReplies(String id) {
    _channelReplyEpochs[id] = (_channelReplyEpochs[id] ?? 0) + 1;
    threadRepliesSync.revokeChannel(id);
    messageWindows.invalidateParentThreads(id);
    if (channel?.id != id) _summariesByChannel.remove(id);
  }

  void _invalidateAllChannelReplies() {
    _replyAuthorityEpoch++;
    threadRepliesSync.reset();
    messageWindows.clearThreads();
    _summariesByChannel.removeWhere((id, _) => id != channel?.id);
  }

  /// Whether a member list change can change this principal's visibility of
  /// the channel (Source resource views fail closed on the same rule).
  bool _membershipScoped(String id) {
    final known = [...channels, ...dms].where((c) => c.id == id).firstOrNull;
    if (known == null) return true;
    return const {'private', 'dm', 'joint', 'thread'}.contains(known.type);
  }

  void _mergeThreadSummaries(String id, dynamic rows) => _summariesByChannel
      .putIfAbsent(id, () => {})
      .addAll(
        _hydrateThreadSummaries(rows, id, expectedToken: _replyToken(id)),
      );

  Map<String, dynamic> _hydrateThreadSummaries(
    dynamic raw,
    String parentChannelId, {
    String? expectedToken,
  }) {
    _ensureSyncIdentity();
    if (expectedToken != null &&
        expectedToken != _replyToken(parentChannelId)) {
      return {};
    }
    if (raw is! Map) return {};
    final id = client.serverId, principal = client.user?.id;
    return {
      for (final entry in raw.entries)
        if (entry.key is String && entry.value is Map)
          entry.key as String: id == null || principal == null
              ? Map<String, dynamic>.from(entry.value)
              : threadRepliesSync.hydrateSummary(
                  id,
                  principal,
                  parentChannelId,
                  entry.key as String,
                  Map<String, dynamic>.from(entry.value),
                ),
    };
  }

  bool _replyCurrent(String token, ThreadRebaselineRequest request) =>
      !_disposed &&
      token == _replyToken(request.parentChannelId) &&
      !_revokedChannels.contains(request.parentChannelId) &&
      !_revokedChannels.contains(request.threadChannelId);

  Future<void> _rebaselineThread(ThreadRebaselineRequest request) async {
    final token = _replyToken(request.parentChannelId);
    try {
      // Exact mounted compatibility snapshot used by threadRepliesSyncDomain.
      final values = await Future.wait([
        client.get(
          '/messages/channel/${request.threadChannelId}',
          query: {'limit': inlineReplyCap},
        ),
        client.get(
          '/channels/${request.parentChannelId}/threads/${request.parentMessageId}',
        ),
      ]);
      if (!_replyCurrent(token, request)) return;
      final page = values[0], summary = values[1];
      if (page is! Map ||
          page['messages'] is! List ||
          summary is! Map ||
          summary['threadChannelId'] != request.threadChannelId ||
          summary['replyCount'] is! int ||
          summary['replyCount'] < 0) {
        return;
      }
      final previews = (page['messages'] as List)
          .map((m) => projectThreadReply(m, snapshot: true))
          .whereType<Map<String, dynamic>>();
      final result = threadRepliesSync.completeRebaseline(
        request,
        replies: previews,
        replyCount: summary['replyCount'],
        historyLimited: page['historyLimited'] == true,
      );
      if (result.kind != 'applied' || result.summary == null) return;
      _summariesByChannel.putIfAbsent(
        request.parentChannelId,
        () => {},
      )[request.parentMessageId] = result.summary;
      _applyThreadUnread(request.threadChannelId, result.summary);
      if (channel?.id == request.parentChannelId) _saveWindow(channel!.id);
      notifyListeners();
    } catch (_) {
      // No guessed transport or fabricated completion. The next eligible
      // producer frame can retry its authorized compatibility snapshot.
    } finally {
      threadRepliesSync.release(request);
    }
  }

  void _consumeThreadUpdate(Map<String, dynamic> payload) {
    _ensureSyncIdentity();
    final id = client.serverId, principal = client.user?.id;
    if (id == null || principal == null) return;
    if (payload['serverId'] != null && payload['serverId'] != id) return;
    final parentId = payload['parentMessageId'];
    final latest = payload['latestReply'];
    final anchor = latest is Map ? latest['conversationContext'] : null;
    final parentChannel = anchor is Map ? anchor['parentChannelId'] : null;
    if (_revokedChannels.contains(payload['threadChannelId']) ||
        _revokedChannels.contains(parentChannel)) {
      return;
    }
    if (parentId is String &&
        channel != null &&
        _revokedChannels.contains(channel!.id)) {
      return;
    }
    final result = threadRepliesSync.consume(
      payload,
      serverId: id,
      principalId: principal,
    );
    if (result.summary != null && parentId is String) {
      final owner = _summaryOwner(payload, parentId);
      if (owner != null && !_revokedChannels.contains(owner)) {
        _summariesByChannel.putIfAbsent(owner, () => {})[parentId] =
            result.summary;
      }
      _applyThreadUnread(payload['threadChannelId'], payload);
    }
    if (result.request != null) unawaited(_rebaselineThread(result.request!));
  }

  /// The parent channel whose reply rows a `thread:updated` summary belongs
  /// to: the wire anchor first, else the channel already holding the parent.
  String? _summaryOwner(Map<String, dynamic> payload, String parentId) {
    final latest = payload['latestReply'];
    final anchor = latest is Map ? latest['conversationContext'] : null;
    if (anchor is Map && anchor['parentChannelId'] is String) {
      return anchor['parentChannelId'] as String;
    }
    final window = payload['syncCoreReplyWindow'];
    final discussion = window is Map ? window['discussion'] : null;
    final scope = discussion is Map ? discussion['parentScopeKey'] : null;
    if (scope is Map && scope['scopeId'] is String) {
      return scope['scopeId'] as String;
    }
    final current = channel?.id;
    if (current != null &&
        (_summariesByChannel[current]?.containsKey(parentId) == true ||
            ledger.contains(current, parentId))) {
      return current;
    }
    for (final entry in _summariesByChannel.entries) {
      if (entry.value.containsKey(parentId)) return entry.key;
    }
    for (final entry in visibleIds.entries) {
      if (entry.value.contains(parentId)) return entry.key;
    }
    return null;
  }

  /// A thread summary carries the server's own per-thread unread count; it
  /// replaces only a count the unread snapshot already tracks.
  void _applyThreadUnread(dynamic threadChannelId, dynamic summary) {
    if (threadChannelId is! String ||
        summary is! Map ||
        !unread.containsKey(threadChannelId) ||
        threadChannelId == this.threadChannelId) {
      return;
    }
    final count = summary['unreadCount'];
    if (count is! int || count < 0 || unread[threadChannelId] == count) return;
    unread = {...unread, threadChannelId: count};
    _persistUnread();
  }

  void _consumeNotificationPrefs(dynamic payload) {
    _ensureSyncIdentity();
    var update = readNotificationPrefsUpdate(payload);
    if (update == null ||
        update['serverId'] != client.serverId ||
        client.user == null) {
      return;
    }
    if (syncCoreNotificationPrefsEnabled) {
      update = notificationPrefsSync.consume(update);
      if (update == null) return;
    }
    final accepted = update;
    if (accepted['type'] == 'server') {
      final version = accepted['prefsVersion'];
      RaftRecord patch(RaftRecord value) {
        if (value.id != accepted['serverId']) return value;
        final prior = value.json['notificationPrefsVersion'];
        if (prior is int && version is int && version < prior) return value;
        return RaftRecord({
          ...value.json,
          'serverPushMuted': accepted['serverPushMuted'],
          'notificationPrefsVersion': ?version,
        });
      }

      servers = servers.map(patch).toList();
      if (server != null) server = patch(server!);
    } else {
      final state = accepted['state'] as Map;
      final id = accepted['channelId'];
      if (_revokedChannels.contains(id)) return;
      RaftChannel patch(RaftChannel value) {
        if (value.id != id) return value;
        final old = value.json['prefsVersion'],
            incoming = state['prefsVersion'];
        if (old is int && incoming is int && incoming < old) return value;
        return RaftChannel({...value.json, ...state});
      }

      channels = channels.map(patch).toList();
      dms = dms.map(patch).toList();
      if (channel != null) channel = patch(channel!);
    }
    refreshUnread();
  }

  /// Source socketBridge `readMessageDisplayPrefsUpdate`: null unless the
  /// frame carries a string `serverId`/`scopeId` and a boolean
  /// `prefs.collapseLongMessages`.
  static ({String serverId, String channelId, bool collapse, int? version})?
  readMessageDisplayPrefsUpdate(dynamic payload) {
    if (payload is! Map) return null;
    final serverId = payload['serverId'], scopeId = payload['scopeId'];
    final prefs = payload['prefs'];
    if (serverId is! String || scopeId is! String || prefs is! Map) return null;
    final collapse = prefs['collapseLongMessages'];
    if (collapse is! bool) return null;
    final version = payload['prefsVersion'];
    return (
      serverId: serverId,
      channelId: scopeId,
      collapse: collapse,
      version: version is num ? version.toInt() : null,
    );
  }

  /// Source `applyMessageDisplayPrefsState` for one row: a patch older than
  /// the row's `displayPrefsVersion` is ignored.
  static RaftChannel applyMessageDisplayPrefsToChannel(
    RaftChannel value, {
    required bool collapse,
    int? version,
  }) {
    final prior = value.json['displayPrefsVersion'];
    if (prior is num && version != null && version < prior) return value;
    return RaftChannel({
      ...value.json,
      'collapseLongMessages': collapse,
      'displayPrefsVersion': ?version,
    });
  }

  /// Source socketBridge `message_display_prefs:updated`: written onto the
  /// channel row so chat folding follows another device's toggle.
  void _consumeMessageDisplayPrefs(dynamic payload) {
    final update = readMessageDisplayPrefsUpdate(payload);
    if (update == null ||
        (client.serverId != null && update.serverId != client.serverId) ||
        _revokedChannels.contains(update.channelId)) {
      return;
    }
    RaftChannel patch(RaftChannel value) => value.id != update.channelId
        ? value
        : applyMessageDisplayPrefsToChannel(
            value,
            collapse: update.collapse,
            version: update.version,
          );
    channels = channels.map(patch).toList();
    dms = dms.map(patch).toList();
    if (channel != null) channel = patch(channel!);
  }

  bool syncCoreMessagesEnabled = false;

  /// Web serverFeatureFlags `remote_computer_upgrade_v2`: whether the
  /// Computer detail may offer the one-click upgrade (fail-closed).
  static const remoteComputerUpgradeFlag = 'remote_computer_upgrade_v2';
  bool remoteComputerUpgradeEnabled = false;

  /// Web `AGENT_MIGRATION_FEATURE_FLAG_KEY` (`useAgentMigrationUiEnabled`):
  /// the "Move to another computer" action exists only while it is on.
  static const agentMigrationFlag = 'agent_migration_v0';

  /// Web `RUNTIME_FORM_V2_WEB_FLAG_KEY` (default off): the agent runtime
  /// editor presents the protocol-v2 server form only while it is on.
  static const runtimeFormV2WebFlag = 'runtime_form_v2_web';

  /// Both fail closed and ride the same evaluation as the sync flags (no
  /// request of their own).
  bool agentMigrationEnabled = false, runtimeFormV2WebEnabled = false;
  int _messageFlagRequest = 0;

  Future<void> refreshMessageSyncFlag() async {
    _ensureSyncIdentity();
    final request = ++_messageFlagRequest;
    final generation = client.generation, principal = client.user?.id;
    final identity = _syncIdentity();
    final id = client.serverId;
    if (id == null || principal == null) return;
    try {
      final result = await client.post(
        '/feature-flags/evaluate',
        data: featureFlagEvaluateBody(
          keys: [
            'sync_core_messages_v0',
            notificationPrefsFlag,
            remoteComputerUpgradeFlag,
            agentMigrationFlag,
            runtimeFormV2WebFlag,
          ],
          serverId: id,
        ),
      );
      if (_disposed ||
          request != _messageFlagRequest ||
          generation != client.generation ||
          principal != client.user?.id ||
          id != client.serverId ||
          identity != _syncIdentity()) {
        return;
      }
      final enabled =
          result is Map &&
          (result['evaluations'] as List? ?? []).whereType<Map>().any(
            (f) => f['key'] == 'sync_core_messages_v0' && f['enabled'] == true,
          );
      if (enabled != syncCoreMessagesEnabled) messageSync.reset();
      syncCoreMessagesEnabled = enabled;
      final prefsEnabled =
          result is Map &&
          (result['evaluations'] as List? ?? []).whereType<Map>().any(
            (f) => f['key'] == notificationPrefsFlag && f['enabled'] == true,
          );
      if (prefsEnabled != syncCoreNotificationPrefsEnabled) {
        notificationPrefsSync.reset();
      }
      syncCoreNotificationPrefsEnabled = prefsEnabled;
      remoteComputerUpgradeEnabled =
          result is Map &&
          (result['evaluations'] as List? ?? []).whereType<Map>().any(
            (f) =>
                f['key'] == remoteComputerUpgradeFlag && f['enabled'] == true,
          );
      bool evaluated(String key) =>
          result is Map &&
          (result['evaluations'] as List? ?? []).whereType<Map>().any(
            (f) => f['key'] == key && f['enabled'] == true,
          );
      final migration = evaluated(agentMigrationFlag);
      final runtimeV2 = evaluated(runtimeFormV2WebFlag);
      final flagsChanged =
          migration != agentMigrationEnabled ||
          runtimeV2 != runtimeFormV2WebEnabled;
      agentMigrationEnabled = migration;
      runtimeFormV2WebEnabled = runtimeV2;
      if (flagsChanged) notifyListeners();
    } catch (_) {
      if (!_disposed &&
          request == _messageFlagRequest &&
          identity == _syncIdentity()) {
        syncCoreNotificationPrefsEnabled = false;
        notificationPrefsSync.reset();
        if (agentMigrationEnabled || runtimeFormV2WebEnabled) {
          agentMigrationEnabled = runtimeFormV2WebEnabled = false;
          notifyListeners();
        }
      }
      // Unknown capability never authorizes the gated preference path.
    }
  }

  Future<void> _cacheWrites = Future<void>.value();

  /// Set when the account's device data is being cleared (logout): nothing
  /// more is written for it, so a late write cannot resurrect cleared rows.
  bool _cacheRetired = false;

  /// Writes every pending (debounced) change and completes once all queued
  /// device writes have landed.
  Future<void> flushCache() async {
    await MessageHistorySnapshots.flush(this);
    await MessageHistoryPages.storage(cache)?.receiver.settled;
    await MessageHistoryPages.storage(cache)?.positionsSettled;
    if (_ownsEntityDirectory) entityDirectory.flushDevice();
    _translations?.flushDevice();
    await Future.wait([_cacheWrites, composer.flushed]);
  }

  /// Stops all further device writes of this workspace and completes once
  /// the writes already queued have landed. The caller then clears the
  /// account's device data.
  Future<void> retireCache() {
    _cacheRetired = true;
    return Future.wait([_cacheWrites, composer.flushed]);
  }

  Map<String, String> get drafts =>
      !composer.ownedBy(this) && (server == null || _disposed)
      ? const {}
      : composer.drafts;
  final Set<String> _revokedChannels = {};
  final Set<String> _revokedServers = {};
  Object? _presentationOwner;
  bool _mainPresented = true, _threadPresented = true;
  Object? _tabPresentationOwner;
  bool _chatTabPresented = true;

  /// Channel tabs and folded panels are independent presentation owners.
  /// Neither loaded data nor an offstage retained editor admits a read receipt.
  void setChatTabPresentation(Object owner, bool visible) {
    _tabPresentationOwner = owner;
    _chatTabPresented = visible;
    if (presentationSource != null) {
      if (presentationActive) {
        presentationSource!.setChatTabPresentation(owner, visible);
      }
    }
  }

  void releaseChatTabPresentation(Object owner) {
    presentationSource?.releaseChatTabPresentation(owner);
    if (!identical(owner, _tabPresentationOwner)) return;
    _tabPresentationOwner = null;
    _chatTabPresented = true;
  }

  /// Mounted layout admission, separate from accepted message windows. This
  /// changes synchronously without notifying/rebuilding the controller tree.
  void setConversationPresentation(
    Object owner, {
    required bool main,
    required bool thread,
  }) {
    _presentationOwner = owner;
    _mainPresented = main;
    _threadPresented = thread;
    if (presentationSource != null) {
      if (presentationActive) {
        presentationSource!.setConversationPresentation(
          owner,
          main: main,
          thread: thread,
        );
      }
    }
  }

  void releaseConversationPresentation(Object owner) {
    presentationSource?.releaseConversationPresentation(owner);
    if (!identical(owner, _presentationOwner)) return;
    _presentationOwner = null;
    _mainPresented = _threadPresented = true;
  }

  SourceActivityUnreadStore? _activityUnread;

  /// This session owns one window; pages and platform adapters only borrow it.
  /// Server selection creates it before UI; standalone views can borrow a
  /// passive projection before selection. Neither path starts work by reading.
  SourceActivityUnreadStore get activityUnread => _disposed
      ? throw StateError('Workspace session is disposed.')
      : presentationSource?.activityUnread ??
            (_activityUnread ??= SourceActivityUnreadStore(this));

  /// The existing session projection; reading it never constructs or starts it.
  SourceActivityUnreadStore? get activityUnreadIfBound => _disposed
      ? null
      : presentationSource?.activityUnreadIfBound ?? _activityUnread;

  final foregroundChanges = ValueNotifier<bool>(true);

  /// Counts returns to the foreground that reached [resumeLiveSession]; views
  /// that own live summaries (the server menu's Activity count) refresh on it.
  final resumes = ValueNotifier<int>(0);
  bool foreground = true;
  void setForeground(bool value) {
    foreground = value;
    if (!value) RaftUserActivity.forget();
    foregroundChanges.value = value;
    if (value) {
      if (channel != null && !hasNewer) markRead(channel!.id);
      if (threadChannelId != null) markRead(threadChannelId!);
    }
  }

  /// A workspace switch and loss of the active workspace retire the same
  /// accepted windows and session projections before another read can publish.
  void _resetServerWindows(String? id) {
    historyLimited = threadHistoryLimited = false;
    channel = null;
    channels = [];
    dms = [];
    unread = {};
    mentions = {};
    newFlags = {};
    _latestActivity.clear();
    threadParent = null;
    threadChannelId = null;
    _summariesByChannel.clear();
    highlightedMessageId = null;
    messageWindows.channelGeneration++;
    messageWindows.threadGeneration++;
    messageWindows.switchServer(id);
    readState.reset();
    resourceSnapshots.clear();
    _prefetched.clear();
    reactionViewer.reset();
    messageSync.reset();
    syncCoreMessagesEnabled = false;
  }

  void revokeServer(String id) {
    final principal = client.user?.id;
    if (principal == null) return;
    _revokedServers.add(id);
    servers.removeWhere((s) => s.id == id);
    final origin = client.origin;
    unawaited(discardSharedScope?.call(origin, principal, id, null));
    // Everything cached for that server on this device is revoked with it.
    final store = attachmentImageStore;
    if (store != null && presentationSource == null) {
      unawaited(
        store
            .purgeIdentity(AttachmentImageKey.identityOf(origin, principal, id))
            .catchError((Object _) {}),
      );
    }
    if (client.serverId == id) {
      composer.clearOwnedBy(this);
      _resetServerWindows(null);
      server = null;
      sidebarOrder = {};
      error = null;
      _messageFlagRequest++;
      if (ownsClient) client.selectServer(null);
      loading = false;
      channelLoading = false;
      threadLoading = false;
    }
    _cacheWrites = _cacheWrites.then((_) async {
      try {
        await cache?.revokeServer(origin, principal, id);
      } catch (_) {
        if (!_disposed) {
          // ignore: raft_lints/avoid_imperative_notice_state -- Cache-purge device failures and background refresh failures raise a one-shot toast; stale-scope and rate-limited failures are suppressed (_quietFailure) and the error is cleared on server switch or revoke.
          setError(
            'Revoked workspace data could not be removed from this device.',
          );
        }
      }
    });
    _save('servers', '', servers.map((s) => s.json).toList(), server: '');
    notifyListeners();
  }

  Future<dynamic> _cached(String kind, String id, {String? server}) async {
    if (cache == null || client.user == null) return null;
    final origin = client.origin,
        principal = client.user!.id,
        serverId = server ?? client.serverId ?? '';
    await _cacheWrites;
    try {
      return await cache!.read(origin, principal, serverId, kind, id);
    } catch (_) {
      if (!_disposed) error ??= 'Local workspace history is unavailable.';
      return null;
    }
  }

  Future<void> _save(
    String kind,
    String id,
    dynamic value, {
    String? server,
    bool requiredSuccess = false,
  }) async {
    final user = client.user?.id;
    if (cache == null || user == null || _cacheRetired) return;
    if (_revokedChannels.contains(id) ||
        value is Map && _revokedChannels.contains(value['channelId'])) {
      return;
    }
    final origin = client.origin, serverId = server ?? client.serverId ?? '';
    final windowScope = value is Map && const {'window', 'thread-window'}.contains(kind)
        ? '${value['parentChannelId'] ?? id}'
        : null;
    final windowTicket = windowScope == null
        ? null
        : messageHistoryTicket(windowScope);
    final writing = _cacheWrites.then((_) async {
      if (windowScope != null &&
          (!cacheWritable || windowTicket != messageHistoryTicket(windowScope))) {
        return;
      }
      try {
        await cache!.write(origin, user, serverId, kind, id, value);
      } catch (cause) {
        if (!_disposed) {
          error ??= MessageHistoryPages.saveError(cause);
          notifyListeners();
        }
        if (requiredSuccess) rethrow;
      }
    });
    _cacheWrites = writing.then<void>((_) {}, onError: (Object _) {});
    await writing;
  }

  void saveDraft(String text, {bool thread = false}) {
    final source = presentationSource;
    if (source != null) {
      if (_presentationAuthority == source.navigationAuthority) {
        source._savePresentedDraft(draftScope(thread: thread), text);
      }
      return;
    }
    _savePresentedDraft(draftScope(thread: thread), text);
  }

  void _savePresentedDraft(String? scope, String text) {
    if (scope == null || _disposed || _revokedChannels.contains(scope)) return;
    drafts[scope] = text;
    unawaited(_saveDraftScope(scope));
  }

  Future<void> _saveDraftScope(String scope, {bool requiredSuccess = false}) =>
      composer.saveScope(scope, requiredSuccess: requiredSuccess);

  Future<void> stageSharedDraft(
    String text,
    List<SharedDraftAttachment> files, {
    String? sourceToken,
  }) {
    final scope = draftScope();
    if (scope == null) throw const RaftApiException('Choose a conversation.');
    return composer.stageSharedDraft(
      scope,
      text,
      files,
      sourceToken: sourceToken,
      persist: _saveDraftScope,
    );
  }

  void updateDraftFromShare(String text) {
    final scope = draftScope();
    if (scope == null) return;
    saveDraft(text);
    _draftRevisions[scope] = (_draftRevisions[scope] ?? 0) + 1;
    notifyListeners();
  }

  Map<String, int> get _draftRevisions => composer.revisions;
  int draftRevision({bool thread = false}) =>
      _draftRevisions[draftScope(thread: thread)] ?? 0;
  ValueListenable<int> draftChanges({bool thread = false}) =>
      composer.changes(draftScope(thread: thread));

  /// Capture the common session facts once; the window owner captures its
  /// own epochs. A caller supplies only the operation-specific selection rule.
  MessageWindowRequest _windowRequest(
    String id, {
    bool thread = false,
    bool checkNavigation = false,
    bool Function()? allowed,
  }) {
    final parent = thread ? threadParentChannelId : id;
    final reply = _replyToken(parent), authority = _windowAuthority();
    final revision = navigationRevision;
    return messageWindows.request(
      thread: thread,
      allowed: () =>
          reply == _replyToken(parent) &&
          authority == _windowAuthority() &&
          !_revokedChannels.contains(id) &&
          (!checkNavigation || revision == navigationRevision) &&
          (allowed?.call() ?? true),
    );
  }

  Future<void> _restoreDraft(String scope) async {
    final generation = ledger.generation;
    await composer.restore(
      scope,
      current: () => generation == ledger.generation && !_disposed,
      restoreFiles: (files) async {
        final principal = client.user?.id, origin = client.origin;
        if (restoreSharedAttachment == null || principal == null) return;
        await composer.uploads.restore(
          scope,
          files,
          read: (reference) =>
              restoreSharedAttachment!(reference, origin, principal),
        );
      },
      restored: notifyListeners,
    );
  }

  String _windowAuthority() =>
      messageWindowAuthority(server?.string('role'), channel?.json ?? const {});

  /// Device writes stop before logout purges are queued.
  bool get cacheWritable => !_cacheRetired && !_disposed;
  Object messageHistoryTicket(String id) => _replyToken(id);
  void rejectHistoryRead(String id) => _windowReceiver._revokeChannel(id);
  Future<void> saveHistoryWindow(String id, {bool thread = false}) =>
      _windowReceiver._saveWindow(id, thread: thread);

  Future<void> _saveWindow(String id, {bool thread = false, bool coalesce = false}) =>
      MessageHistorySnapshots.save(
        this, id, thread: thread, coalesce: coalesce,
        identity: _threadIdentity, replyToken: _replyToken, write: _save,
      );

  /// Disk windows retain canonical versions already held in memory. Only the
  /// bootstrap replaces visible membership and starts from disk summaries.
  bool _restoreCachedTail(
    String id,
    dynamic page,
    MessageWindowRequest request, {
    String? replyAuthority,
    bool replace = false,
  }) {
    if (!MessageHistorySnapshots.restore(this, id, page, request, replace: replace,
        allowed: () => !_revokedServers.contains(server?.id))) {
      return false;
    }
    _summariesByChannel[id] = {
      ..._hydrateThreadSummaries(
        page['threadSummaries'],
        id,
        expectedToken: replyAuthority ?? _replyToken(id),
      ),
      if (!replace) ...?_summariesByChannel[id],
    };
    return true;
  }

  late final StreamSubscription<RaftEvent> subscription;
  MessageLedger get ledger => messageWindows.ledger;
  final _reactionViewer = ReactionViewerLedger();
  ReactionViewerLedger get reactionViewer =>
      presentationSource?.reactionViewer ?? _reactionViewer;
  final Map<String, Future<void>> _reactionWrites = {};
  final Map<String, Future<void>> _viewerLoads = {};
  Map<String, List<UploadDraft>> get _uploads => composer.uploads.byScope;
  Map<String, ComposerSendAttempt> get _attempts => composer.attempts;

  bool _disposed = false;
  int _stateStamp = 0;
  @override
  void notifyListeners() {
    _stateStamp++;
    _attachmentImages?.synchronize(attachmentImageScope);
    _attachmentUrls.synchronize(attachmentImageScope);
    _playerSessions.synchronize(attachmentImageScope);
    if (!_disposed) {
      _syncScopeVisits();
      _ensureSyncIdentity();
      if (presentationSource == null) messageWindows.synchronize();
      super.notifyListeners();
    }
  }

  /// Outside-channel mention actions from the latest send per draft scope
  /// (Web MessageInput `pendingMentionActions`).
  final Map<String, List<Map<String, dynamic>>> pendingMentionActions = {};

  List<Map<String, dynamic>> pendingMentionsFor({bool thread = false}) =>
      (presentationSource?.pendingMentionActions ??
          pendingMentionActions)[draftScope(thread: thread)] ??
      const [];

  void dismissPendingMention(String resolutionId, {bool thread = false}) {
    if (presentationSource != null) {
      presentationSource!.dismissPendingMention(resolutionId, thread: thread);
      return;
    }
    final scope = draftScope(thread: thread);
    final list = pendingMentionActions[scope];
    if (list == null) return;
    pendingMentionActions[scope!] = [
      for (final a in list)
        if (a['resolutionId'] != resolutionId) a,
    ];
    notifyListeners();
  }

  /// Send-receipt warnings of the latest send per draft scope (Web MessageInput
  /// `unresolvedMentionHandles` / `deliveryWarnings`): handles the server could
  /// not resolve to a member, and whether a DM's agent is stopped.
  final Map<String, ({List<String> unresolved, bool agentStopped})>
  sendWarnings = {};

  ({List<String> unresolved, bool agentStopped}) sendWarningsFor({
    bool thread = false,
  }) =>
      (presentationSource?.sendWarnings ?? sendWarnings)[draftScope(
        thread: thread,
      )] ??
      (unresolved: const [], agentStopped: false);

  /// The banner's X (Web `dismissUnresolvedMentionWarning` /
  /// `dismissDeliveryWarning`).
  void dismissSendWarnings({bool thread = false}) {
    if (presentationSource != null) {
      presentationSource!.dismissSendWarnings(thread: thread);
      return;
    }
    final scope = draftScope(thread: thread);
    if (scope == null || sendWarnings.remove(scope) == null) return;
    notifyListeners();
  }

  /// `POST /messages/mention-actions/execute`; returns ids whose result
  /// status matches the action (`queued` for notify, `delivered` for add).
  Future<Set<String>> executeMentionActions(
    String action,
    List<String> resolutionIds,
  ) async => (await executeMentionActionResults(action, resolutionIds)).entries
      .where((r) => mentionActionSucceeded(action, r.value))
      .map((r) => r.key)
      .toSet();

  /// Web `mentionActionSucceeded`.
  static bool mentionActionSucceeded(String action, Map? result) =>
      result?['status'] == (action == 'notify' ? 'queued' : 'delivered');

  /// The per-row results of `POST /messages/mention-actions/execute`, by
  /// resolution id (a row the server did not answer for is absent).
  Future<Map<String, Map<String, dynamic>>> executeMentionActionResults(
    String action,
    List<String> resolutionIds,
  ) async {
    final value = await command(
      'POST',
      '/messages/mention-actions/execute',
      data: {'action': action, 'resolutionIds': resolutionIds},
    );
    return {
      if (value is Map && value['results'] is List)
        for (final r in (value['results'] as List).whereType<Map>())
          if (r['resolutionId'] is String)
            r['resolutionId'] as String: Map<String, dynamic>.from(r),
    };
  }

  String? draftScope({bool thread = false}) => thread
      ? (threadParentMessageId == null ? null : 'thread:$threadParentMessageId')
      : channel?.id;
  bool get conversationPaused =>
      channel != null && channelConversionBlocksSending(channel!.json);
  List<UploadDraft> uploads({bool thread = false}) =>
      (presentationSource?._uploads ?? _uploads)[draftScope(thread: thread)] ??
      [];
  bool uploadsReady({bool thread = false}) =>
      !composer.sends.queued(draftScope(thread: thread)) &&
      uploads(thread: thread)
          .every((u) => u.id != null || u.queued && u.error == null);

  /// Web MessageInput composer error banner per draft scope. Each message is a
  /// [CauseNotice] bound to the composer visit that raised it (Web resets the
  /// banner when the channel changes) and, for send blocks, to the upload
  /// state that caused it, so it cannot outlive its cause. The store lives on
  /// the session source; the visit is this controller's.
  final CauseNotices<String> _composerNotices = CauseNotices<String>();
  String? composerErrorFor({bool thread = false}) {
    final scope = draftScope(thread: thread);
    return scope == null
        ? null
        : (presentationSource?._composerNotices ?? _composerNotices).messageFor(
            scope,
          );
  }

  /// Web MessageInput `setError` outside a file selection (for example the
  /// folder notices of a drop). Null clears the banner. [whileTrue] is the
  /// cause: the banner disappears the moment it turns false.
  void setComposerError(
    String? message, {
    bool thread = false,
    bool Function()? whileTrue,
  }) {
    final scope = draftScope(thread: thread);
    if (scope == null) return;
    if (_raiseComposerNotice(scope, message, whileTrue: whileTrue)) {
      notifyListeners();
    }
  }

  bool _raiseComposerNotice(
    String scope,
    String? message, {
    bool Function()? whileTrue,
  }) {
    final visit = _visitOf(scope);
    return (presentationSource?._composerNotices ?? _composerNotices).raise(
      scope,
      message,
      whileTrue: () => visit == _visitOf(scope) && (whileTrue?.call() ?? true),
    );
  }

  /// Composer visits: a draft scope is "visited" from the moment it becomes
  /// the open channel / thread until another one replaces it. Notices record
  /// the visit they were raised in and read as gone in a later one (Web
  /// MessageInput resets its banners on channel change).
  final Map<String, int> _scopeVisits = {};
  String? _visitedChannelScope, _visitedThreadScope;

  int _visitOf(String scope) {
    _syncScopeVisits();
    return _scopeVisits[scope] ?? 0;
  }

  void _syncScopeVisits() {
    final channelScope = draftScope(), threadScope = draftScope(thread: true);
    if (channelScope != _visitedChannelScope) {
      _leaveScope(_visitedChannelScope);
      _visitedChannelScope = channelScope;
    }
    if (threadScope != _visitedThreadScope) {
      _leaveScope(_visitedThreadScope);
      _visitedThreadScope = threadScope;
    }
  }

  void _leaveScope(String? scope) {
    if (scope == null) return;
    _scopeVisits[scope] = (_scopeVisits[scope] ?? 0) + 1;
    // Web resets the delivery warnings with the channel.
    (presentationSource?.sendWarnings ?? sendWarnings).remove(scope);
  }

  /// Shared content hand-off (docs/sharing-evidence.md): the composer opens
  /// from device data and the destination is checked afterwards. While a scope
  /// is held, Send is off: `null` = still checking, a string = the check
  /// refused it and this notice (an English key) shows on the composer. The
  /// draft and the staged files stay either way.
  Map<String, String?> get _shareHolds => composer.uploads.holds;
  Map<String, String?> get shareHolds =>
      presentationSource?.shareHolds ?? _shareHolds;
  bool sharedBlocked({bool thread = false}) =>
      !thread && shareHolds.containsKey(draftScope());
  String? shareNoticeFor({bool thread = false}) =>
      thread ? null : shareHolds[draftScope()];

  void holdShared(String scope) {
    final source = presentationSource;
    if (source != null) return source.holdShared(scope);
    if (_shareHolds.containsKey(scope)) return;
    _shareHolds[scope] = null;
    notifyListeners();
  }

  /// The check passed (or could not be made: the server still decides on send).
  void releaseShared(String scope) {
    final source = presentationSource;
    if (source != null) return source.releaseShared(scope);
    if (!_shareHolds.containsKey(scope) || _shareHolds[scope] != null) return;
    _shareHolds.remove(scope);
    notifyListeners();
  }

  /// The check refused the destination: Send stays off and [notice] shows.
  void blockShared(String scope, String notice) {
    final source = presentationSource;
    if (source != null) return source.blockShared(scope, notice);
    if (_disposed || _shareHolds[scope] == notice) return;
    _shareHolds[scope] = notice;
    notifyListeners();
  }

  /// A destination that does not exist on the server yet (a new DM): the
  /// composer opens on this local row; [adoptResolvedDm] swaps in the real one.
  static RaftChannel pendingDm({
    required String key,
    required String serverId,
    required String name,
  }) => RaftChannel({
    'id': 'pending-dm:$key',
    'name': name,
    'type': 'dm',
    'joined': true,
    'serverId': serverId,
    'peerDisplayName': name,
    'pendingDm': true,
  });
  static bool isPendingDm(RaftChannel? c) => c?.json['pendingDm'] == true;

  /// The new DM exists now: its draft, staged files and hold move from the
  /// local row to [real], which takes its place in the lists and, when open,
  /// as the selection.
  Future<void> adoptResolvedDm(String pendingId, RaftChannel real) async {
    final source = presentationSource;
    if (source != null) return source.adoptResolvedDm(pendingId, real);
    if (_disposed) return;
    final wasOpen = channel?.id == pendingId;
    composer.adoptScope(pendingId, real.id);
    _channelRowSeq[real.id] = ++_channelReadSeq;
    if (wasOpen) channel = real;
    _putChannelRow(real);
    notifyListeners();
    if (wasOpen) {
      await selectChannel(
        [
          ...channels,
          ...dms,
        ].firstWhere((c) => c.id == real.id, orElse: () => real),
        autoRead: false,
      );
    }
  }

  /// A joined copy of a public channel's row while its join is in flight; the
  /// original goes back when [joined] is false.
  void putSharedChannelRow(RaftChannel row) {
    final source = presentationSource;
    if (source != null) return source.putSharedChannelRow(row);
    _channelRowSeq[row.id] = ++_channelReadSeq;
    _putChannelRow(row);
  }

  /// Server `LEGACY_ATTACHMENT_SAFE_LIMIT_BYTES`: the multipart request must
  /// stay below the edge request-body boundary.
  static const multipartUploadSafeLimitBytes = 90 * 1024 * 1024;

  /// The file ceiling this client can honour from an upload-capabilities
  /// body. With direct uploads enabled the server advertises the plan's
  /// single-file `maxBytes`, but this client always posts multipart, so the
  /// legacy transport ceiling applies. Null when the body is unusable.
  static int? uploadLimitFromCapabilities(dynamic value) {
    final max = value is Map ? value['maxBytes'] : null;
    if (max is! num || !max.isFinite || max <= 0) return null;
    final bytes = max.toInt();
    return value['directUploadEnabled'] == true &&
            bytes > multipartUploadSafeLimitBytes
        ? multipartUploadSafeLimitBytes
        : bytes;
  }

  /// Web `resolveAttachmentUploadLimitBytes`: the server ceiling from
  /// `GET /attachments/upload-capabilities`, or null when it is unusable.
  Future<int?> uploadLimitBytes() async {
    try {
      return uploadLimitFromCapabilities(
        await query('/attachments/upload-capabilities'),
      );
    } catch (_) {
      return null;
    }
  }

  Future<void> attachSelection(
    List<({String name, Uint8List bytes})> files, {
    bool thread = false,
    required String Function(String key, Map<String, Object> args) text,
  }) {
    final source = presentationSource;
    if (source != null) {
      return source.attachSelection(files, thread: thread, text: text);
    }
    if (draftScope(thread: thread) == null || _disposed) {
      return Future<void>.value();
    }
    return composer.uploads.select(files, _uploadVisit(thread), text: text);
  }

  String _uploadAuthority() => jsonEncode([
    client.generation,
    client.user?.id,
    client.serverId,
    server?.id,
    server?.string('role'),
    channel?.joined,
    channel?.archived,
    channel?.json['channelCapabilities'],
  ]);

  @visibleForTesting
  static String uploadErrorText(Object failure) =>
      WorkspaceUploads.uploadErrorText(failure);

  /// A 403 whose reason is membership revocation (not a transient failure).
  static bool _isNotServerMember(Object failure) =>
      failure is RaftApiException &&
      failure.status == 403 &&
      failure.message == 'Not a member of this server';

  ComposerUploadVisit _uploadVisit(bool thread) {
    final scope = draftScope(thread: thread)!;
    final generation = ledger.generation, authority = _uploadAuthority();
    return ComposerUploadVisit(
      scope: scope,
      channelId: channel?.id,
      thread: thread,
      selectionCurrent: () =>
          !_disposed &&
          generation == ledger.generation &&
          scope == draftScope(thread: thread) &&
          authority == _uploadAuthority(),
      resolveThread: ensureThread,
      limit: uploadLimitBytes,
      upload: (id, draft, progress) => draft.local != null
          ? client.uploadStream(
              id,
              draft.local!.openRead,
              draft.sizeBytes,
              draft.filename,
              cancellation: draft.cancel,
              onProgress: progress,
              mimeType: composerMimeType(draft.filename),
            )
          : client.upload(
              id,
              draft.bytes,
              draft.filename,
              cancellation: draft.cancel,
              onProgress: progress,
              mimeType: composerMimeType(draft.filename),
            ),
      persist: _saveDraftScope,
      notice: (message) => _raiseComposerNotice(scope, message),
    );
  }

  Future<void> attachUpload(
    String name,
    Uint8List bytes, {
    bool thread = false,
  }) async {
    final source = presentationSource;
    if (source != null) return source.attachUpload(name, bytes, thread: thread);
    if (draftScope(thread: thread) == null || _disposed) return;
    await composer.uploads.attach(name, bytes, _uploadVisit(thread));
  }

  Future<void> retryUpload(UploadDraft draft, {bool thread = false}) async {
    final source = presentationSource;
    if (source != null) return source.retryUpload(draft, thread: thread);
    if (draftScope(thread: thread) == null || _disposed) return;
    await composer.uploads.retry(draft, _uploadVisit(thread));
  }

  Future<void> _transferUpload(
    UploadDraft draft, {
    required bool thread,
  }) async {
    if (draftScope(thread: thread) == null || _disposed) return;
    await composer.uploads.transfer(draft, _uploadVisit(thread));
  }

  void removeUpload(UploadDraft draft, {bool thread = false}) {
    final source = presentationSource;
    if (source != null) return source.removeUpload(draft, thread: thread);
    composer.uploads.remove(draft, _saveDraftScope);
  }

  final _readState = ReadStateLedger();
  ReadStateLedger get readState => presentationSource?.readState ?? _readState;

  /// Workspace-owned page snapshots (Activity, Saved) that survive
  /// navigation; dropped on every server or account switch.
  final _resourceSnapshots = ResourceSnapshotCache();
  ResourceSnapshotCache get resourceSnapshots =>
      presentationSource?.resourceSnapshots ?? _resourceSnapshots;
  Map<String, Set<String>> get visibleIds => messageWindows.visibleIds;
  bool hasNewer = false, threadHasMore = false;
  bool threadHistoryLimited = false, historyLimited = false;
  String? highlightedMessageId;

  /// Pending unknown context; cross-channel cached tails are not that context.
  String? get pendingMessageContextChannelId =>
      messageWindows.pendingContextChannelId;
  List<RaftRecord> servers = [];
  List<RaftChannel> channels = [], dms = [];
  Map<String, int> unread = {};

  /// Source messageStore `mentionFlags`: conversations whose unread messages
  /// mention the member (unread snapshot `hasMention`, a live `message:new`
  /// that mentions the member). Cleared with the unread count.
  Set<String> mentions = {};

  /// Unread summary `hasNew`: non-joined public channels with messages past
  /// the cursor (no exact count). Lights the quiet dot of a non-joined row;
  /// cleared with the unread count (Web `newFlags`).
  Set<String> newFlags = {};
  Map<String, dynamic> sidebarOrder = {};
  RaftRecord? server;
  bool can(String capability, {RaftChannel? resource}) => raftCan(
    server?.string('role'),
    capability,
    channelCapabilities: resource?.json['channelCapabilities'] is Map
        ? Map<String, dynamic>.from(resource!.json['channelCapabilities'])
        : null,
  );
  RaftChannel? channel;
  WorkspaceThreadIdentity? _threadIdentity;
  MessageWindowRequest? _threadVisit;
  bool _threadResolutionLoading = false, _threadParentLoading = false;
  // ignore: raft_lints/avoid_imperative_notice_state -- Cleared when a thread opens or closes and hidden whenever threadIdentity is stale.
  String? _threadResolutionError;

  WorkspaceThreadIdentity? get threadIdentity =>
      _threadVisit?.current == true ? _threadIdentity : null;
  bool get threadResolutionLoading =>
      threadIdentity != null && _threadResolutionLoading;
  bool get threadParentLoading =>
      threadIdentity != null && _threadParentLoading;
  String? get threadResolutionError =>
      threadIdentity == null ? null : _threadResolutionError;
  RaftMessage? get presentedThreadParent => _threadIdentity == null
      ? threadParent
      : threadIdentity != null &&
            threadParent?.id == threadIdentity!.parentMessageId &&
            threadParent?.channelId == threadIdentity!.parentChannelId
      ? threadParent
      : null;
  String? get threadParentMessageId =>
      threadIdentity?.parentMessageId ??
      (_threadIdentity == null ? threadParent?.id : null);
  String? get threadParentChannelId =>
      threadIdentity?.parentChannelId ??
      (_threadIdentity == null ? threadParent?.channelId : null);

  RaftChannel? _threadChannelResource(String? id) => [
    ?channel,
    ...channels,
    ...dms,
  ].where((value) => value.id == id).firstOrNull;
  String? _threadResourceAuthority(String? id) {
    final resource = _threadChannelResource(id);
    return resource == null
        ? null
        : messageWindowAuthority(server?.string('role'), resource.json);
  }

  RaftChannel? get threadSourceChannel =>
      _threadChannelResource(threadParentChannelId);

  RaftMessage? threadParent;
  String? threadChannelId;
  bool loading = false,
      channelLoading = false,
      threadLoading = false,
      loadingOlder = false,
      hasMore = false;
  bool connected = false;
  // ignore: raft_lints/avoid_imperative_notice_state -- One-shot toast consumed by WorkspaceView; cleared on channel open, jump, send start, server switch and server revoke.
  String? error;

  /// Source messageStore `contextLoadError` for the open channel: the jump
  /// target's context failed to load and the latest window replaced it.
  /// `notFound` -> ChatPanel's "Message not found" banner; `beyondHistory` ->
  /// the plan history-limit banner. Cleared by the next channel open or jump.
  /// Bound to the channel and server it was raised for: it reads as null in
  /// any other one (a channel replaced without a re-open, a server switch).
  MessageContextLoadError? get contextLoadError {
    if (_contextFailure?.$2 != (channel?.id, server?.id)) {
      _contextFailure = null;
    }
    return _contextFailure?.$1;
  }

  set contextLoadError(MessageContextLoadError? value) => _contextFailure =
      value == null ? null : (value, (channel?.id, server?.id));

  (MessageContextLoadError, (String?, String?))? _contextFailure;

  final navigation = WorkspaceNavigation();
  String _unboundSection = 'chat';
  String? _navigationAuthority;
  String get navigationAuthority => jsonEncode([
    client.origin,
    client.generation,
    client.user?.id,
    server?.id,
    client.serverId,
    server?.string('role'),
  ]);
  void bindNavigation() {
    if (server == null) return;
    final authority = navigationAuthority;
    if (_navigationAuthority == authority) return;
    final slug =
        [
          server!.string('slug'),
          server!.id,
          client.serverId,
        ].whereType<String>().where((value) => value.isNotEmpty).firstOrNull ??
        '_';
    final initial = _navigationAuthority == null
        ? WorkspaceNavigation.locationForSection(
            slug,
            _unboundSection,
            channelId: channel?.id,
            dm: channel?.type == 'dm',
          )
        : navigation.location.serverSlug == slug
        ? navigation.location
        : WorkspaceNavigation.locationForSection(
            slug,
            mobileNavigation ? 'home' : _unboundSection,
            channelId: null,
          );
    _navigationAuthority = authority;
    navigation.bind(authority, initial);
  }

  RaftLocation get location {
    bindNavigation();
    return navigation.location;
  }

  int get navigationRevision {
    bindNavigation();
    return navigation.revision;
  }

  String get section {
    bindNavigation();
    if (server == null) return _unboundSection;
    final projected = navigation.section;
    // SettingsPanel.tsx:7882–7894 retains the requested URL while a denied
    // tab resolves to Account. It does not redirect the entire workspace.
    return navigation.location.route == RaftRoute.settings &&
            !canVisitSection(projected)
        ? 'settings'
        : projected;
  }

  set section(String value) {
    _unboundSection = value;
    final firstBind = _navigationAuthority == null && server != null;
    bindNavigation();
    if (server != null && !firstBind) {
      navigation.selectSection(
        value,
        channelId: channel?.id,
        dm: channel?.type == 'dm',
      );
    }
  }

  int get channelGeneration => messageWindows.channelGeneration;
  int get threadGeneration => messageWindows.threadGeneration;

  /// Accepted thread summaries ("N replies" rows) per parent channel, keyed
  /// by parent message. Like the channel's message window they outlive a
  /// channel switch, so a revisit or a prefetched first open renders its
  /// reply rows in the same frame as the messages.
  final Map<String, Map<String, dynamic>> _pageSummariesByChannel = {};
  Map<String, Map<String, dynamic>> get _summariesByChannel =>
      presentationSource?._summariesByChannel ?? _pageSummariesByChannel;

  /// Summaries of the selected channel's messages.
  Map<String, dynamic> get threadSummaries {
    final id = channel?.id;
    return id == null
        ? <String, dynamic>{}
        : _summariesByChannel.putIfAbsent(id, () => {});
  }

  set threadSummaries(Map<String, dynamic> value) {
    final id = channel?.id;
    if (id != null) _summariesByChannel[id] = value;
  }

  /// Summaries held for [channelId] (selected or not).
  Map<String, dynamic> threadSummariesFor(String channelId) =>
      Map.unmodifiable(_summariesByChannel[channelId] ?? const {});
  List<RaftMessage> get messages => messageWindows.rows(channel?.id);

  /// Same membership as [messages], in constant time (no list projection).
  bool presentsMessage(String? id) =>
      messageWindows.retainsContextRows(channel?.id) &&
      messageWindows.containsVisible(channel?.id, id);

  /// Same membership as [replies], in constant time.
  bool presentsReply(String? id) =>
      messageWindows.containsVisible(threadChannelId, id);

  List<RaftMessage> get replies => messageWindows.rows(threadChannelId);

  /// Web display order uses server sequence, with a display-only coordinate
  /// for pending rows. Context windows keep their existing admission policy.
  List<RaftMessage> timeline({bool thread = false, List<RaftMessage>? rows}) =>
      messageWindows.project(
        rows ?? (thread ? replies : messages),
        draftScope(thread: thread),
        context:
            !thread &&
            (hasNewer || pendingMessageContextChannelId == channel?.id),
      );

  /// Stable presentation identity: a row sent from this session keeps its
  /// optimistic key after the server copy replaces it, so the timeline
  /// updates that row in place instead of removing and inserting one.
  String messageKey(RaftMessage message) {
    if (presentationSource != null) {
      return presentationSource!.messageKey(message);
    }
    final randomId = message.json['randomId'];
    return randomId is String && composer.sends.localId(randomId)
        ? 'optimistic-$randomId'
        : message.id;
  }

  /// Whether [message] is an optimistic row still waiting for the server.
  static bool isPendingSend(RaftMessage message) =>
      message.json['pendingSend'] == true;

  static bool _rateLimited(Object e) => e is RaftApiException && e.rateLimited;

  /// A background refresh that failed is only an error worth showing when its
  /// scope is still the open one: a rate limit, or a fence tripped because the
  /// account or workspace changed (a deleted or left server), is not.
  static bool _quietFailure(Object e) => _rateLimited(e) || _staleScope(e);

  /// A request fenced by an account or workspace change: its owner moved on,
  /// so it reports nothing (also for the user's own actions, which are
  /// otherwise reported, rate limits included).
  static bool _staleScope(Object e) => e is RaftApiException && e.staleScope;

  void setError(String? value) {
    final source = presentationSource;
    if (source != null) {
      // ignore: raft_lints/avoid_imperative_notice_state -- Delegates to the session source.
      source.setError(value);
      return;
    }
    error = value;
    notifyListeners();
  }

  /// Hydrates the account's real server directory without selecting a server.
  /// The app root uses this before exposing the global chooser. Offline rows
  /// retain the existing account-scoped cache contract; fresh authority and
  /// cache writes share the membership reducer's request/revocation fences.
  ///
  /// With [cachedFirst] (cold start) an accepted on-device directory is
  /// adopted at once, so the remembered server and its cached channels paint
  /// without waiting for the network; [revalidateServerDirectory] then
  /// replaces it in the background.
  Future<bool> loadServerDirectory({
    bool cachedFirst = false,
    void Function()? onCacheMiss,
  }) => _burstAt.directoryRead(() async {
    if (!ownsClient) {
      throw StateError('A borrowed editor cannot load the server directory.');
    }
    final generation = client.generation,
        principal = client.user?.id,
        request = ++_membershipRequest,
        revision = _membershipRevision;
    bool current() =>
        !_disposed &&
        generation == client.generation &&
        principal == client.user?.id &&
        request == _membershipRequest &&
        revision == _membershipRevision;
    if (principal == null) return false;
    if (client.restoredOffline || cachedFirst) {
      final cached = await _cached('servers', '', server: '');
      if (!current()) return false;
      if (cached is List && cached.isNotEmpty) {
        servers = cached
            .map((row) => RaftRecord(Map<String, dynamic>.from(row)))
            .where((server) => !_revokedServers.contains(server.id))
            .toList();
        notifyListeners();
        if (cachedFirst) unawaited(revalidateServerDirectory());
        return true;
      }
    }
    onCacheMiss?.call();
    final accepted = await client.servers();
    if (!current()) return false;
    _revokedServers.removeAll(accepted.map((server) => server.id));
    servers = accepted;
    await _save(
      'servers',
      '',
      accepted.map((server) => server.json).toList(),
      server: '',
    );
    if (!current()) return false;
    notifyListeners();
    return true;
  });

  /// Channel and DM rows saved on this device for [serverId], or null. A
  /// display seed (for example the share picker) and never an authorization
  /// grant: callers revalidate before acting on a row.
  Future<List<RaftChannel>?> cachedChannelDirectory(String serverId) async {
    if (_disposed || _revokedServers.contains(serverId)) return null;
    final cached = await _cached('channels', '', server: serverId);
    if (cached is! Map || _disposed || _revokedServers.contains(serverId)) {
      return null;
    }
    try {
      return [
        for (final key in const ['channels', 'dms'])
          for (final row in cached[key] as List)
            RaftChannel(Map<String, dynamic>.from(row as Map)),
      ];
    } catch (_) {
      return null;
    }
  }

  /// Background replacement of a cache-first directory once the restored
  /// session is validated. A server missing from the fresh directory is
  /// revoked (its on-device data is purged and, when selected, its workspace
  /// closes); a changed role replaces the selected server's authority, which
  /// retires role-bound windows and revalidates the open channel. A failure
  /// keeps the accepted directory (offline).
  Future<void> revalidateServerDirectory() => _burstAt.directoryRead(() async {
    final principal = client.user?.id,
        request = ++_membershipRequest,
        revision = _membershipRevision;
    bool current() =>
        !_disposed &&
        principal != null &&
        principal == client.user?.id &&
        request == _membershipRequest &&
        revision == _membershipRevision;
    await client.sessionValidated;
    if (!current() || client.restoredOffline) return;
    final List<RaftRecord> accepted;
    try {
      accepted = await client.accountServers();
    } catch (_) {
      return;
    }
    if (!current()) return;
    final ids = {for (final s in accepted) s.id};
    for (final held in [...servers]) {
      if (!ids.contains(held.id)) revokeServer(held.id);
    }
    final selected = server;
    if (selected != null && !ids.contains(selected.id)) {
      revokeServer(selected.id);
    }
    _revokedServers.removeAll(ids);
    servers = accepted;
    _save('servers', '', accepted.map((s) => s.json).toList(), server: '');
    final held = server;
    final fresh = held == null
        ? null
        : accepted.where((s) => s.id == held.id).firstOrNull;
    if (held != null && fresh != null) {
      final role = fresh.string('role');
      if (role != held.string('role')) {
        applyMembershipRole({
          'userId': principal,
          'serverId': held.id,
          'role': role,
        });
        server = fresh;
        notifyListeners();
        await refreshChannels();
        final open = channel;
        if (!_disposed && open != null && server?.id == held.id) {
          unawaited(
            selectChannel(
              open,
              navigate: false,
              preserveThread: true,
              autoRead: false,
            ),
          );
        }
        return;
      }
      if (jsonEncode(fresh.json) != jsonEncode(held.json)) server = fresh;
    }
    notifyListeners();
  });

  Future<void> bootstrap() async {
    final source = presentationSource;
    if (source != null) {
      return source.bootstrap();
    }
    if (!ownsClient) {
      throw StateError('A borrowed editor cannot bootstrap a session.');
    }
    loading = true;
    error = null;
    notifyListeners();
    try {
      final cached = await _cached('servers', '', server: '');
      if (cached is List) {
        servers = cached
            .map((e) => RaftRecord(Map<String, dynamic>.from(e)))
            .where((s) => !_revokedServers.contains(s.id))
            .toList();
      }
      notifyListeners();
      if (servers.isNotEmpty && client.restoredOffline) {
        await selectServer(servers.first);
        return;
      }
      servers = await client.servers();
      _revokedServers.removeAll(servers.map((s) => s.id));
      await _save(
        'servers',
        '',
        servers.map((s) => s.json).toList(),
        server: '',
      );
      if (servers.isNotEmpty) await selectServer(servers.first);
    } catch (e) {
      if (!_staleScope(e)) error = userErrorText(e, where: 'load servers');
    } finally {
      loading = false;
      notifyListeners();
    }
  }

  /// [onHydrated] runs once the on-device channels, selection window and
  /// unread counts of [next] are painted, before any network response; it
  /// does not run when nothing is cached. The returned future still settles
  /// only after the fresh channel lists (and the selected page) are applied.
  Future<void> selectServer(
    RaftRecord next, {
    void Function()? onHydrated,
    void Function()? onCacheMiss,
  }) async {
    // A live mobile page presents the source's session; the source selects.
    final source = presentationSource;
    if (source != null) {
      return source.selectServer(
        next,
        onHydrated: onHydrated,
        onCacheMiss: onCacheMiss,
      );
    }
    if (!ownsClient) {
      throw StateError('A borrowed editor cannot select a server.');
    }
    if (_revokedServers.contains(next.id)) return;
    server = next;
    // A workspace error belongs to the server it was raised in (it is a toast
    // not yet shown when no workspace view was mounted to consume it).
    error = null;
    _burstAt.clear();
    if (mobileNavigation) section = 'home';
    if (!canVisitSection(section)) section = 'chat';
    sidebarOrder = {};
    client.selectServer(next.id);
    activityUnread.start();
    entityDirectory.synchronize();
    // Agents, members and computers saved on this device paint with the
    // cached channels (first workspace frame); the preload revalidates them.
    final restoredEntities = entityDirectory.restored;
    // Saved translations and the server's translation gate paint with them,
    // so a cached row shows its translation at its first frame.
    final restoredTranslations = translations.restore();
    unawaited(entityDirectory.preload());
    followedThreads.start();
    composer.clearOwnedBy(this);
    _resetServerWindows(next.id);
    refreshMessageSyncFlag();
    loadSidebar();
    _revokedChannels.clear();
    _channelRowSeq.clear();
    // The new server's socket connects for the first time.
    _connectedBefore = _roomsJoinedPending = false;
    _gapSyncedThrough = null;
    hasNewer = false;
    final selectionAtStart = channelGeneration,
        threadAtStart = threadGeneration,
        navigationAtStart = navigationRevision;
    notifyListeners();
    final generation = client.generation;
    final replyIdentity = _syncIdentity();
    final cached = await _cached('channels', '');
    await restoredEntities;
    await restoredTranslations;
    if (generation != client.generation) return;
    if (cached is Map) {
      channels = (cached['channels'] as List)
          .map((e) => RaftChannel(Map<String, dynamic>.from(e)))
          .toList();
      dms = (cached['dms'] as List)
          .map((e) => RaftChannel(Map<String, dynamic>.from(e)))
          .toList();
      final last = await _cached('selection', '');
      final initial =
          [...channels, ...dms].where((c) => c.id == last).firstOrNull ??
          channels.where((c) => c.joined && !c.archived).firstOrNull;
      if (initial != null) {
        channel = initial;
        // A principal/role change before this point retires its summaries.
        final replyAuthority = replyIdentity == _syncIdentity()
            ? _replyToken(initial.id)
            : '';
        final request = messageWindows.request(
          allowed: () => generation == client.generation,
        );
        final page = await _cached('window', initial.id);
        if (generation != client.generation) return;
        if (_restoreCachedTail(
          initial.id,
          page,
          request,
          replyAuthority: replyAuthority,
          replace: true,
        )) {
          await _restoreDraft(initial.id);
        }
      }
      notifyListeners();
    }
    final cachedFrontiers = await _cached('read-frontiers', '');
    if (generation != client.generation) return;
    readState.restore(
      cachedFrontiers,
      serverId: next.id,
      principalId: client.user!.id,
    );
    final cachedUnread = await _cached('unread', '');
    if (generation != client.generation) return;
    if (cachedUnread is Map) {
      unread = {
        for (final entry in cachedUnread.entries)
          if (entry.value is int && entry.value >= 0)
            '${entry.key}': entry.value,
      };
    }
    notifyListeners();
    if (cached is Map) {
      onHydrated?.call();
    } else {
      onCacheMiss?.call();
    }
    final readGeneration = readState.generation;
    List<List<RaftChannel>> values;
    try {
      values = await Future.wait([
        client.channels(),
        client.channels(dm: true),
      ]);
    } catch (e) {
      if (_isNotServerMember(e) && generation == client.generation) {
        revokeServer(next.id);
        await recoverMembership();
        loading = false;
        notifyListeners();
        return;
      }
      if (generation == client.generation) {
        error = userErrorText(e, where: 'load channels');
        loading = false;
        notifyListeners();
      }
      return;
    }
    if (generation != client.generation) return;
    final accessible = [...values[0], ...values[1]].map((c) => c.id).toSet();
    for (final old in [...channels, ...dms]) {
      if (!accessible.contains(old.id)) _revokeChannel(old.id);
    }
    channels = values[0];
    dms = values[1];
    // A cached selection adopts its fresh row. When the fresh row changes
    // the window authority, the cached rows are not shown under it: the
    // selection is revalidated below (or by the newer selection in flight).
    final held = channel;
    final freshHeld = held == null
        ? null
        : [...channels, ...dms].where((c) => c.id == held.id).firstOrNull;
    var heldAuthorityChanged = false;
    if (held != null && freshHeld != null && !identical(held, freshHeld)) {
      final before = _windowAuthority();
      channel = freshHeld;
      heldAuthorityChanged = before != _windowAuthority();
    }
    // The cache-painted workspace is already on screen: show the fresh lists
    // (and any revocation) now, not after the unread read below.
    notifyListeners();
    await _save('channels', '', {
      'channels': channels.map((c) => c.json).toList(),
      'dms': dms.map((c) => c.json).toList(),
    });
    // The cache write yields: a sign-out meanwhile clears `client.user`.
    if (generation != client.generation) return;
    final principalId = client.user?.id;
    if (principalId == null) return;
    for (final c in [...channels, ...dms]) {
      readState.consumeSnapshot(
        c.json['readState'],
        serverId: next.id,
        principalId: principalId,
        scopeId: c.id,
        generationAtRequest: readGeneration,
      );
    }
    client.connect();
    await refreshUnread();
    final joined = channels.where((c) => c.joined && !c.archived).toList();
    if (joined.isNotEmpty &&
        selectionAtStart == channelGeneration &&
        threadAtStart == threadGeneration) {
      final selected =
          joined.where((c) => c.id == channel?.id).firstOrNull ?? joined.first;
      if (mobileNavigation && section == 'home') {
        channel = selected;
        await _restoreDraft(selected.id);
      } else {
        // A location the app root applied over the cache-painted workspace
        // (e.g. a remembered non-chat page) keeps its section.
        await selectChannel(
          selected,
          navigate: navigationAtStart == navigationRevision,
        );
      }
    } else if (heldAuthorityChanged &&
        channel != null &&
        identical(channel, freshHeld)) {
      await selectChannel(
        channel!,
        navigate: false,
        preserveThread: true,
        autoRead: false,
      );
    }
    notifyListeners();
  }

  void _persistReadState() {
    final serverId = client.serverId, principal = client.user?.id;
    if (serverId == null || principal == null) return;
    _save('read-frontiers', '', readState.exportFor(serverId, principal));
    _save('unread', '', Map.of(unread));
  }

  void _persistUnread() {
    if (client.serverId == null || client.user == null) return;
    _save('unread', '', Map.of(unread));
  }

  /// Latest activity seq per scope from the last unread snapshot; live
  /// messages raise it. Lets a read receipt clear a count without a GET.
  final Map<String, BigInt> _latestActivity = {};

  /// Source messageStore `message:new`: +1 for a conversation the member is
  /// not reading, never for the member's own message. Counts only what the
  /// unread snapshot counts (joined channels, DMs, tracked threads).
  void _receiveUnread(Map<String, dynamic> row) {
    final id = row['channelId'];
    if (id is! String || _revokedChannels.contains(id)) return;
    final seq = canonicalUint64(row['seq']);
    if (seq != null && seq > (_latestActivity[id] ?? BigInt.zero)) {
      _latestActivity[id] = seq;
    }
    // Source canAutoMarkLiveAppendRead: a live arrival in the open
    // conversation is read only if the user was just interacting; while the
    // user is idle it stays unread (Activity keeps its indicator). Phone
    // (owner decision 2026-10-12, docs/parity.md): a foreground page whose
    // timeline sits at the latest end reads it with no tap.
    if ((id == channel?.id || id == threadChannelId) &&
        _mayMarkRead(id) &&
        (RaftUserActivity.recent ||
            mobileNavigation && !_scrolledAway.contains(id))) {
      unawaited(markRead(id));
      return;
    }
    if (row['senderType'] == 'user' && row['senderId'] == client.user?.id) {
      return;
    }
    final known = [...channels, ...dms].where((c) => c.id == id).firstOrNull;
    final counted = known == null
        ? unread.containsKey(id)
        : known.type != 'channel' || known.joined;
    if (!counted) return;
    final serverId = client.serverId, principal = client.user?.id;
    final frontier = serverId == null || principal == null
        ? null
        : readState.state(serverId, principal, id);
    if (seq != null &&
        frontier != null &&
        BigInt.from(frontier['maxReadSeq']) >= seq) {
      return;
    }
    unread = {...unread, id: (unread[id] ?? 0) + 1};
    if (_mentionsMember(row)) mentions = {...mentions, id};
    _persistUnread();
    unawaited(prefetchLikelyChannels());
  }

  /// Source `mentionsCurrentUser`: a structured user mention of the member.
  bool _mentionsMember(Map<String, dynamic> row) {
    final me = client.user?.id, list = row['mentions'];
    return me != null &&
        list is List &&
        list.any((m) => m is Map && m['type'] == 'user' && m['id'] == me);
  }

  void _clearMention(String id) {
    if (mentions.contains(id)) mentions = {...mentions}..remove(id);
    if (newFlags.contains(id)) newFlags = {...newFlags}..remove(id);
  }

  /// Source `applyAcceptedReadStateProjection`: an accepted read frontier
  /// recounts the scope from held messages when they cover it, and clears
  /// it when it reaches the latest known activity. Otherwise the count
  /// waits for the next reconcile.
  void _projectReadState(String id) {
    final serverId = client.serverId, principal = client.user?.id;
    if (serverId == null || principal == null || !unread.containsKey(id)) {
      return;
    }
    final state = readState.state(serverId, principal, id);
    if (state == null) return;
    final maxRead = BigInt.from(state['maxReadSeq']);
    final rows = ledger.messages(id);
    var latest = _latestActivity[id];
    if (rows.isNotEmpty) {
      final last = RaftMessage(rows.last).seq;
      if (latest == null || last > latest) latest = last;
    }
    int? count;
    if (latest != null && maxRead >= latest) {
      count = 0;
    } else if (rows.isNotEmpty &&
        !messageWindows.isContext(id) &&
        !(id == channel?.id && hasNewer) &&
        RaftMessage(rows.first).seq <= maxRead) {
      count = rows
          .where(
            (row) =>
                RaftMessage(row).seq > maxRead &&
                !(row['senderType'] == 'user' && row['senderId'] == principal),
          )
          .length;
    }
    if (count == null || unread[id] == count) return;
    unread = {...unread, id: count};
    if (count == 0) _clearMention(id);
  }

  Future<void> refreshUnread() async {
    final source = presentationSource;
    if (source != null) {
      return source.refreshUnread();
    }
    final requestGeneration = readState.generation;
    final serverId = client.serverId, principal = client.user?.id;
    if (serverId == null || principal == null) return;
    try {
      final value = await client.get('/channels/unread', query: {'summary': 1});
      if (client.serverId != serverId || client.user?.id != principal) return;
      final counts = value is Map ? value['channels'] ?? value : {};
      if (counts is Map) {
        final next = <String, int>{};
        final nextMentions = <String>{};
        final nextNew = <String>{};
        for (final e in counts.entries) {
          final id = e.key.toString(), entry = e.value;
          if (entry is num) {
            next[id] = entry.toInt();
            continue;
          }
          if (entry is! Map) continue;
          final frontier = entry['readState'];
          final outcome = readState.consumeSnapshot(
            frontier,
            serverId: serverId,
            principalId: principal,
            scopeId: id,
            generationAtRequest: requestGeneration,
            // RisingWave projects this endpoint asynchronously. An absent row
            // cannot erase a newer positive HTTP/socket acknowledgement.
            authoritativeAbsence: false,
          );
          final state = readState.state(serverId, principal, id);
          final latest = frontier is Map && frontier['latestActivity'] is Map
              ? canonicalUint64(frontier['latestActivity']['seq'])
              : null;
          if (latest != null) _latestActivity[id] = latest;
          final fullyRead =
              state != null &&
              latest != null &&
              BigInt.from(state['maxReadSeq']) >= latest;
          next[id] = fullyRead
              ? 0
              : outcome == 'corrupt'
              ? (unread[id] ?? 0)
              : ((entry['unreadCount'] ?? 0) as num).toInt();
          if (entry['hasMention'] == true && next[id]! > 0) {
            nextMentions.add(id);
          }
          if (entry['hasNew'] == true) nextNew.add(id);
        }
        unread = next;
        mentions = nextMentions;
        newFlags = nextNew;
        _persistReadState();
      }
      notifyListeners();
      unawaited(prefetchLikelyChannels());
    } catch (_) {
      /* Keep accepted counts through transient failures. */
    }
  }

  final _prefetched = <String>{};
  bool _prefetching = false;

  /// Loads the latest page of channels the member is likely to open next
  /// (those with unread messages) in the background, so a first open shows
  /// content at once instead of waiting for the network. Never selects,
  /// joins or marks anything read; each channel is prefetched once per
  /// session, sequentially, and only while the same account/server/role and
  /// view permission hold.
  Future<void> prefetchLikelyChannels({int limit = 6}) async {
    // Not during startup: the restored selection (possibly hidden behind
    // mobile Home) is decided first and is never prefetched.
    if (_prefetching || _disposed || loading || channel == null) return;
    _prefetching = true;
    try {
      // A window's authority is per channel (role + that channel's record).
      String authorityOf(RaftChannel c) =>
          messageWindowAuthority(server?.string('role'), c.json);
      final role = server?.string('role'),
          identity = _syncIdentity(),
          generation = ledger.generation;
      bool stillCurrent() =>
          !_disposed &&
          role == server?.string('role') &&
          identity == _syncIdentity() &&
          generation == ledger.generation;
      final candidates = [
        for (final c in [...channels, ...dms])
          if (c.id != channel?.id &&
              !_prefetched.contains(c.id) &&
              !_revokedChannels.contains(c.id) &&
              messageWindows.accepted[c.id] == null &&
              (unread[c.id] ?? 0) > 0 &&
              can('viewChannel', resource: c))
            c,
      ].take(limit).toList();
      for (final c in candidates) {
        if (!stillCurrent()) return;
        _prefetched.add(c.id);
        final authority = authorityOf(c), reply = _replyToken(c.id);
        final request = messageWindows.request(
          allowed: () =>
              stillCurrent() &&
              reply == _replyToken(c.id) &&
              channel?.id != c.id &&
              messageWindows.accepted[c.id] == null &&
              !_revokedChannels.contains(c.id) &&
              authority == authorityOf(c) &&
              can('viewChannel', resource: c),
        );
        try {
          // The channel's task chips load with its first page.
          unawaited(MessageTaskCache.of(client).warm(this, c.id));
          final page = await MessageHistoryPages.read(this, request, c.id);
          if (!request.current) continue;
          final rows = acceptedWindowRows(page['messages'], c.id);
          if (rows.isEmpty) continue;
          final limited = page['historyLimited'] == true;
          messageWindows.acceptRows(request, c.id, rows, replace: true);
          await MessageHistoryPages.record(this, request, page);
          if (!request.current) continue;
          messageWindows.rememberWindow(
            request,
            c.id,
            authority,
            !limited && rows.length >= 50,
            reply,
            historyLimited: limited,
          );
          // Reply rows are part of the window: a first open renders them in
          // the same frame as the prefetched messages.
          _mergeThreadSummaries(c.id, page['threadSummariesByParentMessageId']);
        } catch (_) {
          // Best effort: the channel opens normally on demand.
        }
      }
    } finally {
      _prefetching = false;
    }
  }

  void _revokeChannel(String id) {
    final origin = client.origin,
        principal = client.user?.id,
        serverId = client.serverId;
    // Content just handed over from a share is kept when its destination is
    // refused: the composer says so and Send stays off, nothing is discarded.
    final keepShared = _shareHolds.containsKey(id);
    if (keepShared) {
      _shareHolds[id] = 'This conversation is no longer available. Your draft and files are kept.';
    }
    if (!keepShared && principal != null && serverId != null) {
      unawaited(discardSharedScope?.call(origin, principal, serverId, id));
    }
    final messageIds = <dynamic>{
      ...ledger.messages(id).map((m) => m['id']),
      ...threadRepliesSync.parentMessageIdsForChannel(id),
      if (_threadIdentity != null &&
          (_threadIdentity!.parentChannelId == id || threadChannelId == id))
        _threadIdentity!.parentMessageId,
      if (threadChannelId == id && threadParent != null) threadParent!.id,
    };
    final threadIds = <String>{
      for (final message in ledger.messages(id))
        if (message['threadChannelId'] is String) message['threadChannelId'],
      for (final summary in [
        ...?_summariesByChannel[id]?.values,
        for (final bucket in _summariesByChannel.values)
          for (final messageId in messageIds) ?bucket[messageId],
      ])
        if (summary is Map &&
            messageIds.contains(summary['parentMessageId']) &&
            summary['threadChannelId'] is String)
          summary['threadChannelId'],
      if ((threadParent?.channelId == id ||
              _threadIdentity?.parentChannelId == id) &&
          threadChannelId != null)
        threadChannelId!,
    };
    for (final scope in {id, ...threadIds}) {
      _revokedChannels.add(scope);
      _attachmentImages
        ?..synchronize(attachmentImageScope)
        ..invalidateChannel(scope);
      _attachmentUrls.invalidateChannel(scope);
      _playerSessions.invalidateChannel(scope);
      // Stored images, preview files and documents of the channel go too,
      // also when no decoded image was ever held this session.
      _purgeStoredChannel(scope);
      messageWindows.revokeChannel(scope);
      messageSync.revokeChannel(scope);
      threadRepliesSync.revokeChannel(scope);
      notificationPrefsSync.revokeChannel(scope);
      messageWindows.discardWindow(scope);
      unread.remove(scope);
      _clearMention(scope);
      _latestActivity.remove(scope);
      _summariesByChannel.remove(scope);
      messageWindows.revokeThreads(scope);
      if (serverId != null && principal != null) {
        readState.revoke(serverId, principal, scope);
      }
      composer.retireScope(scope, keepFiles: keepShared && scope == id);
      if (keepShared && scope == id) continue;
      _attempts.remove(scope);
    }
    for (final messageId in messageIds) {
      final scope = 'thread:$messageId';
      _attempts.remove(scope);
      composer.retireScope(scope);
      reactionViewer.remove(messageId);
      for (final bucket in _summariesByChannel.values) {
        bucket.remove(messageId);
      }
    }
    if (channel?.id == id && !keepShared) {
      channel = null;
      _missingConversationChannelId = id;
      messageWindows.channelGeneration++;
    }
    if (threadParent?.channelId == id ||
        _threadIdentity?.parentChannelId == id ||
        threadIds.contains(threadChannelId) ||
        threadChannelId == id) {
      closeThread();
    }
    _persistReadState();
    if (principal != null && serverId != null) {
      // Web keeps authored text on channel removal or a refused message read.
      // Snapshot it before the private-cache purge; never restore server files.
      final keptDrafts = {
        for (final scope in {
          id,
          ...threadIds,
          for (final messageId in messageIds) 'thread:$messageId',
        })
          scope: composer.draftRecord(scope, includeShareTokens: false),
      };
      _cacheWrites = _cacheWrites.then((_) async {
        try {
          final sharedDraft = keepShared
              ? await cache?.read(origin, principal, serverId, 'draft', id)
              : null;
          for (final scope in {id, ...threadIds}) {
            await cache?.revokeChannel(origin, principal, serverId, scope);
          }
          for (final entry in keptDrafts.entries) {
            await cache?.write(
              origin,
              principal,
              serverId,
              'draft',
              entry.key,
              entry.key == id && keepShared ? sharedDraft : entry.value,
            );
          }
        } catch (_) {
          if (!_disposed) {
            // ignore: raft_lints/avoid_imperative_notice_state -- Cache-purge device failures and background refresh failures raise a one-shot toast; stale-scope and rate-limited failures are suppressed (_quietFailure) and the error is cleared on server switch or revoke.
            setError(
              'Revoked workspace data could not be removed from this device.',
            );
          }
        }
      });
    }
  }

  void _purgeStoredChannel(String channelId) {
    final store = attachmentImageStore;
    if (store == null || presentationSource != null) return;
    unawaited(
      store
          .purgeChannel(
            AttachmentImageKey.storeIdentity(attachmentImageScope),
            channelId,
          )
          .catchError((Object _) {}),
    );
  }

  int _sidebarRequest = 0;
  Future<void> loadSidebar() async {
    if (presentationSource != null) return presentationSource!.loadSidebar();
    final request = ++_sidebarRequest;
    final generation = client.generation, id = client.serverId;
    final authority = jsonEncode([
      client.user?.id,
      server?.id,
      server?.string('role'),
    ]);
    bool accepts() =>
        !_disposed &&
        request == _sidebarRequest &&
        generation == client.generation &&
        id == client.serverId &&
        authority ==
            jsonEncode([client.user?.id, server?.id, server?.string('role')]);
    if (id == null) return;
    final cached = await _cached('sidebar-order', '');
    if (!accepts()) return;
    if (cached is Map) {
      sidebarOrder = Map<String, dynamic>.from(cached);
      notifyListeners();
    }
    try {
      final value = await client.get('/servers/$id/sidebar-order');
      if (!accepts()) return;
      sidebarOrder = Map<String, dynamic>.from(value);
      _save('sidebar-order', '', sidebarOrder);
      notifyListeners();
    } catch (_) {
      /* Cached order remains usable while offline. */
    }
  }

  // ---- sidebar-order writes (Source serverStore.updateSidebarOrder) -------

  static const _sectionFields = {
    'customSections',
    'sectionOrder',
    'sectionPlacements',
  };
  int _sidebarSectionSeq = 0, _sidebarSectionsInFlight = 0;
  Future<void> _sidebarSectionTail = Future.value();
  num? _sidebarConfirmedVersion;

  /// Applies [updates] to [sidebarOrder] at once (the row moves before the
  /// request returns), then PATCHes `/servers/:id/sidebar-order`. A request
  /// that touches sections carries the `sectionsVersion` the server demands
  /// (409 otherwise) and is queued behind the previous section write; a
  /// failure restores the previous order, a 409 reloads it. Never throws.
  Future<void> updateSidebarOrder(Map<String, dynamic> updates) async {
    final source = presentationSource;
    if (source != null) return source.updateSidebarOrder(updates);
    final id = server?.id ?? client.serverId;
    if (id == null || _disposed) return;
    final generation = client.generation;
    final previous = sidebarOrder;
    final next = {...previous, ...updates};
    final sections = updates.keys.any(_sectionFields.contains);
    sidebarOrder = next;
    _save('sidebar-order', '', next);
    notifyListeners();
    bool current() =>
        !_disposed &&
        (server?.id ?? client.serverId) == id &&
        client.generation == generation;

    Future<void> perform(int sequence, num? version, bool queued) async {
      if (!current()) return;
      try {
        final value = await client.request(
          'PATCH',
          '/servers/$id/sidebar-order',
          data: sections ? {...updates, 'sectionsVersion': ?version} : updates,
        );
        if (!current()) return;
        final reply = value is Map
            ? Map<String, dynamic>.from(value)
            : <String, dynamic>{};
        if (queued) _sidebarConfirmedVersion = reply['sectionsVersion'] as num?;
        if (queued && sequence != _sidebarSectionSeq) {
          // A newer section write is queued: keep its optimistic order and
          // only adopt the versions.
          sidebarOrder = {
            ...sidebarOrder,
            if (reply['sectionsVersion'] != null)
              'sectionsVersion': reply['sectionsVersion'],
            if (reply['pinnedVersion'] != null)
              'pinnedVersion': reply['pinnedVersion'],
          };
        } else {
          sidebarOrder = {...next, ...reply};
        }
        _save('sidebar-order', '', sidebarOrder);
        notifyListeners();
      } catch (error) {
        if (!current()) return;
        if (!queued || sequence == _sidebarSectionSeq) {
          sidebarOrder = previous;
          _save('sidebar-order', '', previous);
          notifyListeners();
        }
        if (error is RaftApiException && error.status == 409) {
          unawaited(loadSidebar());
        }
      }
    }

    if (!sections) return perform(0, null, false);
    final sequence = ++_sidebarSectionSeq;
    _sidebarConfirmedVersion ??= previous['sectionsVersion'] as num?;
    // An idle queue sends at once; otherwise this write waits for the
    // previous one so it carries the version that one produced.
    final run = _sidebarSectionsInFlight == 0
        ? perform(sequence, _sidebarConfirmedVersion, true)
        : _sidebarSectionTail.then(
            (_) => perform(sequence, _sidebarConfirmedVersion, true),
          );
    _sidebarSectionsInFlight++;
    _sidebarSectionTail = run;
    return run.whenComplete(() {
      if (--_sidebarSectionsInFlight == 0) _sidebarConfirmedVersion = null;
    });
  }

  /// Source channelStore.setActivityMuteState: patches the mute fields of one
  /// row in place (channels, DMs and the open channel).
  void setActivityMuteState(String id, Map<String, dynamic> state) {
    final source = presentationSource;
    if (source != null) return source.setActivityMuteState(id, state);
    RaftChannel patch(RaftChannel value) =>
        value.id == id ? RaftChannel({...value.json, ...state}) : value;
    channels = channels.map(patch).toList();
    dms = dms.map(patch).toList();
    if (channel != null) channel = patch(channel!);
    notifyListeners();
  }

  /// The channel row [id] as currently held (channels first, then DMs).
  RaftChannel? channelById(String id) =>
      [...channels, ...dms].where((c) => c.id == id).firstOrNull;

  /// Optimistic local unread for Mark as Unread (Source markUnread).
  void markUnreadLocally(String id) {
    final source = presentationSource;
    if (source != null) return source.markUnreadLocally(id);
    if ((unread[id] ?? 0) > 0) return;
    unread = {...unread, id: 1};
    notifyListeners();
  }

  /// Read order of channel-row authority. Every list read, single-row read
  /// and realtime row patch takes the next value when it starts; a row keeps
  /// the value of whatever last wrote it. An older response therefore never
  /// overwrites a newer row, and never revokes a row added after it started.
  int _channelReadSeq = 0, _channelListSeq = 0;
  final Map<String, int> _channelRowSeq = {};

  /// Full channel and DM list reload (Source `loadChannels` +
  /// `loadDMChannels`): server selection, `rooms:joined`, a role change, or a
  /// channel event without an id. Unchanged rows keep their objects.
  Future<void> refreshChannels() async {
    final source = presentationSource;
    if (source != null) {
      return source.refreshChannels();
    }
    final generation = ledger.generation;
    final read = ++_channelReadSeq;
    try {
      final lists = await Future.wait([
        client.channels(),
        client.channels(dm: true),
      ]);
      if (generation != ledger.generation || _disposed) return;
      // A list read that started later has already been applied.
      if (read < _channelListSeq) return;
      _channelListSeq = read;
      // Rows written after this read started keep their current state.
      final newer = {
        for (final entry in _channelRowSeq.entries)
          if (entry.value > read) entry.key,
      };
      _channelRowSeq.removeWhere((_, seq) => seq <= read);
      List<RaftChannel> merge(
        List<RaftChannel> fetched,
        List<RaftChannel> current,
      ) {
        final held = {for (final c in current) c.id: c};
        final fetchedIds = {for (final c in fetched) c.id};
        final next = [
          for (final c in fetched)
            if (!newer.contains(c.id))
              _reuseChannelRow(held[c.id], c)
            else if (held[c.id] != null)
              held[c.id]!,
        ];
        // A row added after this read started keeps its held position.
        for (var i = 0; i < current.length; i++) {
          final c = current[i];
          if (newer.contains(c.id) && !fetchedIds.contains(c.id)) {
            next.insert(i < next.length ? i : next.length, c);
          }
        }
        return next;
      }

      _acceptChannelLists(
        merge(lists[0], channels),
        merge(lists[1], dms),
        readmit: {
          for (final c in [...lists[0], ...lists[1]])
            if (!newer.contains(c.id)) c.id,
        },
      );
    } catch (e) {
      // A rate-limited background refresh is not an error to show: the held
      // lists stay, and the next refresh (after Retry-After) catches up.
      if (generation == ledger.generation && !_disposed && !_quietFailure(e)) {
        // ignore: raft_lints/avoid_imperative_notice_state -- Raised only for the current ledger generation and not for rate-limited or stale-scope failures; cleared on server switch or revoke.
        setError(userErrorText(e, where: 'refresh channels'));
      }
    }
  }

  /// The one place channel lists change after selection. A row missing from
  /// the next lists lost access and is revoked; [readmit] are rows the
  /// server just confirmed readable.
  void _acceptChannelLists(
    List<RaftChannel> nextChannels,
    List<RaftChannel> nextDms, {
    required Set<String> readmit,
  }) {
    final accessible = {
      for (final c in [...nextChannels, ...nextDms]) c.id,
    };
    for (final old in [...channels, ...dms]) {
      if (!accessible.contains(old.id)) _revokeChannel(old.id);
    }
    _revokedChannels.removeAll(readmit);
    channels = nextChannels;
    dms = nextDms;
    _save('channels', '', {
      'channels': channels.map((c) => c.json).toList(),
      'dms': dms.map((c) => c.json).toList(),
    });
    // A held share's destination (and a DM that is not on the server yet)
    // stays open when the lists do not carry it: the composer says why.
    if (channel != null &&
        !isPendingDm(channel) &&
        !(_shareHolds.containsKey(channel!.id) &&
            !accessible.contains(channel!.id))) {
      channel = [
        ...channels,
        ...dms,
      ].where((c) => c.id == channel!.id).firstOrNull;
      if (channel == null) {
        if (threadChannelId != null) {
          messageWindows.revokeChannel(threadChannelId!);
        }
        closeThread();
        messageWindows.channelGeneration++;
      }
    }
    notifyListeners();
  }

  /// A refreshed row equal to the held one keeps the held object, so sidebar
  /// rows and the open channel do not rebuild for an unchanged read.
  static RaftChannel _reuseChannelRow(RaftChannel? held, RaftChannel fresh) {
    if (held == null) return fresh;
    try {
      return jsonEncode(held.json) == jsonEncode(fresh.json) ? held : fresh;
    } catch (_) {
      return fresh;
    }
  }

  /// Whether the server's channel lists carry [row] for this viewer: private
  /// and joint channels only for members (`requiresExplicitMembership`).
  static bool _listsChannel(RaftChannel row) =>
      !const {'private', 'joint'}.contains(row.type) || row.joined;

  /// Replaces [next]'s row in place, or adds it (a new DM goes first, as the
  /// server's recency-ordered DM list would place it).
  void _putChannelRow(RaftChannel next) {
    final held = [...channels, ...dms].where((c) => c.id == next.id);
    if (held.length == 1 && identical(held.single, next)) return;
    final dm = next.type == 'dm';
    List<RaftChannel> put(List<RaftChannel> list, bool owns) {
      final index = list.indexWhere((c) => c.id == next.id);
      if (!owns) return index < 0 ? list : ([...list]..removeAt(index));
      if (index >= 0) return [...list]..[index] = next;
      return dm ? [next, ...list] : [...list, next];
    }

    _acceptChannelLists(put(channels, !dm), put(dms, dm), readmit: {next.id});
  }

  static String? _payloadChannelId(dynamic payload) {
    if (payload is! Map) return null;
    final id = payload['channelId'] ?? payload['id'];
    return id is String && id.isNotEmpty ? id : null;
  }

  /// Source `readApiChannel`: `{channel: row}` or a bare row with a name.
  static Map<String, dynamic>? _payloadChannelRow(dynamic payload) {
    if (payload is! Map) return null;
    final candidate = payload.containsKey('channel')
        ? payload['channel']
        : payload;
    if (candidate is! Map ||
        candidate['id'] is! String ||
        candidate['name'] is! String) {
      return null;
    }
    return Map<String, dynamic>.from(candidate);
  }

  /// Source channelRealtimeSync: a channel event changes one row. A carried
  /// row is patched in place; an id re-reads that one channel; only an event
  /// without an id reloads both lists.
  void _channelRealtime(String name, dynamic payload) {
    if (name == 'channel:updated') {
      final row = _payloadChannelRow(payload);
      if (row != null) {
        // A user-room event can carry another server's joint-channel row.
        final serverId = row['serverId'];
        if (serverId is String &&
            client.serverId != null &&
            serverId != client.serverId) {
          return;
        }
        _patchChannelRow(row);
        return;
      }
    }
    final id = _payloadChannelId(payload);
    if (id == null) {
      if (name != 'dm:new') unawaited(refreshChannels());
      return;
    }
    if (name == 'dm:new') {
      client.joinChannel(id);
      // Source addOrRefreshDM: a held DM moves first without a read.
      final index = dms.indexWhere((c) => c.id == id);
      if (index >= 0 && !_revokedChannels.contains(id)) {
        if (index > 0) {
          _acceptChannelLists(channels, [
            dms[index],
            for (var i = 0; i < dms.length; i++)
              if (i != index) dms[i],
          ], readmit: const {});
        }
        return;
      }
    }
    unawaited(refreshChannel(id));
  }

  /// Web `patchChannel` `hiddenAllChannel`: an admin hiding `#all` turns it
  /// into a private channel named `all`, which leaves the list.
  static bool isHiddenAllChannelRow(Map<String, dynamic> row) =>
      row['name'] == 'all' && row['type'] == 'private';

  void _patchChannelRow(Map<String, dynamic> patch) {
    final id = patch['id'] as String;
    final held = [...channels, ...dms].where((c) => c.id == id).firstOrNull;
    if (isHiddenAllChannelRow({...?held?.json, ...patch})) {
      _channelRowSeq[id] = ++_channelReadSeq;
      if (held != null) {
        _acceptChannelLists(
          channels.where((c) => c.id != id).toList(),
          dms.where((c) => c.id != id).toList(),
          readmit: const {},
        );
      }
      return;
    }
    // A projection lacks viewer fields (membership, capabilities): an unknown
    // or revoked row is read once instead of trusted.
    if (held == null || _revokedChannels.contains(id)) {
      unawaited(refreshChannel(id));
      return;
    }
    final next = _reuseChannelRow(held, RaftChannel({...held.json, ...patch}));
    if (!_listsChannel(next) || next.type == 'thread') {
      unawaited(refreshChannel(id));
      return;
    }
    _channelRowSeq[id] = ++_channelReadSeq;
    _putChannelRow(next);
  }

  final Map<String, Future<void>> _channelReads = {};
  final Set<String> _channelRereads = {};

  /// Source `ensureChannel(id, {refresh: true})`: reads one channel and
  /// upserts its row. Lost access (403/404, or a private row the lists would
  /// not carry) revokes that channel through the list revocation path.
  Future<void> refreshChannel(String id) {
    final pending = _channelReads[id];
    if (pending != null) {
      // The read in flight may predate this change: read once more after it.
      _channelRereads.add(id);
      return pending;
    }
    final read = _readChannel(id).whenComplete(() {
      _channelReads.remove(id);
      if (_channelRereads.remove(id) && !_disposed) {
        unawaited(refreshChannel(id));
      }
    });
    return _channelReads[id] = read;
  }

  Future<void> _readChannel(String id) async {
    final generation = ledger.generation, identity = _syncIdentity();
    final read = ++_channelReadSeq;
    bool current() =>
        !_disposed &&
        generation == ledger.generation &&
        identity == _syncIdentity() &&
        read > _channelListSeq &&
        read > (_channelRowSeq[id] ?? 0);
    RaftChannel? fresh;
    var lostAccess = false;
    try {
      final data = await client.get('/channels/$id');
      if (!current() || data is! Map || data['id'] != id) return;
      fresh = RaftChannel(Map<String, dynamic>.from(data));
      if (fresh.type == 'thread') return;
      final serverId = fresh.json['serverId'];
      if (serverId is String && serverId != client.serverId) fresh = null;
    } on RaftApiException catch (e) {
      if (!current() || (e.status != 403 && e.status != 404)) return;
      lostAccess = true;
    } catch (_) {
      // Transient failure: the held row stays until a later read.
      return;
    }
    _channelRowSeq[id] = read;
    final held = [...channels, ...dms].where((c) => c.id == id).firstOrNull;
    if (fresh != null && _listsChannel(fresh)) {
      _putChannelRow(_reuseChannelRow(held, fresh));
    } else if (held != null) {
      _acceptChannelLists(
        channels.where((c) => c.id != id).toList(),
        dms.where((c) => c.id != id).toList(),
        readmit: const {},
      );
    }
    // The server never emits `channel:removed`: this 403/404 is the way a lost
    // channel is noticed, so it runs that event's cleanup. A held row was
    // already revoked by the list update above.
    if (lostAccess) {
      if (held == null) _revokeChannel(id);
      if (_threadIdentity?.parentChannelId == id) closeThread();
      if (held == null) notifyListeners();
    }
  }

  bool _roomsJoinedPending = false, _connectedBefore = false;
  DateTime? _lastHeartbeat, _lastServerEvent;
  (int, BigInt)? _gapSyncedThrough;

  /// Source socketBridge `roomsJoined`: the server joined this socket's
  /// rooms, so missed messages can be resumed and the lists re-read.
  void _roomsJoined() {
    SourceActivityUnreadStore.activateForRooms(this);
    _roomsJoinedPending = false;
    unawaited(MessageHistoryEvents.resume(this));
    // A reconnect and a resume can both ask within a second; the tail read a
    // moment ago already covers them.
    if (_due('gap', freshness: const Duration(seconds: 3))) {
      unawaited(syncVisibleScopes());
    }
    if (_due('channels')) unawaited(refreshChannels());
    if (_due('unread')) unawaited(refreshUnread());
  }

  /// Server reads started by events and by returning to the foreground (not
  /// by the person asking) are skipped while the last one is this fresh.
  static const _burstFreshness = Duration(seconds: 15);
  final _burstAt = BurstReads();
  bool _due(String name, {Duration freshness = _burstFreshness}) =>
      _burstAt.due(name, freshness);

  /// Source `recordHeartbeat`: a server seq past the ledger watermark means
  /// a push was missed; the visible scopes read their gap.
  void _heartbeat(dynamic payload) {
    _lastHeartbeat = DateTime.now();
    // A lost `rooms:joined`: the connection's first heartbeat stands in.
    if (_roomsJoinedPending) {
      _roomsJoined();
      return;
    }
    final seq = payload is Map ? BigInt.tryParse('${payload['seq']}') : null;
    final synced = _gapSyncedThrough;
    if (seq == null ||
        seq <= ledger.watermark ||
        synced != null && synced.$1 == ledger.generation && seq <= synced.$2) {
      return;
    }
    unawaited(syncVisibleScopes(through: seq));
  }

  /// Source `recoverLiveSession` on return to the foreground: reconnect a
  /// dropped or silent socket (its `rooms:joined` resumes and gap-syncs), or
  /// resume and gap-sync on the live one; then refresh live counts.
  void resumeLiveSession() {
    if (_disposed || !ownsClient || client.serverId == null) return;
    // The Activity window and the cross-server summary are read at once, in
    // parallel with the resume handshake below (not after its reply).
    unawaited(activityUnreadIfBound?.refreshOnResume());
    resumes.value++;
    final now = DateTime.now();
    final heartbeat = _lastHeartbeat, inbound = _lastServerEvent;
    final silent =
        heartbeat != null &&
        inbound != null &&
        now.difference(heartbeat) > const Duration(seconds: 90) &&
        now.difference(inbound) > const Duration(seconds: 120);
    if (!client.connected || silent) {
      client.connect();
    } else {
      unawaited(MessageHistoryEvents.resume(this));
      if (_due('gap', freshness: const Duration(seconds: 3))) {
        unawaited(syncVisibleScopes());
      }
    }
    // Returning to the foreground (alt-tab on a desktop does it often): the
    // counts, sidebar and directory read less than 15 s ago are still fresh.
    if (_due('unread')) unawaited(refreshUnread());
    if (_due('sidebar')) loadSidebar();
    if (entityDirectory.started && _due('authors')) {
      entityDirectory.revalidateAuthors();
    }
    // Another session of this account may have joined or created a server;
    // the server tells only that server's room, so this is the one moment the
    // directory is re-read. On a desktop the engine reports window focus as
    // inactive/resumed, so this also runs when the window regains focus.
    if (_due('membership', freshness: _membershipResumeFreshness)) {
      unawaited(recoverMembership(coalesce: true));
    }
  }

  /// Focus flickers and quick app switches must not re-read the server list.
  static const _membershipResumeFreshness = Duration(seconds: 60);

  static const _gapPageLimit = 200;
  bool _gapSyncing = false;

  /// Source `syncVisibleScopes`: the open thread and the presented channel
  /// tail each read the messages after their newest row and append them to
  /// the held window. Nothing is cleared or reloaded.
  Future<void> syncVisibleScopes({BigInt? through}) async {
    if (_gapSyncing || _disposed) return;

    final thread = threadChannelId, main = channel?.id;
    final targets = [
      if (thread != null && !threadLoading && !messageWindows.contextHasNewer(thread)) thread,
      if (main != null &&
          main != thread &&
          !hasNewer &&
          !channelLoading &&
          pendingMessageContextChannelId != main)
        main,
    ];
    if (targets.isEmpty) return;
    final generation = ledger.generation;
    var complete = true;
    _gapSyncing = true;
    try {
      for (final id in targets) {
        if (!await _syncGap(id)) complete = false;
      }
    } finally {
      _gapSyncing = false;
    }
    if (complete && through != null && generation == ledger.generation) {
      _gapSyncedThrough = (generation, through);
    }
  }

  Future<bool> _syncGap(String id) async {
    final thread = id == threadChannelId;
    var cursor = messageWindows.newestVisibleSequence(id);
    if (cursor == BigInt.zero) return true;
    final owner = thread ? threadParentChannelId : id;
    final request = _windowRequest(
      id,
      thread: thread,
      allowed: () =>
          thread ? threadChannelId == id && !messageWindows.contextHasNewer(id) : channel?.id == id && !hasNewer,
    );
    try {
      while (true) {
        final page = await MessageHistoryPages.read(
          this, request, id,
          thread: thread, after: cursor, limit: _gapPageLimit, localFirst: false,
          remote: () async => {
            'messages': await client.get('/messages/sync', query: {
              'since_seq': '$cursor', 'channel_id': id, 'limit': _gapPageLimit,
            }),
          },
        );
        if (!request.current) return false;
        final listed = page['messages'] is List ? page['messages'] as List : const [];
        final rows = [
          for (final row in listed.whereType<Map>())
            if (row['channelId'] == id && row['id'] is String)
              Map<String, dynamic>.from(row),
        ];
        if (rows.isEmpty) break;
        final arrived = [
          for (final row in rows)
            if (!ledger.contains(id, row['id'] as String)) row,
        ];
        messageWindows.acceptRows(request, id, rows);
        if (!await MessageHistoryPages.record(this, request, page)) return false;
        if (!request.current) return false;
        var next = cursor;
        for (final row in rows) {
          final seq = RaftMessage(row).seq;
          if (seq > next) next = seq;
        }
        if (_mayMarkRead(id)) {
          unawaited(markRead(id));
        } else {
          arrived.forEach(_receiveUnread);
        }
        unawaited(_saveWindow(id, thread: thread));
        notifyListeners();
        if (listed.length < _gapPageLimit || next <= cursor) break;
        cursor = next;
      }
      return true;
    } on RaftApiException catch (e) {
      // Lost access is decided by the channel read and its revocation path.
      if (request.current &&
          owner != null &&
          (e.status == 403 || e.status == 404)) {
        unawaited(refreshChannel(owner));
      }
      return false;
    } catch (_) {
      return false;
    }
  }

  int _membershipRevision = 0, _membershipRequest = 0;

  bool canVisitSection(String name) {
    final capability = const {
      'agents': 'viewAgents',
      'computers': 'viewMachines',
      'members': 'viewMembers',
      'providers': 'manageExternalAuth',
      'integrations': 'manageIntegrations',
      'im-bridges': 'manageIntegrations',
      'administration': 'viewServerSettings',
      'billing': 'viewBilling',
      'workspace-settings': 'viewServerSettings',
    }[name];
    if (capability != null) return can(capability);
    return const {
      'home',
      'chat',
      'activity',
      'search',
      'saved',
      'tasks',
      'settings',
    }.contains(name);
  }

  /// Apply the mounted receiver's own authoritative role event synchronously.
  /// Any earlier HTTP membership snapshot loses its request revision fence.
  void applyMembershipRole(dynamic payload) {
    if (payload is! Map ||
        payload['userId'] != client.user?.id ||
        payload['serverId'] is! String ||
        payload['role'] is! String) {
      return;
    }
    final id = payload['serverId'] as String, role = payload['role'] as String;
    if (!servers.any((s) => s.id == id) && server?.id != id) return;
    _membershipRevision++;
    _membershipRequest++;
    servers = [
      for (final s in servers)
        s.id == id ? RaftRecord({...s.json, 'role': role}) : s,
    ];
    if (server?.id == id) {
      if (server?.string('role') != role) {
        _replyAuthorityEpoch++;
      }
      server = RaftRecord({...server!.json, 'role': role});
      _ensureSyncIdentity();
      refreshMessageSyncFlag();
      if (!canVisitSection(section)) section = 'chat';
    }
    _save('servers', '', servers.map((s) => s.json).toList(), server: '');
    notifyListeners();
  }

  /// Bumped by every `server:plan-updated`. Views that hold billing / usage
  /// reads (Web `loadBilling` + `loadUsage`) reload when it changes.
  int planRevision = 0;

  /// Web socketBridge `server:plan-updated`: patch the server row's plan, or
  /// re-read the servers when the payload is incomplete; billing and usage
  /// are then reloaded by [planRevision] observers.
  void _consumeServerPlanUpdated(dynamic payload) {
    final id = payload is Map ? payload['serverId'] : null;
    final plan = payload is Map ? payload['plan'] : null;
    if (id is String && id.isNotEmpty && plan is String) {
      RaftRecord patch(RaftRecord value) =>
          value.id == id ? RaftRecord({...value.json, 'plan': plan}) : value;
      servers = servers.map(patch).toList();
      if (server != null) server = patch(server!);
      _save('servers', '', servers.map((s) => s.json).toList(), server: '');
    } else {
      unawaited(recoverMembership());
    }
    planRevision++;
    notifyListeners();
  }

  /// Web serverStore `uploadServerAvatar`: the upload response's avatar URL is
  /// upserted into the server row everywhere (rail, switcher, settings) with
  /// no re-read. A mirrored presentation source re-reads instead.
  void applyServerAvatar(String serverId, String? avatarUrl) {
    if (presentationSource != null) {
      unawaited(recoverMembership());
      return;
    }
    if (_disposed) return;
    RaftRecord patch(RaftRecord value) => value.id == serverId
        ? RaftRecord({...value.json, 'avatarUrl': avatarUrl})
        : value;
    servers = servers.map(patch).toList();
    if (server != null) server = patch(server!);
    _save('servers', '', servers.map((s) => s.json).toList(), server: '');
    notifyListeners();
  }

  /// With [coalesce] (a refresh nobody asked for), a directory read already in
  /// flight stands in for it: starting another would retire that read.
  Future<void> recoverMembership({bool coalesce = false}) async {
    if (presentationSource != null) {
      return presentationSource!.recoverMembership();
    }
    if (!ownsClient || coalesce && _burstAt.directoryReading) return;
    final generation = client.generation,
        request = ++_membershipRequest,
        revision = _membershipRevision;
    try {
      final current = await client.servers();
      if (generation != client.generation ||
          _disposed ||
          request != _membershipRequest ||
          revision != _membershipRevision) {
        return;
      }
      _revokedServers.removeAll(current.map((s) => s.id));
      servers = current;
      await _save(
        'servers',
        '',
        current.map((s) => s.json).toList(),
        server: '',
      );
      if (server != null && !current.any((s) => s.id == server!.id)) {
        revokeServer(server!.id);
        await flushCache();
      }
      if (server == null && current.isNotEmpty) {
        await selectServer(current.first);
      }
      if (server != null) {
        server = current.where((s) => s.id == server!.id).firstOrNull;
        if (!canVisitSection(section)) section = 'chat';
      }
      notifyListeners();
    } catch (e) {
      // A refresh superseded by a newer one, or outrun by a server switch
      // (deleting the open server selects the next), is not a failure.
      if (!_disposed &&
          !_quietFailure(e) &&
          generation == client.generation &&
          request == _membershipRequest) {
        // ignore: raft_lints/avoid_imperative_notice_state -- Raised only for the latest membership request on the current client generation and not for rate-limited or stale-scope failures.
        setError(userErrorText(e, where: 'refresh servers'));
      }
    }
  }

  bool _rejectRevokedConversation(String id) {
    if (_disposed || !_revokedChannels.contains(id)) return false;
    if (location.entityId == id) {
      _missingConversationChannelId = id;
      _resolvingConversationChannelId = null;
      notifyListeners();
    }
    return true;
  }

  String? _resolvingConversationChannelId, _missingConversationChannelId;
  int _conversationResolutionRequest = 0;
  String? get missingConversationChannelId => _missingConversationChannelId;

  /// MainLayout ChannelById/ensureChannel: hydrate a real channel without
  /// treating the thread's focused reply as a message in its parent channel.
  /// URI, principal/server epoch and request ownership fence late arrival.
  Future<void> resolveConversationChannel(
    String id, {
    bool preserveThread = false,
  }) async {
    final source = presentationSource;
    if (source != null) {
      return source.resolveConversationChannel(
        id,
        preserveThread: preserveThread,
      );
    }
    final request = ++_conversationResolutionRequest,
        revision = navigationRevision,
        scope = navigationAuthority,
        reply = _replyToken(id),
        generation = ledger.generation;
    bool current() =>
        !_disposed &&
        request == _conversationResolutionRequest &&
        revision == navigationRevision &&
        scope == navigationAuthority &&
        reply == _replyToken(id) &&
        generation == ledger.generation &&
        !_revokedChannels.contains(id);
    if (_disposed ||
        _rejectRevokedConversation(id) ||
        id.isEmpty ||
        !can('viewChannel')) {
      return;
    }
    _resolvingConversationChannelId = id;
    _missingConversationChannelId = null;
    notifyListeners();
    try {
      var real = [...channels, ...dms].where((c) => c.id == id).firstOrNull;
      if (real == null) {
        final data = await client.get('/channels/$id');
        if (!current()) return;
        if (data is! Map ||
            data['id'] != id ||
            data['serverId'] != null && data['serverId'] != server?.id) {
          throw const RaftApiException('This channel is not available.');
        }
        real = RaftChannel(Map<String, dynamic>.from(data));
        if (!can('viewChannel', resource: real)) return;
        if (real.type == 'dm') {
          dms = [...dms.where((c) => c.id != id), real];
        } else {
          channels = [...channels.where((c) => c.id != id), real];
        }
      }
      if (!current() || !can('viewChannel', resource: real)) return;
      await selectChannel(
        real,
        navigate: false,
        preserveThread: preserveThread,
      );
    } catch (_) {
      if (current()) _missingConversationChannelId = id;
    } finally {
      if (current() && _resolvingConversationChannelId == id) {
        _resolvingConversationChannelId = null;
        notifyListeners();
      }
    }
  }

  Future<void> selectChannel(
    RaftChannel next, {
    bool autoRead = true,
    bool navigate = true,
    bool retainContextUntilAccepted = false,
    bool preserveThread = false,
    VoidCallback? onDraftRestored,
  }) async {
    final source = presentationSource;
    if (source != null) {
      return source.selectChannel(
        next,
        autoRead: autoRead,
        navigate: navigate,
        retainContextUntilAccepted: retainContextUntilAccepted,
        preserveThread: preserveThread,
        onDraftRestored: onDraftRestored,
      );
    }
    final retainThread =
        preserveThread &&
        threadIdentity?.parentChannelId == next.id &&
        location.thread?.channelId == next.id;
    if (!retainContextUntilAccepted && channel?.id != next.id) {
      contextLoadError = null;
    }
    channel = next;
    hasNewer = false;
    if (!retainThread) highlightedMessageId = null;
    final priorWindow = messageWindows.accepted[next.id];
    final replyAuthority = _replyToken(next.id);
    final compatible =
        priorWindow != null &&
        priorWindow.matches(_windowAuthority(), replyAuthority);
    if (priorWindow != null && !compatible) {
      messageWindows.discardWindow(next.id);
      _summariesByChannel.remove(next.id);
    }
    hasMore = compatible && priorWindow.hasMore;
    historyLimited = compatible && priorWindow.historyLimited;
    // Keep the accepted context kind until its replacement tail is accepted.
    if (!retainContextUntilAccepted &&
        messageWindows.setContextMode(liveWindowWrite, next.id, null)) {
      messageWindows.hideWindow(next.id);
    }
    if (navigate) section = 'chat';
    if (!retainThread) {
      threadParent = null;
      threadChannelId = null;
      messageWindows.threadGeneration++;
    }
    loadingOlder = false;
    channelLoading = true;
    error = null;
    // Source messageStore selectChannel: opening a conversation clears its
    // local unread at once; the read receipt follows the first page. A load
    // that never sends the receipt reconciles with the server in `finally`.
    final optimisticRead =
        autoRead &&
        navigate &&
        !retainContextUntilAccepted &&
        ((unread[next.id] ?? 0) != 0 || mentions.contains(next.id));
    final readPostsBefore = _readPosts[next.id] ?? 0;
    _scrolledAway.remove(next.id);
    if (optimisticRead) {
      // Web `clearUnread`: the count and the mention flag clear together.
      if ((unread[next.id] ?? 0) != 0) unread = {...unread, next.id: 0};
      _clearMention(next.id);
      _persistUnread();
    }
    messageWindows.channelGeneration++;
    final request = _windowRequest(
      next.id,
      checkNavigation: true,
      allowed: () =>
          channel?.id == next.id && can('viewChannel', resource: channel),
    );
    notifyListeners();
    if (isPendingDm(next)) {
      // A DM that is not on the server yet has nothing to read: only its
      // saved draft, then the composer.
      await _restoreDraft(next.id);
      onDraftRestored?.call();
      if (request.current) {
        channelLoading = false;
        notifyListeners();
      }
      return;
    }
    // Source ChatPanel loads the channel's tasks with its messages: start the
    // bucket read now so the chips are cached when the rows first render.
    unawaited(MessageTaskCache.of(client).warm(this, next.id));
    await _restoreDraft(next.id);
    // The composer can take staged content from here on; the message page
    // below is still loading.
    onDraftRestored?.call();
    if (!request.current) return;
    try {
      final page = retainContextUntilAccepted && MessageHistoryPages.storage(cache) == null
          ? null
          : await _cached('window', next.id);
      if (!request.current) return;
      if (!retainContextUntilAccepted && await MessageHistoryPositions.restoreMain(this, request, next.id)) {
        if (request.current) await _save('selection', '', next.id);
        return;
      }
      if (_restoreCachedTail(next.id, page, request, replace: retainContextUntilAccepted)) {
        notifyListeners();
      }
      final retained = retainContextUntilAccepted
          ? <Map<String, dynamic>>[]
          : messages.map((m) => m.json).toList();
      final retainedHasMore = hasMore;
      client.joinChannel(next.id);
      final fresh = await MessageHistoryPages.read(this, request, next.id);
      if (!request.current) return;
      _acceptFreshTail(
        next.id,
        fresh,
        retained: retained,
        retainedHasMore: retainedHasMore,
        request: request,
      );
      await MessageHistoryPages.record(this, request, fresh);
      if (!request.current) return;
      channelLoading = false;
      notifyListeners();
      await _saveWindow(next.id);
      if (!request.current) return;
      await _save('selection', '', next.id);
      if (request.current && autoRead) await markRead(next.id);
    } catch (e) {
      if (request.current) {
        if (e is RaftApiException && (e.status == 403 || e.status == 404)) {
          _revokeChannel(next.id);
        } else {
          error = userErrorText(e, where: 'select channel');
        }
      }
    } finally {
      if (request.current) {
        channelLoading = false;
        notifyListeners();
      }
      if (optimisticRead &&
          !_disposed &&
          (_readPosts[next.id] ?? 0) == readPostsBefore) {
        unawaited(refreshUnread());
      }
    }
  }

  /// Reconciles a freshly read first page into the open channel's window: the
  /// page is authoritative over its own range, older retained rows stay, and
  /// nothing outside the page range is dropped. Shared by the open and by the
  /// silent revalidation of a retained window.
  void _acceptFreshTail(
    String id,
    Map<String, dynamic> fresh, {
    required List<Map<String, dynamic>> retained,
    required bool retainedHasMore,
    required MessageWindowRequest request,
  }) {
    if (!request.current) return;
    final rows = acceptedWindowRows(fresh['messages'], id);
    messageWindows.setDiskOnly(request, id, false);
    historyLimited = fresh['historyLimited'] == true;
    messageWindows.acceptRows(
      request,
      id,
      rows,
      visible: historyLimited
          ? rows.map((e) => e['id'] as String)
          : reconcileTailWindow(retained, rows),
      replace: true,
    );
    final held = _summariesByChannel[id] ?? const {};
    _summariesByChannel[id] = {
      for (final row in retained)
        if (held[row['id']] != null) row['id'] as String: held[row['id']],
      ..._hydrateThreadSummaries(
        fresh['threadSummariesByParentMessageId'],
        id,
        expectedToken: _replyToken(id),
      ),
    };
    hasMore =
        fresh['historyLimited'] != true &&
        (visibleIds[id]!.length > rows.length
            ? retainedHasMore
            : rows.length >= 50);
  }

  /// Source ChatPanel shows a retained bucket at once and re-reads the tail
  /// behind it. Used where a held window is shown without [selectChannel]
  /// (a jump to a message the window holds, a live seq gap): the first page is
  /// merged into the open window by [_acceptFreshTail], so rows already shown
  /// keep their place and only the missing newer rows are added.
  Future<void> _revalidateTail(String id) async {
    if (channel?.id != id || !messageWindows.beginTailRevalidation(id)) {
      return;
    }
    final request = _windowRequest(
      id,
      allowed: () =>
          channel?.id == id &&
          !hasNewer &&
          can('viewChannel', resource: channel),
    );
    try {
      final retained = messages.map((m) => m.json).toList();
      final retainedHasMore = hasMore;
      final fresh = await MessageHistoryPages.read(this, request, id);
      if (!request.current) return;
      _acceptFreshTail(
        id,
        fresh,
        retained: retained,
        retainedHasMore: retainedHasMore,
        request: request,
      );
      await MessageHistoryPages.record(this, request, fresh);
      if (!request.current) return;
      notifyListeners();
      await _saveWindow(id);
    } catch (e) {
      if (request.current &&
          e is RaftApiException &&
          (e.status == 403 || e.status == 404)) {
        _revokeChannel(id);
      }
      // Any other failure leaves the shown window as it was.
    } finally {
      messageWindows.endTailRevalidation(id);
    }
  }

  Future<void> older({bool thread = false}) => _windowReceiver._older(thread);

  Future<void> newer({bool thread = false}) =>
      _windowReceiver._pageHistory(thread, newer: true);
  Future<void> loadHistoryGap(BigInt before, {bool thread = false}) =>
      _windowReceiver._pageHistory(thread, gapBefore: before);
  Future<void> _older(bool thread) => _pageHistory(thread);
  Future<void> _pageHistory(bool thread, {bool newer = false, BigInt? gapBefore}) =>
      MessageHistoryNavigation.page(this, thread: thread, newer: newer,
        gapBefore: gapBefore,
        requestFor: (id) => _windowRequest(id, thread: thread),
        summaries: _mergeThreadSummaries);

  bool _mayMarkRead(String id) {
    if (!foreground ||
        id == channel?.id &&
            (!_mainPresented ||
                !_chatTabPresented ||
                hasNewer ||
                channelLoading) ||
        id == threadChannelId && !_threadPresented ||
        id != channel?.id && id != threadChannelId) {
      return false;
    }
    final current = location, content = current.content;
    final previewMounted =
        current.route == RaftRoute.search ||
        current.route == RaftRoute.activity;
    if (id == channel?.id) {
      // Source mobile thread folds the parent channel immediately from the URI.
      // A prior Activity layout callback cannot admit its concurrently loaded tail.
      if (mobileNavigation && current.thread != null) return false;
      return (current.route == RaftRoute.channel ||
                  current.route == RaftRoute.dm) &&
              current.entityId == channel?.id ||
          previewMounted &&
              (content?.kind == RaftContentKind.channel ||
                  content?.kind == RaftContentKind.dm) &&
              content?.id == channel?.id;
    }
    // The initial admission guard already restricted this id to the open thread.
    return current.thread != null &&
            current.thread?.channelId == threadParentChannelId &&
            current.thread?.itemId == threadParentMessageId ||
        previewMounted &&
            content?.kind == RaftContentKind.thread &&
            content?.id == threadChannelId;
  }

  /// Open conversations whose timeline is scrolled above the latest end
  /// (reported by the chat view on the edge only, never per frame).
  final Set<String> _scrolledAway = {};

  void setTimelineAtBottom(String id, bool atBottom) {
    if (atBottom) {
      _scrolledAway.remove(id);
    } else {
      _scrolledAway.add(id);
    }
  }

  /// Where the reader was in a conversation when its timeline is about to be
  /// rebuilt for another layout (fold / unfold). The next timeline that
  /// mounts for [conversationId] shortly after resumes there; an anchor that
  /// is not claimed promptly is a stale position and is ignored.
  void saveReadingAnchor(String conversationId, ReadingAnchor anchor) {
    messageWindows.saveReadingAnchor(conversationId, anchor);
    MessageHistoryPositions.save(this, conversationId, anchor);
  }
  ReadingAnchor? takeReadingAnchor(String conversationId) =>
      messageWindows.takeReadingAnchor(conversationId);

  /// Read receipts sent per scope: lets an optimistic open-time clear know
  /// whether the receipt that backs it was ever sent.
  final Map<String, int> _readPosts = {};

  Future<void> markRead(String id) => _windowReceiver._markRead(id);

  /// Windows painted from disk whose first fresh page has not arrived yet.
  /// Their tail may be stale, so they never send a read receipt (nor clear
  /// the unread count) until revalidated.

  Future<void> _markRead(String id) async {
    if (messageWindows.isDiskOnly(id) || !_mayMarkRead(id)) {
      return;
    }
    final rows = ledger
        .messages(id)
        .where((m) => visibleIds[id]?.contains(m['id']) ?? false)
        .toList();
    if (rows.isEmpty) return;
    final origin = client.origin,
        principal = client.user?.id,
        serverId = client.serverId,
        generation = ledger.generation;
    final window = id == threadChannelId ? threadGeneration : channelGeneration;
    _readPosts[id] = (_readPosts[id] ?? 0) + 1;
    try {
      final receipt = await client.post(
        '/channels/$id/read',
        data: {'seq': rows.last['seq']},
      );
      if (client.origin != origin ||
          client.user?.id != principal ||
          client.serverId != serverId) {
        return;
      }
      if (ledger.generation != generation ||
          window !=
              (id == threadChannelId ? threadGeneration : channelGeneration) ||
          !_mayMarkRead(id)) {
        // The user left (or the window was replaced) while the receipt was in
        // flight. The server cursor still advanced: fold the accepted frontier
        // in and clear only what it covers, so Home never keeps an indicator
        // for a conversation that was read (Source clearUnread).
        // A reset ledger or a revoked scope never accepts a late receipt.
        if (ledger.generation == generation && !_revokedChannels.contains(id)) {
          _acceptStaleReadReceipt(id, receipt);
        }
        return;
      }
      if (receipt is Map && client.serverId != null && client.user != null) {
        readState.consumeUpdate(
          {
            'serverId': client.serverId,
            'scopeId': id,
            'maxReadSeq': receipt['maxReadSeq'],
            'readStateVersion': receipt['readStateVersion'],
          },
          serverId: client.serverId!,
          principalId: client.user!.id,
        );
      }
      // Source clearUnread: the read tail is the latest; no snapshot GET.
      if ((unread[id] ?? 0) != 0) unread = {...unread, id: 0};
      _clearMention(id);
      _persistReadState();
      notifyListeners();
    } catch (_) {
      // The cursor did not advance: an optimistic clear must not hide it.
      if (!_disposed &&
          client.origin == origin &&
          client.user?.id == principal &&
          client.serverId == serverId) {
        unawaited(refreshUnread());
      }
    }
  }

  void _acceptStaleReadReceipt(String id, dynamic receipt) {
    final serverId = client.serverId, principal = client.user?.id;
    if (_disposed || serverId == null || principal == null) return;
    if (receipt is Map) {
      readState.consumeUpdate(
        {
          'serverId': serverId,
          'scopeId': id,
          'maxReadSeq': receipt['maxReadSeq'],
          'readStateVersion': receipt['readStateVersion'],
        },
        serverId: serverId,
        principalId: principal,
      );
    }
    final before = unread[id] ?? 0;
    _projectReadState(id);
    if ((unread[id] ?? 0) != before) {
      _persistReadState();
      notifyListeners();
    }
    if ((unread[id] ?? 0) != 0) unawaited(refreshUnread());
  }

  /// Source acceptActivityReadAllAck: an Activity row's persisted read-all
  /// receipt is a read-state fact for [scopeId]. Folding it into the ledger is
  /// what keeps a stale inbox response from re-presenting the row as unread.
  /// Returns false for an invalid receipt or a changed identity.
  bool acceptReadAllAck(
    String scopeId,
    dynamic receipt, {
    required String serverId,
    required String principalId,
  }) {
    if (client.serverId != serverId || client.user?.id != principalId) {
      return false;
    }
    if (receipt is! Map) return false;
    final outcome = readState.consumeUpdate(
      {
        'serverId': serverId,
        'scopeId': scopeId,
        'maxReadSeq': receipt['maxReadSeq'] ?? receipt['seq'],
        'readStateVersion': receipt['readStateVersion'],
      },
      serverId: serverId,
      principalId: principalId,
    );
    if (outcome == 'corrupt') return false;
    if ((unread[scopeId] ?? 0) != 0) unread = {...unread, scopeId: 0};
    _clearMention(scopeId);
    _persistReadState();
    notifyListeners();
    return true;
  }

  Future<bool> send(
    String text, {
    bool thread = false,
    List<String>? attachments,
    List<Map<String, dynamic>>? mentions,
    String? randomId,
    bool asTask = false,
    String Function(int count)? attachmentsOnlyBody,
    void Function(int window)? onWindowRefreshed,
  }) async {
    final source = presentationSource;
    if (source != null) {
      return source.send(
        text,
        thread: thread,
        attachments: attachments,
        mentions: mentions,
        randomId: randomId,
        asTask: asTask,
        attachmentsOnlyBody: attachmentsOnlyBody,
        onWindowRefreshed: onWindowRefreshed,
      );
    }
    if (conversationPaused) {
      error = 'Channel conversion in progress';
      notifyListeners();
      return false;
    }
    // Shared content whose destination is not confirmed yet is never sent.
    if (sharedBlocked(thread: thread)) return false;
    error = null;
    final intendedScope = draftScope(thread: thread);
    if (intendedScope == null) return false;
    final scope = intendedScope, intendedChannel = channel?.id;
    final intendedParent = threadParentMessageId, role = server?.string('role');
    var window = thread ? threadGeneration : channelGeneration;
    bool current() =>
        !_disposed &&
        scope == draftScope(thread: thread) &&
        intendedChannel == channel?.id &&
        (!thread || intendedParent == threadParentMessageId) &&
        window == (thread ? threadGeneration : channelGeneration) &&
        role == server?.string('role') &&
        !conversationPaused;
    final selectedMentions = [
      for (final mention in mentions ?? <Map<String, dynamic>>[])
        Map<String, dynamic>.unmodifiable({
          'type': mention['type'],
          'id': mention['id'],
          'name': mention['name'],
        }),
    ];
    return composer.sends.send(
      text,
      client: client,
      attachments: attachments,
      mentions: selectedMentions,
      randomId: randomId,
      asTask: asTask,
      attachmentsOnlyBody:
          attachmentsOnlyBody ??
          (count) => count == 1 ? '[1 attachment]' : '[$count attachments]',
      visit: ComposerSendVisit(
        scope: scope,
        channelId: thread ? threadChannelId : intendedChannel,
        parentChannelId: intendedChannel,
        displaySeq: ledger.watermark,
        author: {
          'senderId': client.user?.id ?? '',
          'senderType': 'user',
          'senderName':
              client.user?.firstNonEmpty(const ['displayName', 'name']) ?? '',
        },
        current: current,
        prepare: () async {
          if (!thread && hasNewer && channel != null) {
            final refreshing = selectChannel(channel!);
            window = channelGeneration;
            await refreshing;
            if (!current()) return null;
            onWindowRefreshed?.call(window);
          }
          if (!current()) return null;
          return thread ? await ensureThread() : intendedChannel;
        },
        limit: uploadLimitBytes,
        transfer: (file) => _transferUpload(file, thread: thread),
        started: () {
          _composerNotices.clear(scope);
          sendWarnings.remove(scope);
        },
        failed: (cause) {
          error = userErrorText(
            cause,
            fallback: 'Failed to send message',
            where: 'send message',
          );
          notifyListeners();
        },
        received: (sent) {
          pendingMentionActions[scope] = normalizePendingMentionActions(
            sent.receipt['pendingMentionActions'],
          );
          sendWarnings[scope] = (
            unresolved: [
              for (final handle
                  in (sent.receipt['unresolvedMentionHandles'] as List? ??
                      const []))
                if (handle is String) handle,
            ],
            agentStopped:
                (sent.receipt['deliveryWarnings'] as List? ?? const []).any(
                  (w) =>
                      w is Map &&
                      w['targetType'] == 'agent' &&
                      w['reason'] == 'agent_stopped' &&
                      w['targetId'] is String &&
                      (w['targetId'] as String).isNotEmpty,
                ),
          );
        },
      ),
    );
  }

  /// Project session delivery into each existing accepted window. This does
  /// not create another ledger or move a page's current selection.
  void _sendChanged(String scope, RaftMessage? accepted) {
    if (_disposed) return;
    final selected = scope == draftScope() || scope == draftScope(thread: true);
    if (accepted != null && !_revokedChannels.contains(accepted.channelId)) {
      final id = accepted.channelId;
      if (id == channel?.id && !hasNewer ||
          id == threadChannelId ||
          messageWindows.retainsLiveTail(id)) {
        messageWindows.acceptRows(liveWindowWrite, id, [accepted.json]);
        unawaited(_saveWindow(id, thread: id == threadChannelId));
      }
    }
    if (selected) notifyListeners();
  }

  Future<String?> ensureThread() async {
    if (presentationSource != null) return presentationSource!.ensureThread();
    if (threadChannelId != null) return threadChannelId;
    return messageWindows.createThread(_createThread);
  }

  Future<String?> _createThread() async {
    final parentId = threadParentMessageId,
        parentChannelId = threadParentChannelId;
    if (parentId == null || parentChannelId == null) return null;
    final request = _windowRequest(
      parentChannelId,
      thread: true,
      checkNavigation: true,
      allowed: () =>
          parentId == threadParentMessageId &&
          parentChannelId == threadParentChannelId,
    );
    final value = await client.post(
      '/channels/$parentChannelId/threads',
      data: {'parentMessageId': parentId},
    );
    if (!request.current) {
      throw const RaftApiException('The thread changed. Please retry.');
    }
    threadChannelId = value['threadChannelId'];
    client.joinChannel(threadChannelId!);
    notifyListeners();
    return threadChannelId;
  }

  Future<void> openThread(
    RaftMessage parent, {
    String? focusedMessageId,
    bool navigate = true,
  }) => _windowReceiver._openThreadIdentity(
    parent.channelId,
    parent.id,
    focusedMessageId: focusedMessageId,
    navigate: navigate,
    acceptedParent: parent,
  );

  /// Opens a real thread identity independently of its parent metadata.
  /// [initialThreadChannelId] is the accepted inbox row's existing channel;
  /// Source opens that channel immediately without a resolution lookup.
  Future<void> openThreadIdentity({
    required String parentChannelId,
    required String parentMessageId,
    String? focusedMessageId,
    String? initialThreadChannelId,
    bool navigate = true,
  }) => _windowReceiver._openThreadIdentity(
    parentChannelId,
    parentMessageId,
    focusedMessageId: focusedMessageId,
    initialThreadChannelId: initialThreadChannelId,
    navigate: navigate,
  );

  Future<void> _openThreadIdentity(
    String parentChannelId,
    String parentMessageId, {
    String? focusedMessageId,
    String? initialThreadChannelId,
    bool navigate = true,
    RaftMessage? acceptedParent,
  }) async {
    if (parentChannelId.trim().isEmpty || parentMessageId.trim().isEmpty) {
      return;
    }
    final knownThreadChannelId = initialThreadChannelId?.trim().isEmpty == false
        ? initialThreadChannelId
        : null;
    if (_revokedChannels.contains(parentChannelId) ||
        _revokedChannels.contains(knownThreadChannelId) ||
        !can(
          'viewChannel',
          resource: _threadChannelResource(parentChannelId),
        )) {
      return;
    }
    if (navigate) {
      final next = location.withQuery({
        'thread': '$parentChannelId:$parentMessageId',
        'msg': ?focusedMessageId,
      });
      navigation.navigate(next, kind: location.panelNavigationKindTo(next));
    }
    final navigationWindow = navigationRevision,
        requestAuthority = _replyToken(parentChannelId),
        messageAuthority = _threadResourceAuthority(parentChannelId),
        generation = ledger.generation;
    messageWindows.threadGeneration++;
    final request = messageWindows.request(
      thread: true,
      allowed: () =>
          navigationWindow == navigationRevision &&
          requestAuthority == _replyToken(parentChannelId) &&
          (messageAuthority == null ||
              messageAuthority == _threadResourceAuthority(parentChannelId)) &&
          !_revokedChannels.contains(parentChannelId) &&
          !_revokedChannels.contains(threadChannelId) &&
          can('viewChannel', resource: _threadChannelResource(parentChannelId)),
    );
    _threadIdentity = WorkspaceThreadIdentity(
      parentChannelId: parentChannelId,
      parentMessageId: parentMessageId,
      focusedMessageId: focusedMessageId,
    );
    _threadVisit = request;
    final held = messageWindows.matchingThread(
      parentChannelId,
      parentMessageId,
      requestAuthority,
      messageAuthority,
    );
    final cached =
        held != null &&
            (knownThreadChannelId == null ||
                knownThreadChannelId == held.threadChannelId) &&
            (visibleIds[held.threadChannelId]?.isNotEmpty ?? false) &&
            (focusedMessageId == null ||
                visibleIds[held.threadChannelId]!.contains(focusedMessageId))
        ? held
        : null;
    if (cached == null) messageWindows.forgetThread(parentMessageId);
    threadParent = acceptedParent ?? cached?.parent;
    threadChannelId = knownThreadChannelId ?? cached?.threadChannelId;
    threadLoading = cached == null;
    _threadResolutionLoading = threadChannelId == null;
    _threadParentLoading = threadParent == null;
    _threadResolutionError = null;
    threadHasMore = cached?.hasMore ?? false;
    threadHistoryLimited = cached?.historyLimited ?? false;
    highlightedMessageId = focusedMessageId;
    notifyListeners();
    Future<void> restoreCurrentDraft() async {
      final scope = 'thread:$parentMessageId';
      if (await composer.restoreText(scope, () => request.current)) {
        notifyListeners();
      }
    }

    unawaited(restoreCurrentDraft());

    Future<void> resolveParent() async {
      if (acceptedParent != null) return;
      try {
        final page = await client.get(
          '/messages/context/$parentMessageId',
          query: {'channelId': parentChannelId},
        );
        if (!request.current) return;
        final rows = acceptedWindowRows(page['messages'], parentChannelId);
        final parent = rows
            .where((row) => row['id'] == parentMessageId)
            .firstOrNull;
        // Independently fetched parent metadata is not an accepted outer window.
        final liveParent = channel?.id == parentChannelId
            ? messages.where((row) => row.id == parentMessageId).firstOrNull
            : null;
        final resolved =
            liveParent ?? (parent == null ? null : RaftMessage(parent));
        // A background revalidation never takes away a shown parent.
        if (resolved != null || threadParent == null) threadParent = resolved;
        messageWindows.cacheThreadParent(
          request,
          parentMessageId,
          threadParent,
        );
      } catch (_) {
        if (!request.current) return;
        if (cached?.parent == null) threadParent = null;
      } finally {
        if (request.current) {
          _threadParentLoading = false;
          notifyListeners();
        }
      }
    }

    Map<String, dynamic>? readingPage;
    Future<void> resolveReplies() async {
      try {
        if (threadChannelId == null) {
          dynamic info;
          try {
            info = await client.get(
              '/channels/$parentChannelId/threads/$parentMessageId',
            );
          } on RaftApiException catch (e) {
            // Only the route's explicit "no thread for this message" 404 is an
            // empty thread; any other failure raises the in-panel Retry.
            if (!isThreadAbsentLookupError(e)) rethrow;
          }
          if (!request.current) return;
          _threadResolutionLoading = false;
          if (info == null) return;
          threadChannelId = info['threadChannelId'];
          if (!request.current) return;
          notifyListeners();
        }
        readingPage ??= focusedMessageId == null ? await MessageHistoryPositions.restore(this, request, threadChannelId!, thread: true) : null;
        final focused = focusedMessageId != null || readingPage != null;
        final page = readingPage ?? (focusedMessageId == null
            ? await MessageHistoryPages.read(this, request, threadChannelId!, thread: true)
            : await MessageHistoryPages.context(this, request, threadChannelId!, focusedMessageId, thread: true));
        if (!request.current) return;
        final boundary = messageWindows.acceptThreadPage(
          request, threadChannelId!, page,
          focused: focused,
          retainOlder: cached?.contextWindow == false || cached == null && messageWindows.isDiskOnly(threadChannelId),
          retainedHasMore: threadHasMore,
        );
        await MessageHistoryPages.record(this, request, page);
        if (!request.current) return;
        threadHistoryLimited = boundary.limited;
        threadHasMore = boundary.more;
        client.joinChannel(threadChannelId!);
        threadLoading = false;
        messageWindows.cacheThread(
          request,
          parentMessageId,
          CachedMessageThread(
            parentChannelId,
            threadChannelId!,
            requestAuthority,
            messageAuthority,
            generation,
            threadHasMore,
            threadHistoryLimited,
            threadParent ?? cached?.parent,
            contextWindow: focused,
          ),
        );
        messageWindows.setContextMode(request, threadChannelId!,
          focused ? page['hasNewer'] == true : null);
        notifyListeners();
        await markRead(threadChannelId!);
        if (request.current) _saveWindow(threadChannelId!, thread: true);
      } catch (e) {
        final denied = threadChannelId;
        if (request.current && denied != null && e is RaftApiException &&
            (e.status == 403 || e.status == 404)) {
          _revokeChannel(denied);
          threadLoading = false;
          _threadResolutionLoading = false;
          notifyListeners();
        }
        // A cached window stays readable when its revalidation fails.
        if (request.current && cached == null) {
          // ThreadPanel's in-panel "Couldn't load this thread" + Retry; Web raises
          // no workspace-level error for it.
          _threadResolutionError = userErrorText(
            e,
            fallback: "Couldn't load this thread",
            where: 'open thread',
          );
        }
      } finally {
        if (request.current) {
          _threadResolutionLoading = false;
          threadLoading = false;
          notifyListeners();
        }
      }
    }

    /// A tail thread with no window in memory paints its on-device window
    /// (previous visit or previous process) while the lookup revalidates.
    Future<void> restoreDiskWindow() async {
      if (cached != null && !cached.contextWindow || focusedMessageId != null) return;
      final page = await _cached('thread-window', parentMessageId);
      if (!request.current || !threadLoading && cached?.contextWindow != true || page is! Map) return;
      final threadId = page['threadChannelId'];
      if (page['version'] != 1 ||
          page['windowKind'] != 'tail' ||
          messageAuthority == null ||
          page['authority'] != messageAuthority ||
          page['parentChannelId'] != parentChannelId ||
          threadId is! String ||
          (threadChannelId != null && threadChannelId != threadId) ||
          _revokedChannels.contains(threadId)) {
        return;
      }
      readingPage = await MessageHistoryPositions.restoreThread(this, request, threadId, page);
      if (readingPage != null) { _threadResolutionLoading = false; _threadParentLoading = threadParent == null; return; }
      final rows = acceptedWindowRows(page['messages'], threadId);
      if (rows.isEmpty) return;
      messageWindows.acceptRows(request, threadId, rows, replace: true);
      messageWindows.setDiskOnly(request, threadId, true);
      messageWindows.setContextMode(request, threadId, null);
      threadChannelId = threadId;
      final parent = page['parent'];
      if (threadParent == null &&
          parent is Map &&
          parent['id'] == parentMessageId &&
          parent['channelId'] == parentChannelId) {
        threadParent = RaftMessage(Map<String, dynamic>.from(parent));
        _threadParentLoading = false;
      }
      threadHasMore = page['hasMore'] == true;
      threadHistoryLimited = page['historyLimited'] == true;
      threadLoading = false;
      _threadResolutionLoading = false;
      notifyListeners();
    }

    await restoreDiskWindow();
    if (!request.current) return;
    // Source threadStore opens an accepted inbox row's known channel directly,
    // without a lookup. Parent metadata and focused replies load independently.
    final replies = resolveReplies();
    unawaited(resolveParent());
    await replies;
  }

  /// Clears only the focus owned by the mounted timeline's highlight timer.
  void clearHighlightedMessage(
    String? expectedId, {
    int? expectedNavigationRevision,
  }) {
    if (presentationSource != null) {
      if (presentationActive) {
        presentationSource!.clearHighlightedMessage(
          expectedId,
          expectedNavigationRevision: expectedNavigationRevision,
        );
      }
      return;
    }
    if (highlightedMessageId != expectedId ||
        expectedNavigationRevision != null &&
            expectedNavigationRevision != navigationRevision) {
      return;
    }
    final identity = threadIdentity;
    if (expectedId != null &&
        expectedNavigationRevision != null &&
        identity != null &&
        identity.focusedMessageId == expectedId &&
        threadChannelId != null) {
      navigation.consumeThreadFocus(
        threadChannelId: threadChannelId!,
        parentChannelId: identity.parentChannelId,
        parentMessageId: identity.parentMessageId,
        expectedMessageId: expectedId,
        expectedRevision: expectedNavigationRevision,
      );
    }
    highlightedMessageId = null;
    notifyListeners();
  }

  /// Source ChatPanel.handleBackToBottom reloads latest when newer is unloaded.
  Future<void> returnToLatest({bool navigate = true, bool thread = false}) =>
      MessageHistoryNavigation.latest(_windowReceiver, thread: thread, navigate: navigate);

  /// [replaceEntry] makes the destination take over the current history
  /// entry instead of pushing (Web openThreadParentMessageRoute, mobile).
  Future<void> jumpToMessage(
    String channelId,
    String? messageId, {
    bool navigate = true,
    bool replaceEntry = false,
  }) async {
    final source = presentationSource;
    if (source != null) {
      return source.jumpToMessage(
        channelId,
        messageId,
        navigate: navigate,
        replaceEntry: replaceEntry,
      );
    }
    contextLoadError = null;
    final generation = ledger.generation;
    var navigationWindow = navigationRevision;
    var authority = _windowAuthority();
    final replyAuthority = _replyToken(channelId);
    messageWindows.channelGeneration++;
    final request = messageWindows.request(
      thread: false,
      allowed: () =>
          navigationWindow == navigationRevision &&
          authority == _windowAuthority() &&
          replyAuthority == _replyToken(channelId) &&
          !_revokedChannels.contains(channelId),
    );
    if (_rejectRevokedConversation(channelId) || !request.current) return;
    var next = [
      ...channels,
      ...dms,
    ].where((c) => c.id == channelId).firstOrNull;
    try {
      next ??= RaftChannel(
        Map<String, dynamic>.from(await client.get('/channels/$channelId')),
      );
    } catch (_) {
      // Source ChannelById/ensureChannel resolves failed metadata to its
      // unavailable body. UI callbacks may not await this request; neither a
      // global async exception nor a retained conversation owns the failure.
      if (request.current) {
        _missingConversationChannelId = channelId;
        notifyListeners();
      }
      return;
    }
    if (!request.current || !can('viewChannel', resource: next)) return;
    if (messageId == null) {
      await selectChannel(next, navigate: navigate);
      return;
    }
    final sameChannel = channel?.id == channelId;
    authority = messageWindowAuthority(server?.string('role'), next.json);
    final knownWindow = messageWindows.accepted[channelId];
    final compatibleWindow =
        knownWindow?.matches(authority, replyAuthority) == true;
    final cachedTarget =
        compatibleWindow &&
        messageWindows.containsVisible(channelId, messageId);
    channel = next;
    if (navigate) {
      if (replaceEntry && server != null) {
        _unboundSection = 'chat';
        bindNavigation();
        navigation.selectSection(
          'chat',
          channelId: channelId,
          dm: next.type == 'dm',
          kind: RaftNavigationKind.replace,
        );
      } else {
        section = 'chat';
      }
      navigation.navigate(
        location.withQuery({'msg': messageId}),
        kind: RaftNavigationKind.replace,
      );
      navigationWindow = navigationRevision;
    }
    threadParent = null;
    threadChannelId = null;
    messageWindows.threadGeneration++;
    loadingOlder = false;
    error = null;
    highlightedMessageId = messageId;
    if (cachedTarget) {
      MessageHistoryPages.recheckContext(this, request, channelId, messageId);
      hasMore = sameChannel ? hasMore : knownWindow!.hasMore;
      hasNewer = sameChannel
          ? hasNewer
          : messageWindows.contextHasNewer(channelId);
      historyLimited = messageWindows.historyLimit(channelId);
      channelLoading = false;
      messageWindows.finishContextVisit(request);
      notifyListeners();
      // The held window is shown at once, but a window this channel left (or
      // that came from disk) may be behind the server: read the tail quietly
      // and merge it without moving the reading position.
      if ((!sameChannel || messageWindows.isDiskOnly(channelId)) &&
          !hasNewer &&
          !messageWindows.isContext(channelId)) {
        unawaited(_revalidateTail(channelId));
      }
      return;
    }
    channelLoading = true;
    // Source preserves the destination's already accepted memory bucket while
    // its context GET waits. A disk/unaccepted bucket or an earlier principal,
    // server or capability epoch cannot grant this cross-channel projection.
    messageWindows.beginContextVisit((
      channelId: channelId,
      request: request,
      retainRows: sameChannel || compatibleWindow,
    ));
    if (!sameChannel) {
      // Held summaries belong to the held window; an incompatible window
      // takes its summaries with it.
      if (!compatibleWindow) _summariesByChannel.remove(channelId);
      if (compatibleWindow) {
        hasMore = knownWindow!.hasMore;
        hasNewer = messageWindows.contextHasNewer(channelId);
        historyLimited = messageWindows.historyLimit(channelId);
      }
    }
    bool current() =>
        request.current &&
        channel?.id == channelId &&
        can('viewChannel', resource: channel);
    void settled() {
      messageWindows.finishContextVisit(request);
      channelLoading = false;
      notifyListeners();
    }

    notifyListeners();
    try {
      final context = await MessageHistoryPages.context(this, request, channelId, messageId);
      if (!current()) return;
      final target = context['canonicalTarget'];
      if (target is Map && target['kind'] == 'thread') {
        final parentId = target['threadParentMessageId'] as String;
        final parentChannelId = target['channelId'] as String? ?? channelId;
        final opening = openThreadIdentity(
          parentChannelId: parentChannelId,
          parentMessageId: parentId,
          focusedMessageId: target['messageId'] as String? ?? messageId,
          navigate: navigate,
        );
        if (navigate) navigationWindow = navigationRevision;
        await opening;
        if (!current()) return;
        // Source loadMessageContext opens the canonical thread, then loads the
        // outer channel tail. Keep both accepted rows and reply focus while it
        // is pending; do not pass through selectChannel's thread reset/cache.
        try {
          final tail = await MessageHistoryPages.read(this, request, channelId);
          if (!current()) return;
          final accepted = _acceptContextPage(
            request,
            channelId,
            tail,
            context: false,
          );
          await MessageHistoryPages.record(this, request, tail);
          if (!current()) return;
          final refreshedParent = accepted
              .where((row) => row['id'] == parentId)
              .firstOrNull;
          if (parentChannelId == channelId && refreshedParent != null) {
            threadParent = RaftMessage(refreshedParent);
          }
          settled();
          await markRead(channelId);
          if (current()) await _saveWindow(channelId);
        } catch (e) {
          if (!current()) return;
          if (e is RaftApiException && e.status == 403) {
            _revokeChannel(channelId);
          } else {
            error = userErrorText(e, where: 'load message context');
            settled();
          }
        }
      } else {
        _acceptContextPage(request, channelId, context);
        await MessageHistoryPages.record(this, request, context);
        if (!current()) return;
        final targetId = context['targetMessageId'] as String? ?? messageId;
        highlightedMessageId = targetId;
        settled();
        await markRead(channelId);
      }
    } catch (e) {
      if (!current()) return;
      if (e is RaftApiException && e.status == 403) {
        _revokeChannel(channelId);
      } else {
        messageWindows.finishContextVisit(request);
        highlightedMessageId = null;
        // Source falls back to latest after a missing/unavailable context.
        final fallback = selectChannel(
          next,
          navigate: navigate,
          retainContextUntilAccepted: true,
        );
        await fallback;
        if (!_disposed &&
            channel?.id == channelId &&
            generation == ledger.generation &&
            authority == _windowAuthority()) {
          contextLoadError = historyLimited
              ? MessageContextLoadError.beyondHistory
              : MessageContextLoadError.notFound;
          notifyListeners();
        }
      }
    } finally {
      if (current()) {
        settled();
      }
    }
  }

  /// Context and canonical-thread fallback pages publish the same accepted
  /// row versions, boundaries and reply summaries before any notification.
  List<Map<String, dynamic>> _acceptContextPage(
    MessageWindowRequest request,
    String id,
    dynamic page, {
    bool context = true,
  }) {
    if (!request.current) return [];
    final rows = acceptedWindowRows(page['messages'], id);
    historyLimited = !context && page['historyLimited'] == true;
    hasMore = context
        ? page['hasOlder'] == true
        : !historyLimited && rows.length >= 50;
    hasNewer = context && page['hasNewer'] == true;
    messageWindows.rememberWindow(
      request,
      id,
      _windowAuthority(),
      hasMore,
      _replyToken(id),
      historyLimited: historyLimited,
      rows: rows,
      replace: true,
    );
    messageWindows.setContextMode(request, id, context ? hasNewer : null);
    _mergeThreadSummaries(id, page['threadSummariesByParentMessageId']);
    return rows;
  }

  /// Web ChatPanel `joinChannel(channel.id)`: join the open channel, then
  /// re-read the channel list so the sidebar and composer see the membership.
  Future<void> joinOpenChannel() async {
    try {
      final c = channel!;
      await command('POST', '/channels/${c.id}/join');
      await refreshChannels();
      await selectChannel(channels.firstWhere((next) => next.id == c.id));
    } catch (e) {
      if (!_disposed && !_staleScope(e)) {
        // ignore: raft_lints/avoid_imperative_notice_state -- User-initiated join; stale-scope failures are not reported; cleared on server switch or revoke.
        setError(userErrorText(e, where: 'join channel'));
      }
    }
  }

  /// Web ThreadPanel `handleJoinParentChannel`: join the thread's parent
  /// channel (the CTA that replaces the composer for a non-member).
  Future<void> joinThreadParentChannel() async {
    final id = threadParentChannelId;
    if (id == null) return;
    try {
      await command('POST', '/channels/$id/join');
      await refreshChannels();
    } catch (e) {
      if (!_disposed && !_staleScope(e)) {
        // ignore: raft_lints/avoid_imperative_notice_state -- User-initiated join; stale-scope failures are not reported; cleared on server switch or revoke.
        setError(userErrorText(e, where: 'join thread parent channel'));
      }
    }
  }

  // Web profileStore: the profile side panel is a URL slot (`profile=`); an
  // external author's frozen attribution payload lives beside it.
  final Map<String, Map<String, dynamic>> _externalProfiles = {};

  /// The provider attribution frozen when an external author's panel opened.
  Map<String, dynamic>? externalProfile(String messageId) =>
      presentationSource?.externalProfile(messageId) ??
      _externalProfiles[messageId];

  /// Web `openProfile` / `openExternalProfile`: a human or agent (optionally
  /// landing on an agent [agentTab]) or an external message author
  /// ([id] = message id, [channelId] its channel, [externalAuthor] the
  /// attribution). Adding a panel pushes history; retargeting replaces.
  void openProfile(
    RaftProfileKind kind,
    String id, {
    String? agentTab,
    String? channelId,
    Map<String, dynamic>? externalAuthor,
  }) {
    final source = presentationSource;
    if (source != null) {
      source.openProfile(
        kind,
        id,
        agentTab: agentTab,
        channelId: channelId,
        externalAuthor: externalAuthor,
      );
      return;
    }
    if (id.trim().isEmpty ||
        kind == RaftProfileKind.external && (channelId?.isEmpty ?? true)) {
      return;
    }
    if (externalAuthor != null) _externalProfiles[id] = externalAuthor;
    final next = location.withQuery({
      'profile': RaftProfileLocation(kind, id, channelId: channelId).toString(),
      'agentTab': kind == RaftProfileKind.agent ? agentTab : null,
    });
    if (next.uri == location.uri) return;
    navigation.navigateProfile(
      next,
      kind: location.panelNavigationKindTo(next),
    );
    notifyListeners();
  }

  /// Web `closeProfile`: removing a slot replaces the history entry.
  void closeProfile() {
    final source = presentationSource;
    if (source != null) {
      source.closeProfile();
      return;
    }
    if (location.profile == null) return;
    final next = location.withQuery({'profile': null, 'agentTab': null});
    navigation.navigateProfile(
      next,
      kind: location.panelNavigationKindTo(next),
    );
    notifyListeners();
  }

  void closeThread({bool navigate = true}) =>
      _windowReceiver._closeThread(navigate);

  void _closeThread(bool navigate) {
    if (navigate && location.thread != null) {
      final next = location.withQuery({'thread': null});
      navigation.navigate(next, kind: location.panelNavigationKindTo(next));
    }
    messageWindows.threadGeneration++;
    _threadIdentity = null;
    _threadVisit = null;
    _threadResolutionLoading = _threadParentLoading = false;
    _threadResolutionError = null;
    threadParent = null;
    threadChannelId = null;
    threadLoading = false;
    threadHistoryLimited = false;
    notifyListeners();
  }

  void setSection(String next) {
    if (!NavigationRegistry.knowsSection(next)) {
      NavigationRegistry.reportFallback(
        'section',
        next,
        NavigationRegistry.unsupportedSections[next],
      );
      return;
    }
    final source = presentationSource;
    if (source != null) {
      source.setSection(next);
      return;
    }
    if (!canVisitSection(next)) return;
    section = next;
    notifyListeners();
  }

  Future<dynamic> query(String path, {Map<String, dynamic>? query}) =>
      presentationSource?.sharedQuery(path, query: query) ??
      client.get(path, query: query);

  // ---- Signed attachment URLs -------------------------------------------
  // One cache and one resolve path for every attachment surface (list preview,
  // lightbox, document/media preview, inline players, Files panel). Memory
  // only; see [AttachmentUrlCache] for the lifetime rules.
  final AttachmentUrlCache _attachmentUrls;

  /// The players of inline video/audio cards and their full-screen viewers
  /// (one stream per attachment, shared; see [PlayerSessions]). A route-local
  /// presentation shares its source's.
  PlayerSessions get playerSessions =>
      presentationSource?.playerSessions ?? _playerSessions;
  final PlayerSessions _playerSessions = PlayerSessions();
  VoidCallback? _removePlayerTrim;

  int _attachmentUrlRequests = 0;

  /// Number of `/attachments/<id>/url` + batch requests this controller sent.
  @visibleForTesting
  int get debugAttachmentUrlRequests =>
      presentationSource?.debugAttachmentUrlRequests ?? _attachmentUrlRequests;
  @visibleForTesting
  AttachmentUrlCache get debugAttachmentUrls =>
      presentationSource?.debugAttachmentUrls ?? _attachmentUrls;

  /// The signed URL of attachment [id]. Reuses a fresh cached URL and joins a
  /// request already in flight; otherwise asks the API once. [disposition]
  /// null sends no disposition (server default). `attachment` URLs are
  /// one-shot downloads and are never retained. [channelId] ties the entry to
  /// its channel so revocation drops it.
  Future<String> attachmentUrl(
    String id, {
    String? disposition = 'inline',
    String? channelId,
    bool refresh = false,
    bool coalesce = false,
  }) {
    final source = presentationSource;
    if (source != null) {
      return source.attachmentUrl(
        id,
        disposition: disposition,
        channelId: channelId,
        refresh: refresh,
        coalesce: coalesce,
      );
    }
    final scope = attachmentImageScope;
    Future<dynamic> fetch() {
      _attachmentUrlRequests++;
      return client.get(
        '/attachments/${Uri.encodeComponent(id)}/url',
        query: disposition == null ? null : {'disposition': disposition},
      );
    }

    if (disposition == 'attachment' || _revokedChannels.contains(channelId)) {
      return fetch().then(_urlOf);
    }
    if (refresh) _attachmentUrls.invalidate(id, disposition: disposition ?? '');
    if (coalesce && disposition == 'inline') {
      // Many tiles asking at once (a screenful of images) share one batch
      // request instead of sending one request each.
      return _attachmentUrls.resolveCoalesced(
        id,
        scope: scope,
        channelId: channelId,
        fetchMany: _fetchAttachmentUrls,
        fetch: fetch,
      );
    }
    return _attachmentUrls.resolve(
      id,
      disposition: disposition ?? '',
      scope: scope,
      channelId: channelId,
      fetch: fetch,
    );
  }

  static String _urlOf(dynamic body) {
    final url = body is Map ? body['url'] : null;
    if (url is! String || url.isEmpty) {
      throw const FormatException('Invalid attachment response.');
    }
    return url;
  }

  /// A transfer rejected [id]'s cached URL (expired): forget it.
  void invalidateAttachmentUrl(String id) {
    final source = presentationSource;
    if (source != null) {
      source.invalidateAttachmentUrl(id);
    } else {
      _attachmentUrls.invalidate(id);
    }
  }

  /// `POST /attachments/urls` (1..50 ids) for the visible tiles that need a
  /// signed URL in this frame (images without a server thumbnail). Foreground
  /// work: a user-visible load, never speculative.
  Future<dynamic> _fetchAttachmentUrls(List<String> ids) {
    _attachmentUrlRequests++;
    return client.post('/attachments/urls', data: {'attachmentIds': ids});
  }

  DateTime? _previewGateAt;
  bool? _previewGate;
  Object? _previewGateScope;

  /// Whether document/media previews are enabled on the server. The flag is
  /// per server, so a short per-scope memory removes a serial round trip from
  /// every preview open; a failed read is never remembered.
  Future<bool> attachmentPreviewEnabled() async {
    final source = presentationSource;
    if (source != null) return source.attachmentPreviewEnabled();
    final scope = attachmentImageScope;
    final at = _previewGateAt;
    if (_previewGate != null &&
        _previewGateScope == scope &&
        at != null &&
        DateTime.now().difference(at) < const Duration(minutes: 2)) {
      return _previewGate!;
    }
    final gate = await client.get('/messages/attachment-preview/enabled');
    final enabled = gate is Map && gate['enabled'] == true;
    if (_disposed || scope != attachmentImageScope) return enabled;
    _previewGate = enabled;
    _previewGateScope = scope;
    _previewGateAt = DateTime.now();
    return enabled;
  }

  final _sharedReads = <String, Future<dynamic>>{};

  /// Coalesce identical concurrent reads of the same window. The Activity
  /// page and the Activity attention badge both reconcile the inbox 150 ms
  /// after the same socket event; they now share one request. Every caller
  /// receives its own copy of the decoded response.
  Future<dynamic> sharedQuery(String path, {Map<String, dynamic>? query}) {
    final params = query == null
        ? null
        : (query.keys.toList()..sort()).map((k) => [k, query[k]]).toList();
    final key = jsonEncode([
      client.origin,
      client.generation,
      client.user?.id,
      client.serverId,
      path,
      params,
    ]);
    var pending = _sharedReads[key];
    if (pending == null) {
      final started = pending = this.query(path, query: query);
      _sharedReads[key] = started;
      unawaited(
        started
            .then<void>((_) {}, onError: (Object _, StackTrace _) {})
            .whenComplete(() {
              if (identical(_sharedReads[key], started)) {
                _sharedReads.remove(key);
              }
            }),
      );
    }
    return pending.then(_copyDecoded);
  }

  /// A deep copy of a decoded response, so consumers never share its maps.
  static dynamic copyDecoded(dynamic value) => _copyDecoded(value);

  static dynamic _copyDecoded(dynamic value) => switch (value) {
    Map() => <String, dynamic>{
      for (final MapEntry(:key, :value) in value.entries)
        '$key': _copyDecoded(value),
    },
    List() => [for (final item in value) _copyDecoded(item)],
    _ => value,
  };
  int savedRevision = 0;
  Future<dynamic> command(String method, String path, {dynamic data}) async {
    if (presentationSource != null) {
      return presentationSource!.command(method, path, data: data);
    }
    final generation = client.generation,
        principal = client.user?.id,
        serverId = client.serverId;
    final value = await client.request(method, path, data: data);
    if (generation == client.generation &&
        principal == client.user?.id &&
        serverId == client.serverId &&
        ((method == 'POST' && path == '/channels/saved') ||
            (method == 'DELETE' && path.startsWith('/channels/saved/')))) {
      ++savedRevision;
      notifyListeners();
    }
    if (_reconcilesUnread(path)) await refreshUnread();
    return value;
  }

  /// Commands whose effect on unread counts is only known to the server
  /// (read-all, mark unread, join/leave, notification preferences).
  static bool _reconcilesUnread(String path) {
    final segments = Uri.parse(path).pathSegments;
    return segments.isNotEmpty &&
        const {
          'read',
          'read-all',
          'unread',
          'join',
          'leave',
          'notification-settings',
        }.contains(segments.last);
  }

  Future<void> hydrateReactionViewer(RaftMessage message) async {
    if (presentationSource != null) {
      return presentationSource!.hydrateReactionViewer(message);
    }
    if (reactionViewer.reacted(message.id) != null ||
        _viewerLoads.containsKey(message.id) ||
        channel?.archived == true) {
      return;
    }
    final generation = ledger.generation, serverId = client.serverId;
    if (serverId == null) return;
    Future<void> load() async {
      try {
        final snapshot = await client.get(
          '/messages/${message.id}/reactions/viewer',
        );
        if (_disposed ||
            generation != ledger.generation ||
            _revokedChannels.contains(message.channelId)) {
          return;
        }
        if (reactionViewer.accept(snapshot, serverId: serverId) == 'accepted') {
          notifyListeners();
        }
      } catch (_) {
        /* A click retries explicitly and reports an actionable error. */
      }
    }

    final pending = load();
    _viewerLoads[message.id] = pending;
    try {
      await pending;
    } finally {
      if (identical(_viewerLoads[message.id], pending)) {
        _viewerLoads.remove(message.id);
      }
    }
  }

  /// Reaction toggles in flight per message, so a presented row shows the
  /// viewer's own state at once (Web `pendingReactionTargetsRef`).
  final Map<String, Map<String, PendingReaction>> _pendingReactions = {};

  /// The state each pending toggle is heading for (null when none).
  Map<String, bool>? pendingReactionTargets(String messageId) {
    if (presentationSource != null) {
      return presentationSource!.pendingReactionTargets(messageId);
    }
    final pending = _pendingReactions[messageId];
    return pending == null || pending.isEmpty
        ? null
        : {for (final e in pending.entries) e.key: e.value.target};
  }

  String get _reactionViewerName => client.user?.name ?? '';

  /// Replace a held message's reactions (and the open thread's parent copy).
  /// Not persisted: optimistic state never reaches the disk cache.
  bool _setMessageReactions(
    String channelId,
    String messageId,
    List<Map<String, dynamic>> reactions,
  ) {
    var changed = false;
    if (ledger.contains(channelId, messageId)) {
      changed = ledger.ingestUpdate({
        'id': messageId,
        'channelId': channelId,
        'reactions': reactions,
      }, expectedGeneration: ledger.generation);
    }
    final parent = threadParent;
    if (parent != null && parent.id == messageId) {
      threadParent = RaftMessage({...parent.json, 'reactions': reactions});
      changed = true;
    }
    return changed;
  }

  List<Map<String, dynamic>> _heldReactions(RaftMessage message) =>
      reactionList(
        (ledger.message(message.channelId, message.id) ??
            message.json)['reactions'],
      );

  /// Socket rows carry server state that may predate a pending write: lay the
  /// pending toggles back over the row's reactions.
  void _overlayPendingReactions(Map<String, dynamic> row) {
    final id = row['id'], channelId = row['channelId'];
    final pending = _pendingReactions[id];
    final viewerId = client.user?.id;
    if (pending == null ||
        pending.isEmpty ||
        viewerId == null ||
        id is! String ||
        channelId is! String ||
        !row.containsKey('reactions')) {
      return;
    }
    final held = reactionList(ledger.message(channelId, id)?['reactions']);
    final next = overlayPendingReactions(
      held,
      pending,
      viewerId: viewerId,
      viewerName: _reactionViewerName,
    );
    if (!identical(next, held)) _setMessageReactions(channelId, id, next);
  }

  /// Web MessageItem `handleToggleReaction`: the chip and the viewer's own
  /// state change at once, the write follows, the server row reconciles and a
  /// failure puts the previous reaction back. A second toggle of the same
  /// emoji while one is in flight is ignored (Web `pendingReactionEmojisRef`).
  Future<void> toggleReaction(RaftMessage message, String emoji) {
    if (presentationSource != null) {
      return presentationSource!.toggleReaction(message, emoji);
    }
    final key = '${message.id}:$emoji';
    final inFlight = _reactionWrites[key];
    if (inFlight != null) return inFlight;
    final generation = ledger.generation,
        serverId = client.serverId,
        viewerId = client.user?.id;
    if (serverId == null || viewerId == null) return Future.value();
    Future<void> change() async {
      var reacted =
          reactionViewer.reacted(message.id)?.contains(emoji) ??
          ownReactionFromRow(_heldReactions(message), emoji, viewerId);
      if (reacted == null) {
        // Canonical chip without a private snapshot: read it first.
        final viewer = await client.get(
          '/messages/${message.id}/reactions/viewer',
        );
        if (_disposed || generation != ledger.generation) return;
        reactionViewer.accept(viewer, serverId: serverId);
        reacted = reactionViewer.reacted(message.id)?.contains(emoji);
        if (reacted == null) {
          throw StateError('Your reaction state could not be loaded.');
        }
      }
      final target = !reacted;
      final before = _heldReactions(message);
      final pending = beginReaction(before, emoji, target: target);
      (_pendingReactions[message.id] ??= {})[emoji] = pending;
      _setMessageReactions(
        message.channelId,
        message.id,
        setOwnReaction(
          before,
          emoji,
          reacted: target,
          viewerId: viewerId,
          viewerName: _reactionViewerName,
        ),
      );
      notifyListeners();
      bool stale() =>
          _disposed ||
          generation != ledger.generation ||
          _revokedChannels.contains(message.channelId);
      try {
        final value = await client.request(
          target ? 'POST' : 'DELETE',
          '/messages/${message.id}/reactions',
          data: {'emoji': emoji},
        );
        _clearPendingReaction(message.id, emoji);
        if (stale()) return;
        if (value is Map) {
          final accepted = reactionViewer.accept(
            value['reactionViewer'],
            serverId: serverId,
          );
          final row = Map<String, dynamic>.from(value)
            ..remove('reactionViewer');
          ledger.ingest([row], expectedGeneration: generation);
          // Other emojis still in flight keep their optimistic state.
          _overlayPendingReactions(row);
          final snapshot = reactionViewer.reacted(message.id);
          if (accepted != 'accepted' &&
              snapshot != null &&
              snapshot.contains(emoji) != target) {
            // A snapshot older than the write would show the old state.
            reactionViewer.remove(message.id);
            unawaited(hydrateReactionViewer(message));
          }
          final held = ledger.message(message.channelId, message.id);
          if (held != null && threadParent?.id == message.id) {
            threadParent = RaftMessage(held);
          }
          _saveWindow(
            message.channelId,
            thread: message.channelId == threadChannelId,
          );
        }
        notifyListeners();
      } catch (_) {
        _clearPendingReaction(message.id, emoji);
        if (!stale()) {
          _setMessageReactions(
            message.channelId,
            message.id,
            restoreReaction(_heldReactions(message), emoji, pending),
          );
          notifyListeners();
        }
        rethrow;
      }
    }

    final pending = change();
    _reactionWrites[key] = pending;
    return pending.whenComplete(() {
      if (identical(_reactionWrites[key], pending)) _reactionWrites.remove(key);
    });
  }

  void _clearPendingReaction(String messageId, String emoji) {
    final pending = _pendingReactions[messageId];
    pending?.remove(emoji);
    if (pending != null && pending.isEmpty) _pendingReactions.remove(messageId);
  }

  void _event(RaftEvent event) {
    if (_disposed) return;
    _ensureSyncIdentity();
    if (!ownsClient &&
        !const {
          'message:new',
          'message:updated',
          'thread:updated',
          'reaction_viewer:updated',
          'channel:removed',
        }.contains(event.name)) {
      return;
    }
    _lastServerEvent = DateTime.now();
    // Presence patches are owned by the entity directory and the agent
    // presentations; no workspace state changes, so nothing is notified.
    if (presenceEvents.contains(event.name)) return;
    if (event.name == 'connected') {
      connected = true;
      // Source socketBridge reconnectSnapshot: server-level reads only. The
      // channel list, unread counts and the resume/gap sync wait for
      // `rooms:joined`, once the server has joined this socket's rooms.
      _roomsJoinedPending = true;
      _lastHeartbeat = _lastServerEvent;
      // A socket that flaps (a bad network reconnects every few seconds) must
      // not repeat the whole refresh burst each time: what was read less than
      // [_burstFreshness] ago is still fresh, and the resume sync below
      // brings messages up to date.
      if (_due('membership')) recoverMembership(coalesce: true);
      if (_due('syncFlag')) refreshMessageSyncFlag();
      if (_due('sidebar')) loadSidebar();
      // A reconnect refreshes agents and members in place; the first connect
      // reuses the reads the selection already started.
      if (_connectedBefore && entityDirectory.started && _due('authors')) {
        entityDirectory.revalidateAuthors();
      }
      _connectedBefore = true;
    }
    if (event.name == 'rooms:joined') _roomsJoined();
    if (event.name == 'heartbeat') {
      final joining = _roomsJoinedPending;
      _heartbeat(event.payload);
      // A heartbeat only records liveness; a missed-push gap sync notifies
      // when its rows arrive.
      if (!joining) return;
    }
    if (event.name == 'disconnected' || event.name == 'connection:error') {
      connected = false;
    }
    if (event.name == 'server:membership-removed' &&
        event.payload is Map &&
        event.payload['serverId'] is String) {
      revokeServer(event.payload['serverId']);
      recoverMembership();
    }
    if (event.name == 'connection:error' &&
        event.payload is Map &&
        event.payload['notServerMember'] == true) {
      final id = client.serverId;
      if (id != null) revokeServer(id);
      recoverMembership();
    }
    if (event.name == 'server:member-updated') {
      applyMembershipRole(event.payload);
      recoverMembership();
      refreshChannels();
    }
    if (event.name == 'channel:members-updated' ||
        event.name == 'channel:authority-updated' ||
        event.name == 'channel:removed') {
      final payload = event.payload;
      final id = payload is Map ? payload['channelId'] ?? payload['id'] : null;
      if (id is! String || id.isEmpty) {
        // Unknown scope: fail closed for every channel.
        _invalidateAllChannelReplies();
      } else if (event.name != 'channel:members-updated' ||
          _membershipScoped(id)) {
        // Only the named channel's authority may have changed. A public
        // channel's member list never changes who can read it.
        _invalidateChannelReplies(id);
      }
    }
    if (event.name == 'channel:updated' ||
        event.name == 'channel:members-updated' ||
        event.name == 'dm:new' ||
        event.name == 'channel:authority-updated') {
      _channelRealtime(event.name, event.payload);
    }
    if (event.name == 'pinned:updated') loadSidebar();
    if (event.name == 'thread:updated' && event.payload is Map) {
      _consumeThreadUpdate(Map<String, dynamic>.from(event.payload));
    }
    if (event.name == 'notification_prefs:updated') {
      _consumeNotificationPrefs(event.payload);
    }
    if (event.name == 'message_display_prefs:updated') {
      _consumeMessageDisplayPrefs(event.payload);
    }
    if (event.name == 'server:plan-updated') {
      _consumeServerPlanUpdated(event.payload);
    }
    if ((event.name == 'message:new' || event.name == 'message:updated') &&
        event.payload is Map) {
      final row = Map<String, dynamic>.from(event.payload);
      if (_revokedChannels.contains(row['channelId'])) return;
      composer.sends.confirm(row);
      final duplicate =
          row['channelId'] is String &&
          row['id'] is String &&
          ledger.contains(row['channelId'], row['id']);
      if (event.name == 'message:updated') {
        MessageHistoryEvents.store(this, row, updateOnly: true);
        if (!ledger.ingestUpdate(row, expectedGeneration: ledger.generation)) {
          return;
        }
        _overlayPendingReactions(row);
      } else {
        final projection = syncCoreMessagesEnabled
            ? messageSync.consumeNew(row)
            : row;
        if (projection == null) return;
        if (!duplicate) MessageHistoryEvents.store(this, projection);
        ledger.ingest([projection], expectedGeneration: ledger.generation);
      }
      final id = row['channelId'];
      if (id == channel?.id && !hasNewer ||
          id == threadChannelId && !messageWindows.contextHasNewer(id)) {
        if (event.name == 'message:new' && id == channel?.id) {
          if (messageWindows.fillWindowGap(id, row['id'])) {
            unawaited(_revalidateTail(id));
          }
        }
        messageWindows.reveal(liveWindowWrite, id, [row['id']]);
        _saveWindow(id, thread: id == threadChannelId, coalesce: event.name == 'message:updated');
      } else if (event.name == 'message:new' &&
          id is String &&
          row['id'] is String &&
          messageWindows.retainsLiveTail(id)) {
        messageWindows.reveal(liveWindowWrite, id, [row['id']]);
      }
      // Source messageStore: counts move per event; only a first delivery
      // counts, and an edit never does.
      if (event.name == 'message:new' && !duplicate) _receiveUnread(row);
    }
    if (event.name == 'sync:resume:response' && event.payload is Map) {
      unawaited(MessageHistoryEvents.resumeResponse(this, event.payload as Map,
        refreshUnread: () {
          if (_due('unread', freshness: const Duration(seconds: 5))) refreshUnread();
        }));
    }
    if (event.name == 'channel:removed') {
      final p = event.payload;
      if (p is Map && p['channelId'] is String) {
        final id = p['channelId'] as String;
        _revokeChannel(id);
        channels.removeWhere((c) => c.id == id);
        dms.removeWhere((c) => c.id == id);
        if (channel?.id == id) {
          channel = null;
          closeThread();
        }
      }
    }
    if (event.name == 'read_state:updated' ||
        event.name == 'read_state:updated_bulk') {
      final payload = event.payload;
      if (payload is Map && client.serverId != null && client.user != null) {
        final updates = event.name.endsWith('_bulk')
            ? (payload['scopes'] as List? ?? [])
            : [payload];
        for (final update in updates) {
          if (update is Map) {
            final fact = {...update, 'serverId': payload['serverId']};
            final outcome = readState.consumeUpdate(
              fact,
              serverId: client.serverId!,
              principalId: client.user!.id,
            );
            if (outcome == 'accepted' && fact['scopeId'] is String) {
              _projectReadState(fact['scopeId'] as String);
            }
          }
        }
      }
      _persistReadState();
    }
    if (event.name == 'reaction_viewer:updated' && client.serverId != null) {
      reactionViewer.accept(event.payload, serverId: client.serverId!);
    }
    notifyListeners();
  }

  @override
  void dispose() {
    if (_disposed) return;
    _disposed = true;
    MessageHistoryPages.detach(this);
    if (presentationSource == null) messageWindows.dispose();
    composer.sends.detach(this);
    composer.disposeOwnedBy(this);
    if (_ownsEntityDirectory) {
      removeListener(_entityAuthorityChanged);
      entityDirectory.dispose();
    }
    if (_ownsFollowedThreads) followedThreads.dispose();
    _translations?.dispose();
    _threadSearch?.dispose();
    _threadFollowers?.dispose();
    _announcements?.dispose();
    _announcementGate?.dispose();
    _imageAuthorities.clear();
    _removeImageTrimmer?.call();
    _removeImageTrimmer = null;
    _removeImageReporter?.call();
    _removeImageReporter = null;
    _removePlayerTrim?.call();
    _removePlayerTrim = null;
    _removeCacheClear?.call();
    _removeCacheClear = null;
    _attachmentImages?.dispose();
    _attachmentUrls.dispose();
    _playerSessions.dispose();
    _channelFilesStores.clear();
    subscription.cancel();
    _activityUnread?.dispose();
    _activityUnread = null;
    foregroundChanges.dispose();
    resumes.dispose();
    if (ownsClient) client.dispose();
    super.dispose();
  }
}

/// Web `normalizePendingMentionActions`: keep well-formed rows only.
List<Map<String, dynamic>> normalizePendingMentionActions(dynamic value) => [
  if (value is List)
    for (final row in value.whereType<Map>())
      if (row['resolutionId'] is String &&
          row['targetType'] is String &&
          row['availableActions'] is List)
        Map<String, dynamic>.from(row),
];
