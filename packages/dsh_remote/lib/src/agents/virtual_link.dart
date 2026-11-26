// SPDX-FileCopyrightText: 2026 DSH Remote contributors
// SPDX-License-Identifier: MIT
// See THIRD_PARTY_NOTICES.md.
// In-memory DSH Harness surface for a Host's non-DSH agents (ACP Claude,
// Codex). Mirrors ds-harness-remote's virtual harness: projection only,
// nothing is written to the Host's SessionStore. Subclasses translate the
// upstream protocol; this file owns the mux link, `$events`, the session
// logs (contiguous seq, assistant stream) and the shared endpoints.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:dsh_client/dsh_client.dart';

import '../carrier/tunnel_carrier.dart';
import '../channel/rpc_client.dart';

int _nowMs() => DateTime.now().millisecondsSinceEpoch;

final Random _random = Random.secure();

String virtualId() {
  final bytes = List<int>.generate(16, (_) => _random.nextInt(256));
  return base64Url.encode(bytes).replaceAll('=', '');
}

Map<String, Object?> asJsonMap(Object? value) =>
    value is Map ? value.cast<String, Object?>() : <String, Object?>{};

String? asText(Object? value) => value is String ? value : null;

/// `args.request ?? args._request ?? args`.
Map<String, Object?> requestArg(Map<String, Object?> args) {
  final request = args['request'] ?? args['_request'];
  return request is Map ? request.cast<String, Object?>() : args;
}

/// `request.sessionId`, `address.sessionId`, `address.childSessionId` or `agentId`.
String? requestSessionId(Map<String, Object?> args) {
  final request = requestArg(args);
  final address = asJsonMap(request['address']);
  return asText(request['sessionId']) ??
      asText(address['sessionId']) ??
      asText(address['childSessionId']) ??
      asText(args['agentId']);
}

/// Business failure of a virtual endpoint (becomes an [RpcError]).
class VirtualFailure implements Exception {
  const VirtualFailure(this.code, this.message, [this.details = const {}]);
  final String code;
  final String message;
  final Map<String, Object?> details;

  @override
  String toString() => 'VirtualFailure($code): $message';
}

/// One logical downlink stream opened on a [VirtualMuxLink].
class VirtualStream {
  VirtualStream._(this._link, this.streamId, this.endpoint, this.args);

  final VirtualMuxLink _link;
  final String streamId;
  final String endpoint;
  final Map<String, Object?> args;
  final List<void Function()> _onCancel = [];
  bool _closed = false;

  bool get isClosed => _closed || _link._closed;

  void add(Object? value) {
    if (isClosed) return;
    _link._post({'type': 'item', 'streamId': streamId, 'value': value});
  }

  void end() {
    if (isClosed) return;
    _link._post({'type': 'end', 'streamId': streamId});
    _finish();
  }

  void fail(String code, String message, [Map<String, Object?> details = const {}]) {
    if (isClosed) return;
    _link._post({
      'type': 'error',
      'streamId': streamId,
      'error': {'code': code, 'message': message, 'details': details},
    });
    _finish();
  }

  /// Runs [callback] once when the stream ends, fails or is cancelled.
  void onCancel(void Function() callback) {
    if (isClosed) {
      callback();
      return;
    }
    _onCancel.add(callback);
  }

  void _finish() {
    if (_closed) return;
    _closed = true;
    _link._streams.remove(streamId);
    final callbacks = List.of(_onCancel);
    _onCancel.clear();
    for (final callback in callbacks) {
      try {
        callback();
      } on Object {
        // Cleanup is best effort.
      }
    }
  }
}

/// In-memory [DshMuxLink]: frames are delivered as microtasks, in order.
class VirtualMuxLink implements DshMuxLink {
  VirtualMuxLink._(this._carrier);

  final VirtualAgentCarrier _carrier;
  final Map<String, VirtualStream> _streams = {};
  final String clientId = virtualId();
  DshMuxFrameHandler? _onFrame;
  DshMuxLossHandler? _onLost;
  String? _eventsStreamId;
  bool _closed = false;

  @override
  void start({required DshMuxFrameHandler onFrame, required DshMuxLossHandler onLost}) {
    _onFrame = onFrame;
    _onLost = onLost;
  }

  void _post(Map<String, Object?> frame) {
    if (_closed) return;
    scheduleMicrotask(() {
      if (!_closed) _onFrame?.call(frame);
    });
  }

  void _postEvents(Map<String, Object?> value) {
    final id = _eventsStreamId;
    if (id != null) _post({'type': 'item', 'streamId': id, 'value': value});
  }

  void _shutdown() {
    if (_closed) return;
    _closed = true;
    for (final stream in List.of(_streams.values)) {
      stream._finish();
    }
    _streams.clear();
    _carrier._linkGone(this);
  }

  void _lose(String message) {
    if (_closed) return;
    final onLost = _onLost;
    _shutdown();
    onLost?.call(DshCarrierLostException(message), null);
  }

  @override
  void open(String streamId, String endpoint, Map<String, Object?> payload) {
    if (_closed) return;
    final args = asJsonMap(payload['args']);
    if (endpoint == r'$events') {
      _eventsStreamId = streamId;
      _post({
        'type': 'item',
        'streamId': streamId,
        'value': {
          'type': 'ready',
          'clientId': clientId,
          'host': {'home': _carrier.home},
        },
      });
      _carrier._eventsReady(this);
      return;
    }
    final stream = VirtualStream._(this, streamId, endpoint, args);
    if (!_carrier.allowsEndpoint(endpoint)) {
      stream.fail('METHOD_NOT_ALLOWED', '$endpoint is not available for ${_carrier.kind}.');
      return;
    }
    _streams[streamId] = stream;
    unawaited(
      Future<void>.sync(() => _carrier._openStream(stream)).catchError((Object error) {
        final failure = _carrier.failureOf(error);
        stream.fail(failure.code, failure.message, failure.details);
      }),
    );
  }

  @override
  void sendItem(String streamId, Object? value) {}

  @override
  void endUplink(String streamId) {}

  @override
  void cancel(String streamId) => _streams[streamId]?._finish();

  @override
  Future<void> close(int code, String reason) async => _shutdown();
}

class _PendingApproval {
  const _PendingApproval(this.handle, this.frame);
  final String handle;
  final Map<String, Object?> frame;
}

/// One virtual workspace (`workspace/follow` item).
class VirtualWorkspace {
  VirtualWorkspace({required this.workspaceId, required this.path, required this.title, int? createdAt, int? updatedAt})
    : createdAt = createdAt ?? _nowMs(),
      updatedAt = updatedAt ?? createdAt ?? _nowMs();

  final String workspaceId;
  final String path;
  String title;
  final List<String> sessionIds = [];
  final int createdAt;
  int updatedAt;

  Map<String, Object?> toJson() => {
    'workspaceId': workspaceId,
    'path': path,
    'title': title,
    'sessionIds': List.of(sessionIds),
    'createdAt': DateTime.fromMillisecondsSinceEpoch(createdAt, isUtc: true).toIso8601String(),
    'updatedAt': DateTime.fromMillisecondsSinceEpoch(updatedAt, isUtc: true).toIso8601String(),
  };
}

class _Block {
  _Block(this.type);
  final String type;
  final StringBuffer text = StringBuffer();
}

class _Attempt {
  _Attempt(this.attemptId, this.startedAfterSeq, this.turn);
  final String attemptId;
  final int startedAfterSeq;
  final int turn;
  int nextIndex = 0;
  final List<Map<String, Object?>> stream = [];
  final Map<int, _Block> blocks = {};
  int? textIndex;
  int? reasoningIndex;
}

/// In-memory projection of one upstream conversation as a DSH session log.
class VirtualSessionLog {
  VirtualSessionLog(this.sessionId, {this.cwd, this.workspaceId, int? createdAt, int? updatedAt, this.title})
    : createdAt = createdAt ?? _nowMs(),
      updatedAt = updatedAt ?? createdAt ?? _nowMs();

  final String sessionId;
  String? cwd;
  String? workspaceId;
  final int createdAt;
  int updatedAt;
  String? title;
  final List<Map<String, Object?>> events = [];
  final Map<String, Object?> projections = {};

  /// Follow streams and whether each asked for the assistant stream.
  final Map<VirtualStream, bool> followers = {};
  int revision = 0;
  int turn = 0;
  bool stepOpen = false;
  bool running = false;
  bool hydrated = false;
  bool blank = false;
  _Attempt? _attempt;

  /// Set by the carrier: broadcasts `session/control` changes.
  void Function(VirtualSessionLog log, String key, Object? value)? onProjection;

  int get cursor => events.length - 1;
  bool get hasLiveContent => _attempt != null;

  Map<String, Object?> get projectionsJson => {
    'asOfSeq': cursor,
    'values': {
      ...projections,
      'title': ?title,
      'sessionListMetadata': {'blank': blank},
    },
  };

  Map<String, Object?> summaryJson() => {
    'sessionId': sessionId,
    'updatedAt': updatedAt,
    'running': running,
    'blank': blank,
    'agentAvailable': true,
    'cwd': ?cwd,
    'projections': {
      'kind': 'cached',
      'asOfSeq': cursor,
      'values': {'title': ?title, 'sessionListMetadata': {'blank': blank}},
    },
  };

  void setTitle(String? value) {
    if (value == null || value.trim().isEmpty || value == title) return;
    title = value.trim();
    onProjection?.call(this, 'title', title);
  }

  void setProjection(String key, Object? value) {
    projections[key] = value;
    onProjection?.call(this, key, value);
  }

  /// Replaces history (seq renumbered contiguously); open follows resync.
  void resetHistory(Iterable<Object?> records) {
    events.clear();
    _attempt = null;
    stepOpen = false;
    turn = 0;
    for (final record in records) {
      final map = asJsonMap(record);
      final event = map['type'] == 'event' ? asJsonMap(map['event']) : map;
      final type = asText(event['type']);
      if (type == null) continue;
      final copy = {...event, 'seq': events.length};
      events.add(copy);
      final data = asJsonMap(event['data']);
      final eventTurn = data['turn'];
      if (eventTurn is int && eventTurn > turn) turn = eventTurn;
    }
    if (events.isNotEmpty) blank = false;
    hydrated = true;
    for (final follower in List.of(followers.keys)) {
      follower.end();
    }
  }

  Map<String, Object?> push(String type, Map<String, Object?> data) {
    final surface = type == 'user/message' || type == 'assistant/message' || type == 'tool/result';
    final event = <String, Object?>{
      'type': type,
      'seq': events.length,
      'time': _nowMs(),
      'data': data,
      if (surface) 'surfaceOp': 'append',
    };
    events.add(event);
    updatedAt = _nowMs();
    for (final follower in List.of(followers.keys)) {
      follower.add({'type': 'event', 'event': event});
    }
    return event;
  }

  /// Opens `turn/start` + `step/start` when no step is open.
  void ensureStep() {
    if (stepOpen) return;
    turn++;
    push('turn/start', {'turn': turn});
    push('step/start', {'turn': turn, 'step': 1});
    stepOpen = true;
  }

  void pushUserText(String text, {String? requestId}) {
    ensureStep();
    push('user/message', {
      'id': 'user:${_nowMs()}',
      'role': 'user',
      'content': [
        {'type': 'text', 'text': text},
      ],
      'source': {'kind': 'user', 'rpcId': ?requestId},
    });
    blank = false;
  }

  void _streamFrame(Map<String, Object?> frame) {
    for (final entry in List.of(followers.entries)) {
      if (entry.value) entry.key.add({'type': 'assistant-stream', 'frame': frame});
    }
  }

  void _chunk(_Attempt attempt, Map<String, Object?> chunk) {
    final time = _nowMs();
    attempt.stream.add({'type': 'chunk', 'time': time, 'chunk': chunk});
    _streamFrame({
      'type': 'chunk',
      'attemptId': attempt.attemptId,
      'revision': ++revision,
      'index': attempt.nextIndex++,
      'time': time,
      'chunk': chunk,
    });
  }

  /// Streams a `text` or `reasoning` delta into the open assistant attempt.
  void appendDelta(String blockType, String text) {
    if (text.isEmpty) return;
    ensureStep();
    var attempt = _attempt;
    if (attempt == null) {
      attempt = _attempt = _Attempt('virtual-attempt:$turn:${events.length}', cursor, turn);
      _streamFrame({
        'type': 'start',
        'attemptId': attempt.attemptId,
        'revision': ++revision,
        'startedAfterSeq': attempt.startedAfterSeq,
        'turn': turn,
        'step': 1,
      });
    }
    final reasoning = blockType == 'reasoning';
    var index = reasoning ? attempt.reasoningIndex : attempt.textIndex;
    if (index == null) {
      index = attempt.blocks.length;
      attempt.blocks[index] = _Block(reasoning ? 'reasoning' : 'text');
      if (reasoning) {
        attempt.reasoningIndex = index;
      } else {
        attempt.textIndex = index;
      }
      _chunk(attempt, {'type': 'block-start', 'index': index, 'blockType': reasoning ? 'reasoning' : 'text'});
    }
    attempt.blocks[index]!.text.write(text);
    _chunk(attempt, {'type': reasoning ? 'reasoning-delta' : 'text-delta', 'index': index, 'text': text});
  }

  /// Commits the open attempt as `assistant/message`. [reason]: stop |
  /// aborted | error | max-tokens.
  void finishAssistant({
    String reason = 'stop',
    Map<String, Object?>? failure,
    String provider = '',
    String model = '',
    Map<String, Object?>? usage,
  }) {
    final attempt = _attempt;
    if (attempt == null) return;
    _attempt = null;
    final content = <Map<String, Object?>>[];
    final indexes = attempt.blocks.keys.toList()..sort();
    for (final index in indexes) {
      final block = attempt.blocks[index]!;
      final part = {'type': block.type, 'text': block.text.toString()};
      content.add(part);
      _chunk(attempt, {'type': 'block-end', 'index': index, 'block': part});
    }
    final Map<String, Object?> finish = switch (reason) {
      'error' => {'kind': 'error', 'failure': failure ?? {'code': 'UNKNOWN', 'message': 'The agent failed.'}},
      'aborted' => {
        'kind': 'aborted',
        'failure': {'code': 'ABORTED', 'message': 'Cancelled.'},
      },
      'max-tokens' => {'kind': 'max-tokens'},
      _ => {'kind': 'stop'},
    };
    _chunk(attempt, {'type': 'finish', 'reason': finish});
    final event = push('assistant/message', {
      'turn': attempt.turn,
      'step': 1,
      'message': {
        'id': '$sessionId:${attempt.turn}:${events.length}',
        'role': 'assistant',
        'content': content,
        'source': {'kind': 'model', 'provider': provider, 'model': model},
        'usage': ?usage,
      },
      'usage': ?usage,
      'stream': attempt.stream,
    });
    _streamFrame({
      'type': 'end',
      'attemptId': attempt.attemptId,
      'revision': ++revision,
      'index': attempt.nextIndex,
      'outcome': {'kind': 'committed', 'eventType': 'assistant/message', 'seq': event['seq']},
    });
  }

  /// Pushes a finished assistant message without streaming (history, Codex items).
  void pushAssistantParts(List<Map<String, Object?>> content, {String provider = '', String model = ''}) {
    if (content.isEmpty) return;
    ensureStep();
    push('assistant/message', {
      'turn': turn,
      'step': 1,
      'message': {
        'id': '$sessionId:$turn:${events.length}',
        'role': 'assistant',
        'content': content,
        'source': {'kind': 'model', 'provider': provider, 'model': model},
      },
    });
  }

  void pushToolCall(String callId, String name, Object? input, {String status = 'running'}) {
    ensureStep();
    push('tool/call', {
      'turn': turn,
      'step': 1,
      'callId': callId,
      'name': name,
      'arguments': input == null ? '{}' : (input is String ? input : jsonEncode(input)),
      'toolCallId': callId,
      'toolName': name,
      'status': status,
    });
  }

  void pushToolResult(String callId, String output, {bool isError = false}) {
    ensureStep();
    push('tool/result', {
      'turn': turn,
      'step': 1,
      'message': {
        'id': '$callId:result',
        'role': 'user',
        'content': [
          {'type': 'tool-result', 'toolCallId': callId, 'content': output, if (isError) 'isError': true},
        ],
        'source': {'kind': 'tool', 'callId': callId},
      },
    });
  }

  /// Closes the open step and turn. [reason] is a `turn/end` reason map.
  void endTurn(Map<String, Object?> reason, {String provider = '', String model = '', Map<String, Object?>? usage}) {
    final kind = asText(reason['kind']);
    finishAssistant(
      reason: switch (kind) {
        'error' => 'error',
        'aborted' || 'interrupted' => 'aborted',
        'max-tokens' => 'max-tokens',
        _ => 'stop',
      },
      failure: reason['error'] is Map ? asJsonMap(reason['error']) : null,
      provider: provider,
      model: model,
      usage: usage,
    );
    if (!stepOpen) return;
    push('step/end', {'turn': turn, 'step': 1});
    push('turn/end', {'turn': turn, 'reason': reason, 'usage': ?usage});
    stepOpen = false;
  }

  /// Start index of the newest window holding [maxMessages] messages.
  int _windowStart(int end, int maxMessages) {
    var count = 0;
    var start = 0;
    for (var i = end - 1; i >= 0; i--) {
      final type = events[i]['type'];
      if (type == 'user/message' || type == 'assistant/message' || type == 'tool/result') count++;
      if (count >= maxMessages) {
        start = i;
        break;
      }
    }
    while (start > 0 && events[start]['type'] != 'turn/start') {
      start--;
    }
    return start;
  }

  Map<String, Object?> snapshot({int? maxMessages}) {
    var start = 0;
    if (maxMessages != null && maxMessages > 0) start = _windowStart(events.length, maxMessages);
    final attempt = _attempt;
    return {
      'type': 'snapshot',
      'header': {'id': sessionId, 'version': 4, 'createdAt': createdAt, 'isSeeded': false, 'cwd': ?cwd},
      'cursor': cursor,
      'records': [
        for (final event in events.sublist(start)) {'type': 'event', 'event': event},
      ],
      'hasMore': start > 0,
      'projections': projectionsJson,
      'assistantStream': {
        'revision': revision,
        if (attempt != null)
          'activeAttempt': {
            'attemptId': attempt.attemptId,
            'startedAfterSeq': attempt.startedAfterSeq,
            'turn': attempt.turn,
            'step': 1,
            'nextIndex': attempt.nextIndex,
            'stream': List.of(attempt.stream),
          },
      },
    };
  }

  /// `session/page`: events before [beforeSeq] (exclusive), through [throughSeq].
  Map<String, Object?> page({required int throughSeq, int? beforeSeq, int? maxMessages}) {
    final end = min(min(throughSeq + 1, beforeSeq ?? events.length), events.length);
    if (end <= 0) return {'records': <Object?>[], 'hasMore': false};
    final start = _windowStart(end, maxMessages ?? 100);
    return {
      'records': [
        for (final event in events.sublist(start, end)) {'type': 'event', 'event': event},
      ],
      'hasMore': start > 0,
    };
  }
}

/// Shared base of the Claude (ACP) and Codex carriers.
abstract class VirtualAgentCarrier extends DshCarrier {
  /// [connect] returns the live authenticated Host session (the app reuses
  /// the DSH tunnel's). It is called again after every link loss.
  VirtualAgentCarrier(this._connect);

  final Future<RemoteHostSession> Function() _connect;
  RemoteHostSession? _session;
  VirtualMuxLink? _link;
  bool _closed = false;
  final Map<String, _PendingApproval> _approvals = {};
  final Set<VirtualStream> _workspaceFollowers = {};
  final Set<VirtualStream> _controlFollowers = {};

  /// Virtual sessions by DSH session id.
  final Map<String, VirtualSessionLog> logs = {};

  /// Virtual workspaces by id, in display order.
  final Map<String, VirtualWorkspace> workspaces = {};
  bool catalogLoaded = false;
  Future<void>? _catalogLoad;

  /// Host session the upstream clients are bound to.
  RemoteHostSession? get session => _session;

  String get home => '';

  /// Endpoints this carrier implements (unary and streams).
  Set<String> get endpoints;

  @override
  String? get hostVersion => _session?.harnessVersion;

  @override
  DshTracer? get tracer => null;

  @override
  bool get supportsUplink => false;

  @override
  Set<String>? get peerCapabilities => _session?.capabilities.toSet();

  @override
  bool allowsEndpoint(String endpoint) => endpoint == r'$events/result' || _shared.contains(endpoint) || endpoints.contains(endpoint);

  static const Set<String> _shared = {
    'session/list',
    'session/projections',
    'session/page',
    'session/updateQueue',
    'session/follow',
    'session/control',
    'workspace/follow',
    'workspace/create',
    'permissionPresets/catalog',
    'commands/execute',
    'directoryPicker/list',
  };

  // ---------------------------------------------------------------------------
  // Subclass hooks

  /// A new Host session: rebuild upstream clients, drop live streams.
  void bindSession(RemoteHostSession session);

  /// Fills [workspaces] and [logs] (keeping existing logs) from the Host.
  Future<void> loadCatalog();

  /// Hydrates [log] (when needed) and keeps its upstream stream open.
  Future<void> attachLive(VirtualSessionLog log);

  /// Backend-specific unary endpoints.
  Future<Object?> dispatchAgent(String endpoint, Map<String, Object?> args);

  /// [decision]: allow | reject | cancel.
  Future<void> respondApproval(String handle, String decision);

  /// Upstream `dsh/directoryList`.
  Future<Object?> listDirectory(String path);

  /// Workspace id for an absolute Host directory.
  String workspaceIdFor(String path);

  /// Applies a permission preset (`workspace-write` / `danger-full-access`).
  Future<void> applyPermission(VirtualSessionLog log, String preset) async {
    log.setProjection('permissions', {'currentValue': preset});
  }

  // ---------------------------------------------------------------------------
  // Helpers for subclasses

  Future<RemoteHostSession> requireSession() async {
    final current = _session;
    if (current != null && !current.isClosed) return current;
    return _bind();
  }

  Future<RemoteHostSession> _bind() async {
    final RemoteHostSession next;
    try {
      next = await _connect();
    } on DshException {
      rethrow;
    } on Object catch (error) {
      throw DshTransportException('Remote Host is not connected: $error', error);
    }
    if (!identical(next, _session)) {
      _session = next;
      _approvals.clear();
      bindSession(next);
      unawaited(
        next.rpc.done.then((_) {
          if (identical(_session, next)) _link?._lose('Remote channel closed');
        }),
      );
    }
    return next;
  }

  Future<void> ensureCatalog() {
    if (catalogLoaded) return Future.value();
    return _catalogLoad ??= loadCatalog().then((_) => catalogLoaded = true).whenComplete(() => _catalogLoad = null);
  }

  VirtualSessionLog registerLog(VirtualSessionLog log) {
    final existing = logs[log.sessionId];
    if (existing != null) return existing;
    log.onProjection = _broadcastProjection;
    logs[log.sessionId] = log;
    return log;
  }

  VirtualSessionLog requireLog(String? sessionId) {
    final log = sessionId == null ? null : logs[sessionId];
    if (log == null) throw VirtualFailure('session/not-found', 'Session $sessionId was not found.');
    return log;
  }

  VirtualWorkspace upsertWorkspace(String path, {String? title}) {
    final id = workspaceIdFor(path);
    final workspace = workspaces[id] ??= VirtualWorkspace(workspaceId: id, path: path, title: title ?? _basename(path));
    if (title != null && title.isNotEmpty) workspace.title = title;
    return workspace;
  }

  void attachToWorkspace(VirtualSessionLog log, VirtualWorkspace workspace, {bool publish = true}) {
    log.workspaceId = workspace.workspaceId;
    log.cwd ??= workspace.path;
    if (!workspace.sessionIds.contains(log.sessionId)) workspace.sessionIds.insert(0, log.sessionId);
    workspace.updatedAt = max(workspace.updatedAt, log.updatedAt);
    if (publish) publishWorkspace(workspace);
  }

  void publishWorkspace(VirtualWorkspace workspace) {
    for (final follower in List.of(_workspaceFollowers)) {
      follower.add({'type': 'upsert', 'workspace': workspace.toJson()});
    }
  }

  void emitHost(String event, List<Object?> args) =>
      _link?._postEvents({'type': 'emit', 'event': event, 'args': args});

  void emitStatus(VirtualSessionLog log, bool running) {
    log.running = running;
    emitHost('api-session/status', [log.sessionId, running]);
    emitHost('api-session/activity', [log.sessionId, DateTime.now().millisecondsSinceEpoch]);
  }

  void emitSessionAdded(VirtualSessionLog log) => emitHost('api-session/added', [log.summaryJson()]);

  /// Publishes an approval request; [handle] is the Host's `requestHandle`.
  void emitApproval(String handle, String sessionId, Map<String, Object?> request) {
    final frame = <String, Object?>{
      'type': 'waterfall',
      'event': 'approval/request',
      'eventId': handle,
      'agentId': sessionId,
      'request': request,
    };
    _approvals[handle] = _PendingApproval(handle, frame);
    _link?._postEvents(frame);
  }

  void retractApproval(String handle) {
    if (_approvals.remove(handle) == null) return;
    _link?._postEvents({'type': 'cancel', 'eventId': handle});
  }

  /// Retracts every approval of [sessionId] (turn ended).
  void retractApprovalsOf(String sessionId) {
    for (final entry in List.of(_approvals.entries)) {
      if (entry.value.frame['agentId'] == sessionId) retractApproval(entry.key);
    }
  }

  ({String code, String message, Map<String, Object?> details}) failureOf(Object error) => switch (error) {
    VirtualFailure() => (code: error.code, message: error.message, details: error.details),
    RemoteRpcException() => (
      code: error.code,
      message: error.message,
      details: {...asJsonMap(error.details), if (error.retryable) 'retryable': true},
    ),
    DshRemoteException() => (code: error.code, message: error.message, details: error.details),
    FormatException() => (code: 'gateway/internal', message: error.message, details: const {}),
    ArgumentError() => (code: 'gateway/bad-request', message: '${error.message}', details: const {}),
    _ => (code: 'gateway/internal', message: '$error', details: const {}),
  };

  // ---------------------------------------------------------------------------
  // DshCarrier

  @override
  Future<RpcResult> call(String endpoint, Map<String, Object?> args, {Duration? timeout}) async {
    if (_closed) throw DshClosedException();
    if (!allowsEndpoint(endpoint)) {
      return RpcError(DshRemoteException('METHOD_NOT_ALLOWED', '$endpoint is not available for $kind.'));
    }
    try {
      await requireSession();
      final value = await _dispatch(endpoint, args);
      return RpcOk(value);
    } on RemoteRpcException catch (error) {
      if (error.isLocal) throw DshTransportException(error.message, error);
      final failure = failureOf(error);
      return RpcError(DshRemoteException(failure.code, failure.message, failure.details));
    } on DshTransportException {
      rethrow;
    } on DshClosedException {
      rethrow;
    } on Object catch (error) {
      final failure = failureOf(error);
      return RpcError(DshRemoteException(failure.code, failure.message, failure.details));
    }
  }

  @override
  Future<DshMuxLink> openLink(MuxOptions options) async {
    if (_closed) throw DshClosedException();
    final previous = _session;
    if (previous != null && previous.isClosed) _session = null;
    final session = await requireSession();
    if (session.isClosed) throw DshTransportException('Remote channel is closed');
    _link?._shutdown();
    final link = VirtualMuxLink._(this);
    _link = link;
    return link;
  }

  @override
  Future<void> close() async {
    if (_closed) return;
    _closed = true;
    _link?._shutdown();
    _link = null;
    _approvals.clear();
  }

  void _linkGone(VirtualMuxLink link) {
    if (identical(_link, link)) _link = null;
  }

  void _eventsReady(VirtualMuxLink link) {
    // Pending requests are redelivered to every new generation.
    for (final pending in _approvals.values) {
      link._postEvents(pending.frame);
    }
  }

  void _broadcastProjection(VirtualSessionLog log, String key, Object? value) {
    for (final follower in List.of(_controlFollowers)) {
      follower.add({'sessionId': log.sessionId, 'seq': log.cursor, 'key': key, 'value': value});
    }
  }

  Future<Object?> _dispatch(String endpoint, Map<String, Object?> args) async {
    switch (endpoint) {
      case r'$events/result':
        return _eventResult(args);
      case 'session/list':
        await ensureCatalog();
        return {
          'items': [for (final log in logs.values) log.summaryJson()],
        };
      case 'session/projections':
        await ensureCatalog();
        return logs[requestSessionId(args)]?.projectionsJson;
      case 'session/page':
        final request = requestArg(args);
        final log = requireLog(requestSessionId(args));
        return log.page(
          throughSeq: request['throughSeq'] is int ? request['throughSeq'] as int : log.cursor,
          beforeSeq: request['beforeSeq'] is int ? request['beforeSeq'] as int : null,
          maxMessages: request['maxMessages'] is int ? request['maxMessages'] as int : null,
        );
      case 'session/updateQueue':
        throw const VirtualFailure('session/queue-item-not-found', 'This agent has no prompt queue.');
      case 'workspace/create':
        final path = asText(requestArg(args)['path']);
        if (path == null || path.isEmpty) throw const VirtualFailure('gateway/bad-request', 'path is required.');
        await ensureCatalog();
        final created = !workspaces.containsKey(workspaceIdFor(path));
        final workspace = upsertWorkspace(path);
        publishWorkspace(workspace);
        return {'workspace': workspace.toJson(), 'created': created};
      case 'permissionPresets/catalog':
        const options = [
          {'value': 'workspace-write', 'name': 'Workspace write', 'description': 'Ask before running commands outside the workspace.'},
          {'value': 'danger-full-access', 'name': 'Full access', 'description': 'Approve every request automatically.'},
        ];
        return {'options': options, 'defaultOptions': options, 'defaultPreset': 'workspace-write'};
      case 'commands/execute':
        return _command(args);
      case 'directoryPicker/list':
        final path = asText(args['path']);
        final raw = asJsonMap(await listDirectory(path ?? ''));
        final entries = raw['entries'] ?? raw['items'] ?? const <Object?>[];
        return {
          'path': asText(raw['path']) ?? path ?? '',
          'home': asText(raw['home']) ?? home,
          'entries': [
            for (final entry in entries is List ? entries : const <Object?>[])
              if (entry is Map && entry['path'] is String)
                {
                  'name': asText(entry['name']) ?? _basename(entry['path'] as String),
                  'path': entry['path'],
                  'hidden': entry['hidden'] == true,
                },
          ],
          'truncated': raw['truncated'] == true,
        };
      default:
        return dispatchAgent(endpoint, args);
    }
  }

  Future<Object?> _command(Map<String, Object?> args) async {
    final line = (asText(args['line']) ?? '').trim();
    final match = RegExp(r'^/permission\s+(\S+)$').firstMatch(line);
    if (match == null) {
      return {
        'commandId': null,
        'result': {'kind': 'error', 'text': 'This command is not available for $kind.'},
      };
    }
    final preset = match.group(1)!;
    if (preset != 'workspace-write' && preset != 'danger-full-access') {
      return {
        'commandId': 'permission',
        'result': {'kind': 'error', 'text': 'Unsupported permission preset: $preset'},
      };
    }
    final log = requireLog(requestSessionId(args));
    await applyPermission(log, preset);
    return {
      'commandId': 'permission',
      'result': {'kind': 'success', 'text': preset},
    };
  }

  Future<Object?> _eventResult(Map<String, Object?> args) async {
    if (args['clientId'] != _link?.clientId) {
      throw const VirtualFailure('gateway/bad-request', 'clientId identifies no active event stream.');
    }
    final eventId = asText(args['eventId']);
    final pending = eventId == null ? null : _approvals.remove(eventId);
    if (pending == null) return null; // Already settled.
    final outcome = asJsonMap(args['outcome']);
    final value = asText(outcome['value']) ?? '';
    final decision = switch (asText(outcome['kind'])) {
      'result' when value.startsWith('allow') => 'allow',
      'result' => 'reject',
      _ => 'cancel',
    };
    await respondApproval(pending.handle, decision);
    return null;
  }

  Future<void> _openStream(VirtualStream stream) async {
    await requireSession();
    switch (stream.endpoint) {
      case 'workspace/follow':
        await ensureCatalog();
        if (stream.isClosed) return;
        stream.add({
          'type': 'baseline',
          'value': {
            'items': [for (final workspace in workspaces.values) workspace.toJson()],
            'archivedSessionIds': const <String>[],
            'pinnedSessionIds': const <String>[],
          },
        });
        _workspaceFollowers.add(stream);
        stream.onCancel(() => _workspaceFollowers.remove(stream));
      case 'session/control':
        await ensureCatalog();
        if (stream.isClosed) return;
        stream.add({
          'type': 'baseline',
          'value': {
            'projections': {for (final log in logs.values) log.sessionId: log.projectionsJson},
          },
        });
        _controlFollowers.add(stream);
        stream.onCancel(() => _controlFollowers.remove(stream));
      case 'session/follow':
        await ensureCatalog();
        final request = requestArg(stream.args);
        final log = requireLog(requestSessionId(stream.args));
        await attachLive(log);
        if (stream.isClosed) return;
        stream.add(log.snapshot(maxMessages: request['maxMessages'] is int ? request['maxMessages'] as int : null));
        log.followers[stream] = request['assistantStream'] == true;
        stream.onCancel(() => log.followers.remove(stream));
      default:
        throw VirtualFailure('METHOD_NOT_ALLOWED', '${stream.endpoint} is not available for $kind.');
    }
  }
}

String _basename(String path) {
  final parts = path.split(RegExp(r'[\\/]')).where((p) => p.isNotEmpty).toList();
  return parts.isEmpty ? path : parts.last;
}
