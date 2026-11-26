// SPDX-FileCopyrightText: 2026 DSH Remote contributors
// SPDX-License-Identifier: MIT
// See THIRD_PARTY_NOTICES.md.
// Dart port of ds-harness-remote `packages/client-core/src/acp-client.ts`.
library;

import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import '../channel/rpc_client.dart';

/// ACP backend ids the Host plugin registers (`agent.acp.<backend>.v1`).
abstract final class AcpBackends {
  static const claude = 'claude';
  static const cursor = 'cursor';
  static const antigravity = 'antigravity';
  static const pi = 'pi';
  static const opencode = 'opencode';
  static const hermes = 'hermes';

  /// Backends whose catalog / history failures must surface (never an empty list).
  static const strict = {pi, claude, hermes, opencode, antigravity};
}

/// `session/prompt` blocks until the upstream turn finishes.
const Duration acpPromptTimeout = Duration(minutes: 10);

/// Host budget: 20 s process start + 120 s `session/load`.
const Duration acpHistoryTimeout = Duration(seconds: 150);

/// Decoded bytes per `agent.acp.transfer.*` chunk.
const int acpTransferChunkBytes = 512 * 1024;

/// Largest transferred ACP request or response.
const int maxAcpTransferBytes = 288 * 1024 * 1024;

/// One `agent.acp.frame` event: an ordered upstream `session/update` or
/// `session/request_permission` (with a Host `requestHandle`).
class AcpFrame {
  const AcpFrame(this.streamId, this.method, this.params);

  final String streamId;
  final String method;
  final Object? params;

  Map<String, Object?>? get paramsMap => params is Map ? (params! as Map).cast<String, Object?>() : null;
}

/// `agent.acp.stream.closed.reason`: cancelled | completed | failed | peer-disconnected.
class AcpStreamClosed {
  const AcpStreamClosed(this.streamId, this.reason);
  final String streamId;
  final String reason;
}

class AcpWorkspace {
  const AcpWorkspace({required this.path, this.title, this.sessionCount});
  final String path;
  final String? title;
  final int? sessionCount;
}

class AcpSessionSummary {
  const AcpSessionSummary({
    required this.conversationId,
    required this.title,
    required this.createdAt,
    required this.updatedAt,
  });
  final String conversationId;
  final String title;
  final int createdAt;
  final int updatedAt;
}

class AcpSessionDetails {
  const AcpSessionDetails({required this.events, this.permissionPreset, this.running, this.raw});
  final List<Object?> events;
  final String? permissionPreset;
  final bool? running;
  final Map<String, Object?>? raw;
}

class AcpStream {
  AcpStream._(this.streamId, this._close);
  final String streamId;
  final Future<void> Function() _close;
  Future<void> close() => _close();
}

/// Client for the Host plugin's generic Agent ACP domain (`agent.acp.*`),
/// carried on an authenticated [RemoteRpcClient]. Only Host-allowlisted ACP
/// methods are accepted by the Host; this class does not widen anything.
class AcpRemoteClient {
  AcpRemoteClient(this._rpc, {List<String> capabilities = const []}) : capabilities = List.unmodifiable(capabilities);

  final RemoteRpcClient _rpc;
  final List<String> capabilities;

  bool supportsBackend(String backend) =>
      capabilities.contains('agent.acp.v1') && capabilities.contains('agent.acp.$backend.v1');

  bool get supportsTransfer => capabilities.contains('agent.acp.transfer.v1');

  Future<Object?> call(String method, [Map<String, Object?> params = const {}]) =>
      _rpc.call('agent.acp.call', {'method': method, 'params': params}, timeout: _timeoutFor(method));

  Future<Object?> initialize({String? backend}) => call('initialize', {
    'backend': ?backend,
    'clientInfo': {'name': 'dsh-go'},
  });

  /// `decision`: allow-once | reject-once | cancel. No persistent grants exist.
  Future<Object?> respond(String requestHandle, String decision, {Object? result}) {
    if (decision != 'allow-once' && decision != 'reject-once' && decision != 'cancel') {
      throw ArgumentError.value(decision, 'decision');
    }
    return _rpc.call('agent.acp.respond', {'requestHandle': requestHandle, 'decision': decision, 'result': ?result});
  }

  Future<List<AcpWorkspace>> listWorkspaces(String backend) async {
    final result = await _strict(backend, call('dsh/workspaceList', {'backend': backend}), const <Object?>[]);
    if (result is! List) {
      if (AcpBackends.strict.contains(backend)) throw const FormatException('ACP workspace catalog is invalid.');
      return const [];
    }
    return [
      for (final item in result)
        if (item is Map && item['path'] is String)
          AcpWorkspace(
            path: item['path'] as String,
            title: item['title'] is String ? item['title'] as String : null,
            sessionCount: item['sessionCount'] is int ? item['sessionCount'] as int : null,
          ),
    ];
  }

  Future<List<AcpSessionSummary>> listSessions(String path, String backend, {int? limit}) async {
    final result = await _strict(
      backend,
      call('dsh/sessionList', {'path': path, 'backend': backend, 'limit': ?limit}),
      const <String, Object?>{'items': <Object?>[]},
    );
    final items = result is Map ? result['items'] : null;
    if (items is! List) {
      if (AcpBackends.strict.contains(backend)) throw const FormatException('ACP session catalog is invalid.');
      return const [];
    }
    return [
      for (final item in items)
        if (item is Map && item['conversationId'] is String)
          AcpSessionSummary(
            conversationId: item['conversationId'] as String,
            title: item['title'] is String ? item['title'] as String : '',
            createdAt: _int(item['createdAt']),
            updatedAt: _int(item['updatedAt']),
          ),
    ];
  }

  /// Bounded history via `agent.acp.transfer.*`; failures are never an empty history.
  Future<AcpSessionDetails> loadSessionDetails(String sessionId, String backend) async {
    final result = await transferCall('dsh/sessionHistory', {'sessionId': sessionId, 'backend': backend});
    if (result is! Map || result['events'] is! List) throw const FormatException('ACP session history is invalid.');
    final map = result.cast<String, Object?>();
    return AcpSessionDetails(
      events: List<Object?>.from(map['events']! as List),
      permissionPreset: map['permissionPreset'] is String ? map['permissionPreset'] as String : null,
      running: map['running'] is bool ? map['running'] as bool : null,
      raw: map,
    );
  }

  /// `session/new` (cwd must be an existing absolute Host directory).
  Future<String> createSession(String cwd, String backend) async {
    final result = await call('session/new', {'cwd': cwd, 'backend': backend, 'mcpServers': <Object?>[]});
    final id = result is Map ? result['sessionId'] : null;
    if (id is! String || id.isEmpty) throw const FormatException('ACP session/new did not return sessionId.');
    return id;
  }

  Future<Object?> loadSession(String sessionId, String backend) =>
      call('session/load', {'sessionId': sessionId, 'backend': backend});

  /// Text-only prompt; resolves when the upstream turn ends.
  Future<Object?> prompt(String sessionId, String backend, String text) => call('session/prompt', {
    'sessionId': sessionId,
    'backend': backend,
    'prompt': [
      {'type': 'text', 'text': text},
    ],
  });

  Future<Object?> cancel(String sessionId) => call('session/cancel', {'sessionId': sessionId});

  Future<Object?> sessionModels(String sessionId, String backend) =>
      call('dsh/sessionModels', {'sessionId': sessionId, 'backend': backend});

  Future<Object?> selectModel(String sessionId, String backend, String model, {String? reasoningEffort}) => call(
    'dsh/selectModel',
    {'sessionId': sessionId, 'backend': backend, 'model': model, 'reasoningEffort': ?reasoningEffort},
  );

  Future<Object?> listDirectory(String path) => call('dsh/directoryList', {'path': path});

  /// Subscribes to a session's ordered frames. Subscribe-before-open: the
  /// event listener is attached before `agent.acp.stream.open` is sent.
  Future<AcpStream> openStream(
    String sessionId, {
    required void Function(AcpFrame frame) onFrame,
    void Function(AcpStreamClosed closed)? onClosed,
  }) async {
    final streamId = _newId();
    var closed = false;
    var opened = false;
    late final StreamSubscription<RemoteEvent> sub;
    bool markClosed() {
      if (closed) return false;
      closed = true;
      unawaited(sub.cancel());
      return true;
    }

    sub = _rpc.events.listen((event) {
      final data = event.data;
      if (data is! Map) return;
      if (event.event == 'agent.acp.frame') {
        final frame = data['frame'];
        if (frame is! Map || frame['method'] is! String) return;
        final parsed = AcpFrame(data['streamId'] is String ? data['streamId'] as String : '', frame['method'] as String, frame['params']);
        if (_matches(parsed, streamId, sessionId)) onFrame(parsed);
      } else if (event.event == 'agent.acp.stream.closed' && data['streamId'] == streamId) {
        if (markClosed()) onClosed?.call(AcpStreamClosed(streamId, data['reason'] is String ? data['reason'] as String : 'failed'));
      }
    });
    unawaited(
      _rpc.done.then((_) {
        if (!opened) {
          markClosed();
        } else if (markClosed()) {
          onClosed?.call(AcpStreamClosed(streamId, 'peer-disconnected'));
        }
      }),
    );
    try {
      await _rpc.call('agent.acp.stream.open', {'streamId': streamId, 'sessionId': sessionId});
      opened = true;
    } on Object {
      markClosed();
      rethrow;
    }
    return AcpStream._(streamId, () async {
      if (!markClosed()) return;
      try {
        await _rpc.call('agent.acp.stream.close', {'streamId': streamId});
      } on Object {
        // Best effort: the Host also drops streams when the peer goes away.
      }
    });
  }

  /// Bounded request/response through `agent.acp.transfer.open|chunk|commit|read|close`.
  Future<Object?> transferCall(String method, Map<String, Object?> params) async {
    final request = utf8.encode(jsonEncode({'method': method, 'params': params}));
    if (request.length > maxAcpTransferBytes) {
      throw const RemoteRpcException('REQUEST_TOO_LARGE', 'The ACP transfer request exceeds the bounded limit.', isLocal: true);
    }
    final transferId = _newId();
    final totalChunks = max(1, (request.length / acpTransferChunkBytes).ceil());
    try {
      await _rpc.call('agent.acp.transfer.open', {
        'transferId': transferId,
        'totalBytes': request.length,
        'totalChunks': totalChunks,
      });
      for (var index = 0; index < totalChunks; index++) {
        final start = index * acpTransferChunkBytes;
        final end = min(start + acpTransferChunkBytes, request.length);
        await _rpc.call('agent.acp.transfer.chunk', {
          'transferId': transferId,
          'index': index,
          'data': base64.encode(Uint8List.sublistView(request, start, end)),
        });
      }
      final commit = await _rpc.call('agent.acp.transfer.commit', {
        'transferId': transferId,
      }, timeout: _timeoutFor(method));
      if (commit is! Map) throw const FormatException('Invalid ACP transfer descriptor.');
      if (commit['kind'] == 'inline') return commit['response'];
      final total = commit['totalBytes'];
      final chunks = commit['totalChunks'];
      if (commit['kind'] != 'chunked' ||
          commit['transferId'] != transferId ||
          total is! int ||
          total < 1 ||
          total > maxAcpTransferBytes ||
          chunks is! int ||
          chunks != (total / acpTransferChunkBytes).ceil()) {
        throw const FormatException('Invalid ACP transfer descriptor.');
      }
      final out = BytesBuilder(copy: false);
      for (var index = 0; index < chunks; index++) {
        final part = await _rpc.call('agent.acp.transfer.read', {'transferId': transferId, 'index': index});
        if (part is! Map || part['transferId'] != transferId || part['index'] != index || part['data'] is! String) {
          throw const FormatException('Invalid ACP transfer chunk.');
        }
        final data = part['data'] as String;
        final bytes = base64.decode(data);
        final expected = min(acpTransferChunkBytes, total - index * acpTransferChunkBytes);
        if (bytes.length != expected || base64.encode(bytes) != data) {
          throw const FormatException('Invalid ACP transfer chunk size or encoding.');
        }
        out.add(bytes);
      }
      return jsonDecode(const Utf8Decoder(allowMalformed: false).convert(out.takeBytes()));
    } finally {
      try {
        await _rpc.call('agent.acp.transfer.close', {'transferId': transferId}, timeout: const Duration(seconds: 5));
      } on Object {
        // Host cleans idle transfers after 2 minutes.
      }
    }
  }

  Future<Object?> _strict(String backend, Future<Object?> future, Object? fallback) async {
    try {
      return await future;
    } on Object {
      if (AcpBackends.strict.contains(backend)) rethrow;
      return fallback;
    }
  }
}

/// Host frames match by stream id, the session-scoped fallback id, or the
/// ACP session id inside params.
bool _matches(AcpFrame frame, String streamId, String sessionId) {
  if (frame.streamId == streamId || frame.streamId == 'session:$sessionId') return true;
  final params = frame.paramsMap;
  if (params == null) return false;
  if (params['sessionId'] == sessionId) return true;
  final update = params['update'];
  return update is Map && update['sessionId'] == sessionId;
}

Duration? _timeoutFor(String method) {
  if (method == 'session/prompt') return acpPromptTimeout;
  if (method == 'dsh/sessionHistory' || method == 'session/load') return acpHistoryTimeout;
  return null;
}

int _int(Object? value) => value is int ? value : (value is num ? value.toInt() : 0);

final Random _random = Random.secure();

/// Host transfer ids are validated as UUIDs, so every generated id is one.
String _newId() {
  final bytes = List<int>.generate(16, (_) => _random.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final hex = bytes.map((byte) => byte.toRadixString(16).padLeft(2, '0')).join();
  return '${hex.substring(0, 8)}-${hex.substring(8, 12)}-${hex.substring(12, 16)}-'
      '${hex.substring(16, 20)}-${hex.substring(20)}';
}
