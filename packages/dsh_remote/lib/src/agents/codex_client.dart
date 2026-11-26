// SPDX-FileCopyrightText: 2026 DSH Remote contributors
// SPDX-License-Identifier: MIT
// See THIRD_PARTY_NOTICES.md.
// Dart port of ds-harness-remote `packages/client-core/src/codex-client.ts`
// (wire layer only; projection lives in the virtual carrier).
library;

import 'dart:async';
import 'dart:convert';
import 'dart:math';
import 'dart:typed_data';

import '../channel/rpc_client.dart';

/// Decoded bytes per `codex.app.transfer.*` chunk.
const int codexTransferChunkBytes = 512 * 1024;

/// Largest transferred Codex request or response.
const int maxCodexTransferBytes = 288 * 1024 * 1024;

/// Fixed Remote permission presets; the Host maps them to App Server policy.
abstract final class CodexPermissionPreset {
  static const workspaceWrite = 'workspace-write';
  static const dangerFullAccess = 'danger-full-access';
}

/// One `codex.app.frame`: an App Server notification or server request
/// (approvals carry a Host-generated `requestHandle`).
class CodexFrame {
  const CodexFrame(this.method, this.params);
  final String method;
  final Object? params;

  Map<String, Object?>? get paramsMap => params is Map ? (params! as Map).cast<String, Object?>() : null;
}

class CodexStream {
  CodexStream._(this.streamId, this.replayTruncated, this._close);
  final String streamId;
  final bool replayTruncated;
  final Future<void> Function() _close;
  Future<void> close() => _close();
}

/// Client for the Host plugin's Codex App Server domain (`codex.app.*`).
/// The Host only forwards its compile-time allowlist; nothing here widens it.
class CodexRemoteClient {
  CodexRemoteClient(this._rpc, {List<String> capabilities = const []}) : capabilities = List.unmodifiable(capabilities);

  final RemoteRpcClient _rpc;
  final List<String> capabilities;

  bool get supported => capabilities.contains('codex.appserver.v1');
  bool get supportsTransfer => capabilities.contains('codex.appserver.transfer.v1');

  Future<Object?> call(String method, Map<String, Object?> params, {bool transfer = false}) {
    final envelope = {'method': method, 'params': params};
    if (!transfer) return _rpc.call('codex.app.call', envelope);
    return _transfer(utf8.encode(jsonEncode(envelope)));
  }

  Future<Object?> account() => call('account/read', {'refreshToken': false});

  Future<Object?> models() => call('model/list', const {});

  Future<Object?> projects({String? cursor}) => call('project/list', {'cursor': ?cursor});

  Future<Object?> threads({String? cursor, int? limit, Object? cwd, bool? archived}) => call('thread/list', {
    'cursor': ?cursor,
    'limit': ?limit,
    'cwd': ?cwd,
    'archived': ?archived,
    'sortKey': 'updated_at',
    'sortDirection': 'desc',
  });

  /// Host-paged history in DSH message shape; large pages use transfer.
  Future<Object?> sessionHistory(String threadId, {int? beforeSeq, int? maxMessages}) => call(
    'dsh/sessionHistory',
    {'threadId': threadId, 'beforeSeq': ?beforeSeq, 'maxMessages': ?maxMessages},
    transfer: supportsTransfer,
  );

  Future<Object?> start(String cwd, {String? model, String? permissionPreset}) =>
      call('thread/start', {'cwd': cwd, 'model': ?model, 'permissionPreset': ?permissionPreset});

  Future<Object?> resume(String threadId, {String? model, String? permissionPreset}) =>
      call('thread/resume', {'threadId': threadId, 'model': ?model, 'permissionPreset': ?permissionPreset});

  Future<Object?> rename(String threadId, String name) => call('thread/name/set', {'threadId': threadId, 'name': name});

  Future<Object?> archive(String threadId) => call('thread/archive', {'threadId': threadId});

  Future<Object?> prompt(String threadId, String text, {String? model, String? effort, String? permissionPreset}) =>
      call('turn/start', {
        'threadId': threadId,
        'input': [
          {'type': 'text', 'text': text},
        ],
        'model': ?model,
        'effort': ?effort,
        'permissionPreset': ?permissionPreset,
      });

  Future<Object?> steer(String threadId, String expectedTurnId, String text) => call('turn/steer', {
    'threadId': threadId,
    'expectedTurnId': expectedTurnId,
    'input': [
      {'type': 'text', 'text': text},
    ],
  });

  Future<Object?> interrupt(String threadId, String turnId) =>
      call('turn/interrupt', {'threadId': threadId, 'turnId': turnId});

  Future<Object?> listDirectory(String path) => call('dsh/directoryList', {'path': path});

  /// `decision`: accept | decline | cancel (no session-wide grants).
  Future<Object?> respond(String requestHandle, String decision) {
    if (decision != 'accept' && decision != 'decline' && decision != 'cancel') {
      throw ArgumentError.value(decision, 'decision');
    }
    return _rpc.call('codex.app.respond', {'requestHandle': requestHandle, 'decision': decision});
  }

  /// Subscribe-before-open: the event listener exists before `stream.open`.
  Future<CodexStream> subscribe(
    String threadId, {
    required void Function(CodexFrame frame) onFrame,
    void Function(String reason)? onClosed,
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
      if (data is! Map || data['streamId'] != streamId) return;
      if (event.event == 'codex.app.stream.closed') {
        if (markClosed()) onClosed?.call(data['reason'] is String ? data['reason'] as String : 'failed');
        return;
      }
      final frame = data['frame'];
      if (event.event != 'codex.app.frame' || frame is! Map || frame['method'] is! String) return;
      onFrame(CodexFrame(frame['method'] as String, frame['params']));
    });
    unawaited(
      _rpc.done.then((_) {
        if (!opened) {
          markClosed();
        } else if (markClosed()) {
          onClosed?.call('peer-disconnected');
        }
      }),
    );
    final Object? result;
    try {
      result = await _rpc.call('codex.app.stream.open', {'streamId': streamId, 'threadId': threadId});
      opened = true;
    } on Object {
      markClosed();
      rethrow;
    }
    return CodexStream._(streamId, result is Map && result['replayTruncated'] == true, () async {
      if (!markClosed()) return;
      try {
        await _rpc.call('codex.app.stream.close', {'streamId': streamId});
      } on Object {
        // Best effort.
      }
    });
  }

  Future<Object?> _transfer(Uint8List encoded) async {
    if (encoded.isEmpty || encoded.length > maxCodexTransferBytes) {
      throw const RemoteRpcException('INVALID_MESSAGE', 'The Codex request exceeds the transfer limit.', isLocal: true);
    }
    final transferId = _newId();
    final totalChunks = (encoded.length / codexTransferChunkBytes).ceil();
    var opened = false;
    try {
      await _rpc.call('codex.app.transfer.open', {
        'transferId': transferId,
        'totalBytes': encoded.length,
        'totalChunks': totalChunks,
      });
      opened = true;
      for (var index = 0; index < totalChunks; index++) {
        final start = index * codexTransferChunkBytes;
        final end = min(start + codexTransferChunkBytes, encoded.length);
        await _rpc.call('codex.app.transfer.chunk', {
          'transferId': transferId,
          'index': index,
          'data': base64.encode(Uint8List.sublistView(encoded, start, end)),
        });
      }
      final commit = await _rpc.call('codex.app.transfer.commit', {'transferId': transferId});
      if (commit is! Map) throw const FormatException('Invalid Codex transfer descriptor.');
      if (commit['kind'] == 'inline') return commit['response'];
      final total = commit['totalBytes'];
      final chunks = commit['totalChunks'];
      if (commit['transferId'] != transferId ||
          total is! int ||
          total <= 0 ||
          total > maxCodexTransferBytes ||
          chunks is! int ||
          chunks != (total / codexTransferChunkBytes).ceil()) {
        throw const FormatException('Invalid Codex transfer descriptor.');
      }
      final out = BytesBuilder(copy: false);
      for (var index = 0; index < chunks; index++) {
        final part = await _rpc.call('codex.app.transfer.read', {'transferId': transferId, 'index': index});
        if (part is! Map || part['transferId'] != transferId || part['index'] != index || part['data'] is! String) {
          throw const FormatException('Invalid Codex transfer ordering.');
        }
        final bytes = base64.decode(part['data'] as String);
        if (bytes.length != min(codexTransferChunkBytes, total - out.length)) {
          throw const FormatException('Invalid Codex transfer chunk.');
        }
        out.add(bytes);
      }
      return jsonDecode(utf8.decode(out.takeBytes()));
    } finally {
      if (opened) {
        try {
          await _rpc.call('codex.app.transfer.close', {'transferId': transferId}, timeout: const Duration(seconds: 5));
        } on Object {
          // Host cleans idle transfers after 2 minutes.
        }
      }
    }
  }
}

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
