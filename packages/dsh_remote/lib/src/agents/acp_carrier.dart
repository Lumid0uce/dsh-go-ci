// SPDX-FileCopyrightText: 2026 DSH Remote contributors
// SPDX-License-Identifier: MIT
// See THIRD_PARTY_NOTICES.md.
// Claude Code (ACP `agent.acp.*`) projected as a DSH Harness surface.
// Mirrors ds-harness-remote `plugin/src/acp/virtual-harness.ts` +
// `virtual-live.ts` for the `claude` backend: in-memory only, text prompts,
// single-use approvals (no allow-always).
library;

import 'dart:async';
import 'dart:convert';

import '../carrier/tunnel_carrier.dart';
import 'acp_client.dart';
import 'virtual_link.dart';

/// 1 MiB live terminal-output buffer per tool call.
const int _maxToolOutputChars = 1024 * 1024;

class _ToolCall {
  _ToolCall(this.name);
  String name;
  Object? input;
  final StringBuffer output = StringBuffer();
  bool overflow = false;
}

class _LiveState {
  AcpStream? stream;
  Future<void>? opening;
  final Map<String, _ToolCall> tools = {};
  final Set<String> activeTools = {};
}

/// [DshCarrier] driving the Host's Claude Code through `agent.acp.*`.
class AcpClaudeCarrier extends VirtualAgentCarrier {
  AcpClaudeCarrier(super.connect);

  static const String backend = AcpBackends.claude;
  static const String _prefix = 'claude:';

  AcpRemoteClient? _client;
  final Map<String, _LiveState> _live = {};

  @override
  String get kind => 'acp-claude';

  @override
  Set<String> get endpoints => const {
    'session/create',
    'session/prompt',
    'session/cancel',
    'session/rename',
    'session/modelCatalog',
    'session/selectModel',
  };

  AcpRemoteClient get client {
    final current = _client;
    if (current == null) throw const VirtualFailure('gateway/internal', 'Claude is not connected.');
    return current;
  }

  String _native(String sessionId) =>
      sessionId.startsWith(_prefix) ? sessionId.substring(_prefix.length) : sessionId;

  @override
  String workspaceIdFor(String path) => 'claude:cwd:${Uri.encodeComponent(path)}';

  @override
  void bindSession(RemoteHostSession session) {
    _client = AcpRemoteClient(session.rpc, capabilities: session.capabilities);
    // Upstream streams die with the old connection; history is re-read.
    for (final state in _live.values) {
      final stream = state.stream;
      if (stream != null) unawaited(stream.close());
    }
    _live.clear();
    for (final log in logs.values) {
      if (!log.blank) log.hydrated = false;
      if (log.running) {
        log.endTurn({'kind': 'interrupted'});
        log.running = false;
      }
    }
  }

  @override
  Future<void> loadCatalog() async {
    final remote = client;
    final listed = await remote.listWorkspaces(backend);
    for (final item in listed) {
      final workspace = upsertWorkspace(item.path, title: item.title);
      final sessions = await remote.listSessions(item.path, backend);
      for (final summary in sessions) {
        final log = registerLog(
          VirtualSessionLog(
            '$_prefix${summary.conversationId}',
            cwd: item.path,
            createdAt: summary.createdAt > 0 ? summary.createdAt : null,
            updatedAt: summary.updatedAt > 0 ? summary.updatedAt : null,
            title: summary.title.isEmpty ? null : summary.title,
          ),
        );
        attachToWorkspace(log, workspace, publish: false);
      }
      workspace.sessionIds.sort((a, b) => (logs[b]?.updatedAt ?? 0).compareTo(logs[a]?.updatedAt ?? 0));
    }
  }

  Future<void> _hydrate(VirtualSessionLog log) async {
    if (log.hydrated || log.blank) {
      log.hydrated = true;
      return;
    }
    // The gateway accepts the prefixed id and resolves the native one.
    final details = await client.loadSessionDetails(log.sessionId, backend);
    log.resetHistory(details.events);
    final preset = details.permissionPreset;
    if ((preset == 'workspace-write' || preset == 'danger-full-access') && !log.projections.containsKey('permissions')) {
      log.setProjection('permissions', {'currentValue': preset});
    }
    if (log.title == null) log.setTitle(_firstUserText(log));
    if (details.running == true && !log.running) emitStatus(log, true);
  }

  @override
  Future<void> attachLive(VirtualSessionLog log) async {
    await _hydrate(log);
    final state = _live.putIfAbsent(log.sessionId, _LiveState.new);
    if (state.stream != null) return;
    final opening = state.opening;
    if (opening != null) return opening;
    final future = client
        .openStream(
          _native(log.sessionId),
          onFrame: (frame) => _acceptFrame(log, state, frame),
          onClosed: (closed) => _streamClosed(log, state, closed.reason),
        )
        .then((stream) {
          if (!identical(_live[log.sessionId], state)) {
            unawaited(stream.close());
            return;
          }
          state.stream = stream;
        });
    state.opening = future;
    try {
      await future;
    } finally {
      state.opening = null;
    }
  }

  void _streamClosed(VirtualSessionLog log, _LiveState state, String reason) {
    state.stream = null;
    if (identical(_live[log.sessionId], state)) _live.remove(log.sessionId);
    if (log.running) {
      log.endTurn(
        reason == 'cancelled'
            ? {
                'kind': 'aborted',
                'reason': {'kind': 'user'},
              }
            : {'kind': 'interrupted'},
      );
      retractApprovalsOf(log.sessionId);
      emitStatus(log, false);
    }
  }

  String _permission(VirtualSessionLog log) =>
      asText(asJsonMap(log.projections['permissions'])['currentValue']) ?? 'workspace-write';

  void _acceptFrame(VirtualSessionLog log, _LiveState state, AcpFrame frame) {
    final params = frame.paramsMap ?? const <String, Object?>{};
    if (frame.method == 'session/request_permission') {
      final handle = asText(params['requestHandle']);
      if (handle == null) return;
      if (_permission(log) == 'danger-full-access') {
        unawaited(client.respond(handle, 'allow-once').catchError((Object _) => null));
        return;
      }
      final toolCall = asJsonMap(params['toolCall']);
      final reason = asText(params['reason']) ?? asText(asJsonMap(toolCall['rawInput'])['message']);
      emitApproval(handle, log.sessionId, {
        'toolName': asText(params['toolName']) ?? asText(toolCall['title']) ?? 'permission',
        'method': frame.method,
        'reason': ?reason,
      });
      return;
    }
    if (frame.method == 'session/update') _acceptUpdate(log, state, asJsonMap(params['update']));
  }

  void _acceptUpdate(VirtualSessionLog log, _LiveState state, Map<String, Object?> update) {
    final kind = asText(update['sessionUpdate']) ?? '';
    switch (kind) {
      case 'agent_message_chunk' || 'agent_message':
        final text = _extractText(update);
        if (text != null) log.appendDelta('text', text);
      case 'agent_thought_chunk':
        final text = _extractText(update);
        if (text != null) log.appendDelta('reasoning', text);
      case 'tool_call' || 'tool_call_update':
        _acceptTool(log, state, update, kind);
      case 'session_info_update':
        log.setTitle(asText(update['title']));
      case 'current_mode_update':
        final mode = asText(update['currentModeId']);
        if (mode != null) log.setProjection('agentPreset', mode);
      case 'usage_update':
        final used = update['used'];
        final size = update['size'];
        if (used is num && size is num && size > 0) {
          log.setProjection('contextPressure', {'used': used, 'size': size, 'ratio': used / size});
        }
      case 'plan':
        final entries = update['entries'];
        if (entries is List) {
          log.setProjection('todos', [
            for (final entry in entries)
              if (entry is Map && entry['content'] is String)
                {'content': entry['content'], 'status': asText(entry['status']) ?? 'pending'},
          ]);
        }
      case 'model_selection':
        final selected = update['selected'];
        if (selected is Map) log.setProjection('modelSelection', {'lastUsed': selected, 'next': selected});
      case 'prompt_completed' || 'prompt_failed':
        final catchUp = update['catchUp'];
        if (!log.stepOpen && !log.hasLiveContent && catchUp is List) {
          for (final item in catchUp) {
            if (item is Map && item['method'] == 'session/update') {
              final itemParams = asJsonMap(item['params']);
              _acceptUpdate(log, state, asJsonMap(itemParams['update']));
            }
          }
        }
        log.endTurn(_turnEndReason(kind, update), provider: backend);
        state.activeTools.clear();
        state.tools.clear();
        retractApprovalsOf(log.sessionId);
        if (log.title == null) log.setTitle(_firstUserText(log));
        emitStatus(log, false);
        _touchWorkspace(log);
      default:
        // user_message_chunk (our own prompt echo), available_commands_update, ...
        break;
    }
  }

  void _acceptTool(VirtualSessionLog log, _LiveState state, Map<String, Object?> update, String kind) {
    final title = asText(update['title']) ?? asText(update['toolName']) ?? asText(update['name']) ?? 'tool';
    final status = asText(update['status']);
    final callId = asText(update['toolCallId']) ?? asText(update['callId']) ?? title;
    final terminal = status == 'completed' || status == 'failed';
    final call = state.tools.putIfAbsent(callId, () => _ToolCall(title));
    if (update['title'] is String) call.name = update['title'] as String;
    if (update.containsKey('parameters')) {
      call.input = update['parameters'];
    } else if (update.containsKey('rawInput')) {
      call.input = update['rawInput'];
    }
    final termDelta = asJsonMap(asJsonMap(update['_meta'])['terminal_output'])['data'];
    if (termDelta is String && !call.overflow) {
      if (call.output.length + termDelta.length > _maxToolOutputChars) {
        call.output.clear();
        call.overflow = true;
      } else {
        call.output.write(termDelta);
      }
    }
    if (!state.activeTools.contains(callId)) {
      if (!terminal && kind == 'tool_call_update' && status != 'in_progress' && status != 'pending') return;
      // Commit streamed text first so the transcript reads text → tool → text.
      log.finishAssistant(provider: backend);
      log.pushToolCall(
        callId,
        call.name,
        call.input,
        status: status == 'completed'
            ? 'finished'
            : status == 'failed'
            ? 'failed'
            : 'running',
      );
      if (!terminal) state.activeTools.add(callId);
    } else if (!terminal) {
      return;
    }
    if (!terminal) return;
    var output = _toolOutput(update);
    if (output == null || output.isEmpty) {
      if (call.overflow) {
        output = '[Tool output exceeds the live preview limit; reload history to read the bounded persisted result.]';
      } else if (call.output.isNotEmpty) {
        output = call.output.toString();
      } else {
        final raw = update['rawOutput'];
        output = raw is String ? raw : (raw == null ? '' : jsonEncode(raw));
      }
    }
    log.pushToolResult(callId, output, isError: status == 'failed');
    state.tools.remove(callId);
    state.activeTools.remove(callId);
  }

  void _touchWorkspace(VirtualSessionLog log) {
    final workspace = workspaces[log.workspaceId];
    if (workspace == null) return;
    workspace.updatedAt = log.updatedAt;
    workspace.sessionIds
      ..remove(log.sessionId)
      ..insert(0, log.sessionId);
    publishWorkspace(workspace);
  }

  @override
  Future<void> respondApproval(String handle, String decision) async {
    await client.respond(handle, switch (decision) {
      'allow' => 'allow-once',
      'reject' => 'reject-once',
      _ => 'cancel',
    });
  }

  @override
  Future<Object?> listDirectory(String path) => client.listDirectory(path);

  @override
  Future<Object?> dispatchAgent(String endpoint, Map<String, Object?> args) async {
    final request = requestArg(args);
    switch (endpoint) {
      case 'session/create':
        return _create(request);
      case 'session/prompt':
        return _prompt(request);
      case 'session/cancel':
        final log = requireLog(requestSessionId(args));
        await client.cancel(_native(log.sessionId));
        return null;
      case 'session/rename':
        final log = requireLog(requestSessionId(args));
        final title = (asText(request['title']) ?? '').trim();
        if (title.isEmpty) throw const VirtualFailure('gateway/bad-request', 'title is required.');
        log.setTitle(title);
        return {'title': log.title};
      case 'session/modelCatalog':
        return _modelCatalog(requestSessionId(args));
      case 'session/selectModel':
        final log = requireLog(requestSessionId(args));
        if (log.running) throw const VirtualFailure('PROMPT_IN_PROGRESS', 'Stop the Claude reply before changing its model.');
        final model = asText(request['model']);
        if (model == null || model.isEmpty) throw const VirtualFailure('gateway/bad-request', 'model is required.');
        final result = asJsonMap(
          await client.selectModel(
            _native(log.sessionId),
            backend,
            model,
            reasoningEffort: asText(request['reasoningEffort']),
          ),
        );
        final selected = asJsonMap(result['selected']);
        if (selected['model'] is! String) throw const FormatException('ACP did not confirm the selected model.');
        log.setProjection('modelSelection', {'lastUsed': selected, 'next': selected});
        return {'selected': selected};
    }
    throw VirtualFailure('METHOD_NOT_ALLOWED', '$endpoint is not available for $kind.');
  }

  Future<Object?> _create(Map<String, Object?> request) async {
    await ensureCatalog();
    final workspaceId = asText(request['workspaceId']);
    final cwd = asText(request['cwd']) ?? (workspaceId == null ? null : workspaces[workspaceId]?.path);
    if (cwd == null || cwd.isEmpty) {
      throw const VirtualFailure('session/workspace-not-found', 'The Claude workspace was not found.');
    }
    final reuse = logs[asText(request['sessionId'])];
    if (reuse != null && reuse.cwd == cwd && reuse.blank && !reuse.running) return {'sessionId': reuse.sessionId};
    final nativeId = await client.createSession(cwd, backend);
    final sessionId = '$_prefix$nativeId';
    if (logs.containsKey(sessionId)) {
      throw const VirtualFailure('session/already-exists', 'Claude returned an existing conversation for a new session.');
    }
    final log = registerLog(VirtualSessionLog(sessionId, cwd: cwd))
      ..blank = true
      ..hydrated = true;
    final workspace = upsertWorkspace(cwd);
    attachToWorkspace(log, workspace);
    emitSessionAdded(log);
    return {'sessionId': sessionId};
  }

  Future<Object?> _prompt(Map<String, Object?> request) async {
    final log = requireLog(asText(request['sessionId']));
    if (log.running) throw const VirtualFailure('PROMPT_IN_PROGRESS', 'Claude is still replying.');
    final content = request['content'];
    final parts = content is List ? content : const <Object?>[];
    final texts = <String>[];
    for (final part in parts) {
      final map = asJsonMap(part);
      if (map['type'] != 'text') {
        throw const VirtualFailure('session/attachment-error', 'Claude accepts text prompts only.');
      }
      final text = asText(map['text']);
      if (text != null && text.isNotEmpty) texts.add(text);
    }
    final text = texts.join('\n');
    if (text.trim().isEmpty) throw const VirtualFailure('session/attachment-error', 'A prompt is required.');
    await attachLive(log);
    log.pushUserText(text, requestId: asText(request['requestId']));
    if (log.title == null) log.setTitle(_titleFrom(text));
    emitStatus(log, true);
    _touchWorkspace(log);
    try {
      // The gateway answers `{accepted, stopReason: 'in_progress'}` at once;
      // the turn ends with a `prompt_completed` / `prompt_failed` frame.
      await client.prompt(_native(log.sessionId), backend, text);
    } on Object catch (error) {
      final failure = failureOf(error);
      log.endTurn({
        'kind': 'error',
        'error': {'code': failure.code, 'message': failure.message},
      }, provider: backend);
      emitStatus(log, false);
      rethrow;
    }
    return {'accepted': true};
  }

  Future<Object?> _modelCatalog(String? sessionId) async {
    await ensureCatalog();
    var log = sessionId == null ? null : logs[sessionId];
    if (log == null) {
      final sorted = logs.values.where((l) => !l.blank).toList()..sort((a, b) => b.updatedAt.compareTo(a.updatedAt));
      log = sorted.isEmpty ? null : sorted.first;
    }
    Map<String, Object?> fallback([String? message]) => {
      'default': {'provider': backend, 'model': 'host-settings'},
      'routableProviders': const <String>[],
      'groups': [
        {'id': backend, 'name': 'Claude', 'models': const <Object?>[]},
      ],
      'failures': [
        if (message != null) {'id': backend, 'name': 'Claude', 'message': message},
      ],
    };
    if (log == null) return fallback();
    try {
      final models = asJsonMap(await client.sessionModels(_native(log.sessionId), backend));
      final current = asJsonMap(models['current']);
      if (current['model'] is String) log.setProjection('modelSelection', {'lastUsed': current, 'next': current});
      return {
        'default': current,
        'routableProviders': models['routable'] == true ? [current['provider'] ?? backend] : const <String>[],
        'groups': models['groups'] is List ? models['groups'] : const <Object?>[],
        'failures': models['failures'] is List ? models['failures'] : const <Object?>[],
      };
    } on Object catch (error) {
      return fallback(failureOf(error).message);
    }
  }
}

Map<String, Object?> _turnEndReason(String kind, Map<String, Object?> update) {
  final stopReason = asText(update['stopReason']);
  if (kind == 'prompt_failed' || stopReason == 'error') {
    return {
      'kind': 'error',
      'error': {
        'code': asText(update['code']) ?? 'ACP_PROMPT_FAILED',
        'message': asText(update['message']) ?? 'Claude failed to answer.',
      },
    };
  }
  return switch (stopReason) {
    'cancelled' => {
      'kind': 'aborted',
      'reason': {'kind': 'user'},
    },
    'max_tokens' => {'kind': 'max-tokens'},
    _ => {'kind': 'completed'},
  };
}

String? _extractText(Map<String, Object?> update) {
  final content = update['content'];
  if (content is Map && content['text'] is String) return content['text'] as String;
  if (content is List) {
    final out = StringBuffer();
    for (final part in content) {
      if (part is Map && part['text'] is String) out.write(part['text']);
    }
    return out.isEmpty ? null : out.toString();
  }
  if (content is String) return content;
  return asText(update['text']);
}

/// ACP tool-call `content` / `diff` as display text.
String? _toolOutput(Map<String, Object?> update) {
  final content = update['content'];
  if (content is List && content.isNotEmpty) {
    final out = StringBuffer();
    for (final item in content) {
      if (item is! Map) continue;
      final inner = item['content'];
      if (inner is Map && inner['text'] is String) {
        out.writeln(inner['text']);
      } else if (item['type'] == 'diff' && item['path'] is String) {
        out.writeln('--- ${item['path']}');
        if (item['newText'] is String) out.writeln(item['newText']);
      } else if (item['text'] is String) {
        out.writeln(item['text']);
      }
    }
    final text = out.toString().trimRight();
    if (text.isNotEmpty) return text;
  }
  final diff = update['diff'];
  if (diff is String && diff.isNotEmpty) return diff;
  return null;
}

String? _firstUserText(VirtualSessionLog log) {
  for (final event in log.events) {
    if (event['type'] != 'user/message') continue;
    final data = asJsonMap(event['data']);
    final message = data['message'] is Map ? asJsonMap(data['message']) : data;
    final content = message['content'];
    if (content is List) {
      for (final part in content) {
        if (part is Map && part['type'] == 'text' && part['text'] is String) return _titleFrom(part['text'] as String);
      }
    }
  }
  return null;
}

String _titleFrom(String text) {
  final line = text.trim().split('\n').first.trim();
  return line.length > 80 ? '${line.substring(0, 80)}…' : line;
}
