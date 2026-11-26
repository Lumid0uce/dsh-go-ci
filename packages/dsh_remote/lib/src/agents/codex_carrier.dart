// SPDX-FileCopyrightText: 2026 DSH Remote contributors
// SPDX-License-Identifier: MIT
// See THIRD_PARTY_NOTICES.md.
// Codex (App Server `codex.app.*`) projected as a DSH Harness surface.
// Mirrors ds-harness-remote `plugin/src/codex/virtual-*.ts`: in-memory only,
// text prompts, accept / decline / cancel approvals.
library;

import 'dart:async';
import 'dart:convert';

import '../carrier/tunnel_carrier.dart';
import 'codex_client.dart';
import 'virtual_link.dart';

const String _provider = 'codex';
const int _maxPages = 20;
const int _maxHistoryPages = 10;
const int _maxToolOutputChars = 256 * 1024;

class _ThreadLive {
  CodexStream? stream;
  Future<void>? opening;
  String? activeTurnId;
  String? completedTurnId;

  /// A prompt sent from here already pushed `turn/start` + `user/message`.
  bool localPrompt = false;
  final Set<String> startedTools = {};
  final Set<String> completedItems = {};
  final Map<String, String> toolOutput = {};
}

/// [DshCarrier] driving the Host's Codex App Server through `codex.app.*`.
class CodexCarrier extends VirtualAgentCarrier {
  CodexCarrier(super.connect);

  static const String _prefix = 'codex:';

  CodexRemoteClient? _client;
  final Map<String, _ThreadLive> _live = {};
  Map<String, Object?>? _modelCatalog;

  @override
  String get kind => 'codex';

  @override
  Set<String> get endpoints => const {
    'session/create',
    'session/prompt',
    'session/cancel',
    'session/rename',
    'session/modelCatalog',
    'session/selectModel',
  };

  CodexRemoteClient get client {
    final current = _client;
    if (current == null) throw const VirtualFailure('gateway/internal', 'Codex is not connected.');
    return current;
  }

  String _thread(String sessionId) =>
      sessionId.startsWith(_prefix) ? sessionId.substring(_prefix.length) : sessionId;

  @override
  String workspaceIdFor(String path) {
    for (final workspace in workspaces.values) {
      if (workspace.path == path) return workspace.workspaceId;
    }
    return 'codex-workspace:cwd:${Uri.encodeComponent(path)}';
  }

  @override
  void bindSession(RemoteHostSession session) {
    _client = CodexRemoteClient(session.rpc, capabilities: session.capabilities);
    for (final live in _live.values) {
      final stream = live.stream;
      if (stream != null) unawaited(stream.close());
    }
    _live.clear();
    _modelCatalog = null;
    for (final log in logs.values) {
      if (!log.blank) log.hydrated = false;
      if (log.running) {
        log.endTurn({'kind': 'interrupted'});
        log.running = false;
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Catalog

  Future<List<Map<String, Object?>>> _paged(Future<Object?> Function(String? cursor) read) async {
    final out = <Map<String, Object?>>[];
    String? cursor;
    for (var page = 0; page < _maxPages; page++) {
      final result = asJsonMap(await read(cursor));
      final data = result['data'];
      if (data is List) {
        for (final item in data) {
          if (item is Map) out.add(item.cast<String, Object?>());
        }
      }
      final next = asText(result['nextCursor']);
      if (next == null || next.isEmpty) break;
      cursor = next;
    }
    return out;
  }

  @override
  Future<void> loadCatalog() async {
    final remote = client;
    final results = await Future.wait([
      _paged((cursor) => remote.projects(cursor: cursor)).catchError((Object _) => <Map<String, Object?>>[]),
      _paged((cursor) => remote.threads(cursor: cursor, limit: 100, archived: false)),
    ]);
    final projects = results[0];
    final threads = results[1];
    final roots = <({String path, VirtualWorkspace workspace})>[];
    for (final project in projects) {
      final id = asText(project['id']);
      final rootList = project['roots'];
      final root = rootList is List && rootList.isNotEmpty ? asText(asJsonMap(rootList.first)['path']) : null;
      if (id == null || root == null || root.isEmpty) continue;
      final workspaceId = 'codex-workspace:project:$id';
      final workspace = workspaces[workspaceId] ??= VirtualWorkspace(
        workspaceId: workspaceId,
        path: root,
        title: asText(project['name']) ?? _basename(root),
        createdAt: _epochMs(project['createdAt']),
        updatedAt: _epochMs(project['updatedAt']),
      );
      roots.add((path: root, workspace: workspace));
    }
    // Longest root first: nested projects win.
    roots.sort((a, b) => b.path.length.compareTo(a.path.length));
    for (final thread in threads) {
      final threadId = asText(thread['id']);
      if (threadId == null) continue;
      final cwd = asText(thread['cwd']);
      final projectId = asText(thread['projectId']);
      VirtualWorkspace? workspace = projectId == null ? null : workspaces['codex-workspace:project:$projectId'];
      if (workspace == null && cwd != null) {
        for (final root in roots) {
          if (_contains(root.path, cwd)) {
            workspace = root.workspace;
            break;
          }
        }
        if (workspace == null && projects.isEmpty) workspace = upsertWorkspace(cwd);
      }
      if (workspace == null) continue;
      final name = asText(thread['name']) ?? asText(thread['preview']);
      final log = registerLog(
        VirtualSessionLog(
          '$_prefix$threadId',
          cwd: cwd ?? workspace.path,
          createdAt: _epochMs(thread['createdAt']),
          updatedAt: _epochMs(thread['updatedAt']),
          title: name == null || name.trim().isEmpty ? null : _titleFrom(name),
        ),
      );
      attachToWorkspace(log, workspace, publish: false);
    }
    for (final workspace in workspaces.values) {
      workspace.sessionIds.sort((a, b) => (logs[b]?.updatedAt ?? 0).compareTo(logs[a]?.updatedAt ?? 0));
    }
  }

  // ---------------------------------------------------------------------------
  // History + live stream

  Future<void> _hydrate(VirtualSessionLog log, _ThreadLive live) async {
    if (log.hydrated || log.blank) {
      log.hydrated = true;
      return;
    }
    final threadId = _thread(log.sessionId);
    var page = asJsonMap(await client.sessionHistory(threadId));
    final records = <Object?>[...(page['records'] is List ? page['records'] as List : const <Object?>[])];
    final activeTurnId = asText(page['activeTurnId']);
    for (var i = 1; i < _maxHistoryPages && page['hasMore'] == true && records.isNotEmpty; i++) {
      final firstSeq = asJsonMap(asJsonMap(records.first)['event'])['seq'];
      if (firstSeq is! int || firstSeq <= 0) break;
      page = asJsonMap(await client.sessionHistory(threadId, beforeSeq: firstSeq));
      final older = page['records'];
      if (older is! List || older.isEmpty) break;
      records.insertAll(0, older);
    }
    log.resetHistory(records);
    final header = asJsonMap(page['header']);
    log.cwd ??= asText(header['cwd']);
    if (log.title == null) log.setTitle(_firstUserText(log));
    if (activeTurnId != null) {
      live.activeTurnId = activeTurnId;
      // History ends mid-turn: keep the turn open for the live frames.
      log.stepOpen = true;
      if (!log.running) emitStatus(log, true);
    }
  }

  @override
  Future<void> attachLive(VirtualSessionLog log) async {
    final live = _live.putIfAbsent(log.sessionId, _ThreadLive.new);
    await _hydrate(log, live);
    if (live.stream != null) return;
    final opening = live.opening;
    if (opening != null) return opening;
    final future = client
        .subscribe(
          _thread(log.sessionId),
          onFrame: (frame) => _acceptFrame(log, live, frame),
          onClosed: (reason) => _streamClosed(log, live),
        )
        .then((stream) {
          if (!identical(_live[log.sessionId], live)) {
            unawaited(stream.close());
            return;
          }
          live.stream = stream;
        });
    live.opening = future;
    try {
      await future;
    } finally {
      live.opening = null;
    }
  }

  void _streamClosed(VirtualSessionLog log, _ThreadLive live) {
    live.stream = null;
    if (identical(_live[log.sessionId], live)) _live.remove(log.sessionId);
    // Re-read history on the next follow; the transcript may have moved on.
    if (!log.blank) log.hydrated = false;
    if (log.running) {
      log.endTurn({'kind': 'interrupted'});
      retractApprovalsOf(log.sessionId);
      emitStatus(log, false);
    }
  }

  void _acceptFrame(VirtualSessionLog log, _ThreadLive live, CodexFrame frame) {
    final params = frame.paramsMap ?? const <String, Object?>{};
    switch (frame.method) {
      case 'thread/name/updated':
        final name = asText(params['threadName']);
        if (name != null && name.trim().isNotEmpty) log.setTitle(_titleFrom(name));
      case 'turn/started':
        final turn = asJsonMap(params['turn']);
        live.activeTurnId = asText(turn['id']);
        live.startedTools.clear();
        live.toolOutput.clear();
        if (!(live.localPrompt && log.stepOpen)) {
          log.finishAssistant(provider: _provider, model: _model(log));
          log.ensureStep();
        }
        if (!log.running) emitStatus(log, true);
      case 'item/started':
        final item = asJsonMap(params['item']);
        if (_isTool(asText(item['type']))) _startTool(log, live, item);
      case 'item/completed':
        _completeItem(log, live, asJsonMap(params['item']));
      case 'item/agentMessage/delta':
        final delta = asText(params['delta']);
        if (delta != null) log.appendDelta('text', delta);
      case 'item/reasoning/summaryTextDelta' || 'item/reasoning/textDelta' || 'item/plan/delta':
        final delta = asText(params['delta']);
        if (delta != null) log.appendDelta('reasoning', delta);
      case 'turn/plan/updated':
        final plan = params['plan'];
        if (plan is List) {
          log.setProjection('todos', [
            for (final entry in plan)
              if (entry is Map && entry['step'] is String)
                {
                  'content': entry['step'],
                  'status': switch (entry['status']) {
                    'inProgress' => 'in_progress',
                    'completed' => 'completed',
                    _ => 'pending',
                  },
                },
          ]);
        }
      case 'item/commandExecution/outputDelta' || 'item/fileChange/outputDelta' || 'item/mcpToolCall/progress':
        final itemId = asText(params['itemId']);
        final delta = frame.method == 'item/mcpToolCall/progress' ? asText(params['message']) : asText(params['delta']);
        if (itemId == null || delta == null) return;
        final previous = live.toolOutput[itemId] ?? '';
        var next = previous.isEmpty || frame.method != 'item/mcpToolCall/progress' ? '$previous$delta' : '$previous\n$delta';
        if (next.length > _maxToolOutputChars) {
          next = '… earlier output omitted …\n${next.substring(next.length - _maxToolOutputChars)}';
        }
        live.toolOutput[itemId] = next;
      case 'model/rerouted':
        final model = asText(params['toModel']);
        if (model != null) {
          final previous = _selection(log);
          final selected = {'provider': _provider, 'model': model, 'reasoningEffort': ?previous?['reasoningEffort']};
          log.setProjection('modelSelection', {'lastUsed': selected, 'next': selected});
        }
      case 'thread/status/changed':
        final status = asJsonMap(params['status']);
        if ((status['type'] == 'idle' || status['type'] == 'systemError') && live.activeTurnId == null && log.running) {
          emitStatus(log, false);
        }
      case 'turn/completed':
        _finishTurn(log, live, asJsonMap(params['turn']));
      case 'item/commandExecution/requestApproval' || 'item/fileChange/requestApproval':
        final handle = asText(params['requestHandle']);
        if (handle == null) return;
        final command = _commandText(params['command']);
        emitApproval(handle, log.sessionId, {
          'toolName': frame.method.contains('fileChange') ? 'CodeX file change' : 'CodeX command',
          'reason': ?command,
        });
    }
  }

  void _startTool(VirtualSessionLog log, _ThreadLive live, Map<String, Object?> item) {
    final id = asText(item['id']);
    if (id == null || live.startedTools.contains(id)) return;
    live.startedTools.add(id);
    log.finishAssistant(provider: _provider, model: _model(log));
    log.pushToolCall(id, _toolName(asText(item['type']) ?? 'tool'), _toolArguments(item));
  }

  void _completeItem(VirtualSessionLog log, _ThreadLive live, Map<String, Object?> item) {
    final type = asText(item['type']);
    final id = asText(item['id']);
    if (id != null && !live.completedItems.add(id)) return;
    switch (type) {
      case 'userMessage':
        if (live.localPrompt) {
          live.localPrompt = false;
          return;
        }
        final text = _itemText(item);
        if (text != null) log.pushUserText(text);
      case 'agentMessage':
        if (log.hasLiveContent) {
          log.finishAssistant(provider: _provider, model: _model(log));
        } else {
          final text = _itemText(item);
          if (text != null) {
            log.pushAssistantParts([
              {'type': 'text', 'text': text},
            ], provider: _provider, model: _model(log));
          }
        }
      case 'reasoning':
        if (log.hasLiveContent) return; // Committed with the next message.
        final summary = item['summary'];
        final text = summary is List ? summary.whereType<String>().join('\n') : _itemText(item);
        if (text != null && text.isNotEmpty) {
          log.pushAssistantParts([
            {'type': 'reasoning', 'text': text},
          ], provider: _provider, model: _model(log));
        }
      case 'error':
        log.finishAssistant(provider: _provider, model: _model(log));
        log.pushAssistantParts([
          {'type': 'text', 'text': _itemText(item) ?? 'CodeX reported an error.'},
        ], provider: _provider, model: _model(log));
      default:
        if (!_isTool(type) || id == null) return;
        if (!live.startedTools.contains(id)) {
          log.finishAssistant(provider: _provider, model: _model(log));
          log.pushToolCall(id, _toolName(type!), _toolArguments(item), status: item['status'] == 'failed' ? 'failed' : 'finished');
        }
        live.startedTools.remove(id);
        final failed = item['status'] == 'failed' || item['success'] == false;
        log.pushToolResult(id, _toolResultText(item, type!, live.toolOutput.remove(id)), isError: failed);
    }
  }

  void _finishTurn(VirtualSessionLog log, _ThreadLive live, Map<String, Object?> turn) {
    final id = asText(turn['id']);
    if (id != null && (id == live.completedTurnId || (live.activeTurnId != null && id != live.activeTurnId))) return;
    final items = turn['items'];
    if (items is List) {
      for (final item in items) {
        if (item is Map) _completeItem(log, live, item.cast<String, Object?>());
      }
    }
    log.endTurn(_turnEndReason(turn), provider: _provider, model: _model(log));
    live.completedTurnId = id ?? live.activeTurnId;
    live.activeTurnId = null;
    live.localPrompt = false;
    live.startedTools.clear();
    live.toolOutput.clear();
    retractApprovalsOf(log.sessionId);
    if (log.title == null) log.setTitle(_firstUserText(log));
    emitStatus(log, false);
    _touchWorkspace(log);
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

  // ---------------------------------------------------------------------------
  // Models and permissions

  Map<String, Object?>? _selection(VirtualSessionLog log) {
    final projection = asJsonMap(log.projections['modelSelection']);
    final next = projection['next'];
    return next is Map ? next.cast<String, Object?>() : null;
  }

  String _model(VirtualSessionLog log) => asText(_selection(log)?['model']) ?? '';

  String _permission(VirtualSessionLog log) =>
      asText(asJsonMap(log.projections['permissions'])['currentValue']) ?? CodexPermissionPreset.workspaceWrite;

  Future<Map<String, Object?>> _catalog() async {
    final cached = _modelCatalog;
    if (cached != null) return cached;
    final remote = client;
    final models = await _paged((cursor) => remote.call('model/list', {'limit': 100, 'includeHidden': false, 'cursor': ?cursor}));
    final views = <Map<String, Object?>>[];
    Map<String, Object?>? defaultSelection;
    final seen = <String>{};
    for (final source in models) {
      if (source['hidden'] == true) continue;
      final id = asText(source['model']) ?? asText(source['id']);
      if (id == null || !seen.add(id)) continue;
      final efforts = [
        for (final value in source['supportedReasoningEfforts'] is List ? source['supportedReasoningEfforts'] as List : const <Object?>[])
          if (value is Map && value['reasoningEffort'] is String)
            {
              'id': value['reasoningEffort'],
              'name': _effortName(value['reasoningEffort'] as String),
              'description': ?asText(value['description']),
            },
      ];
      final defaultEffort = asText(source['defaultReasoningEffort']);
      final validDefault = defaultEffort != null && efforts.any((e) => e['id'] == defaultEffort) ? defaultEffort : null;
      views.add({
        'id': id,
        'name': asText(source['displayName']) ?? id,
        'description': ?asText(source['description']),
        if (efforts.isNotEmpty) 'reasoning': {'efforts': efforts, 'defaultEffort': ?validDefault},
      });
      if (source['isDefault'] == true || defaultSelection == null) {
        defaultSelection = {'provider': _provider, 'model': id, 'reasoningEffort': ?validDefault};
      }
    }
    if (defaultSelection == null) throw const VirtualFailure('model-unavailable', 'CodeX did not advertise any available models.');
    return _modelCatalog = {
      'default': defaultSelection,
      'routableProviders': const [_provider],
      'groups': [
        {'id': _provider, 'name': 'CodeX', 'models': views},
      ],
      'failures': const <Object?>[],
    };
  }

  Map<String, Object?>? _findModel(Map<String, Object?> catalog, String model) {
    final groups = catalog['groups'];
    if (groups is! List) return null;
    for (final group in groups) {
      final models = asJsonMap(group)['models'];
      if (models is! List) continue;
      for (final entry in models) {
        if (entry is Map && entry['id'] == model) return entry.cast<String, Object?>();
      }
    }
    return null;
  }

  // ---------------------------------------------------------------------------
  // Endpoints

  @override
  Future<void> respondApproval(String handle, String decision) async {
    await client.respond(handle, switch (decision) {
      'allow' => 'accept',
      'reject' => 'decline',
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
        final turnId = _live[log.sessionId]?.activeTurnId;
        if (turnId != null) await client.interrupt(_thread(log.sessionId), turnId);
        return null;
      case 'session/rename':
        final log = requireLog(requestSessionId(args));
        final title = (asText(request['title']) ?? '').trim();
        if (title.isEmpty) throw const VirtualFailure('gateway/bad-request', 'title is required.');
        await client.rename(_thread(log.sessionId), title);
        log.setTitle(title);
        return {'title': log.title};
      case 'session/modelCatalog':
        return _catalog();
      case 'session/selectModel':
        final log = requireLog(requestSessionId(args));
        final model = asText(request['model']);
        if (model == null || model.isEmpty) throw const VirtualFailure('gateway/bad-request', 'model is required.');
        final view = _findModel(await _catalog(), model);
        if (view == null) throw const VirtualFailure('model-unavailable', 'The selected CodeX model is unavailable on this Host.');
        final effort = asText(request['reasoningEffort']);
        final efforts = asJsonMap(view['reasoning'])['efforts'];
        if (effort != null && !(efforts is List && efforts.any((e) => e is Map && e['id'] == effort))) {
          throw const VirtualFailure('model-unavailable', 'The selected reasoning effort is unavailable for this CodeX model.');
        }
        // Applied on the next `turn/start`, like the plugin.
        final selected = {'provider': _provider, 'model': model, 'reasoningEffort': ?effort};
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
      throw const VirtualFailure('session/workspace-not-found', 'The CodeX workspace was not found.');
    }
    final reuse = logs[asText(request['sessionId'])];
    if (reuse != null && reuse.cwd == cwd && reuse.blank && !reuse.running) return {'sessionId': reuse.sessionId};
    final result = asJsonMap(await client.start(cwd, permissionPreset: CodexPermissionPreset.workspaceWrite));
    final threadId = asText(asJsonMap(result['thread'])['id']) ?? asText(result['threadId']);
    if (threadId == null || threadId.isEmpty) throw const FormatException('CodeX thread/start did not return a thread id.');
    final sessionId = '$_prefix$threadId';
    final log = registerLog(VirtualSessionLog(sessionId, cwd: cwd))
      ..blank = true
      ..hydrated = true;
    final workspace = workspaceId != null && workspaces.containsKey(workspaceId)
        ? workspaces[workspaceId]!
        : upsertWorkspace(cwd);
    attachToWorkspace(log, workspace);
    emitSessionAdded(log);
    return {'sessionId': sessionId};
  }

  Future<Object?> _prompt(Map<String, Object?> request) async {
    final log = requireLog(asText(request['sessionId']));
    final content = request['content'];
    final texts = <String>[];
    for (final part in content is List ? content : const <Object?>[]) {
      final map = asJsonMap(part);
      if (map['type'] != 'text') {
        throw const VirtualFailure('session/attachment-error', 'This client sends text prompts to CodeX only.');
      }
      final text = asText(map['text']);
      if (text != null && text.isNotEmpty) texts.add(text);
    }
    final text = texts.join('\n');
    if (text.trim().isEmpty) throw const VirtualFailure('session/attachment-error', 'A prompt is required.');
    final threadId = _thread(log.sessionId);
    await attachLive(log);
    final live = _live[log.sessionId]!;
    final steer = request['mode'] == 'steer';
    if (steer) {
      final active = live.activeTurnId;
      if (active == null) throw const VirtualFailure('steer-unavailable', 'The CodeX thread has no active turn to steer.');
      await client.steer(threadId, active, text);
      return {'accepted': true};
    }
    if (log.running) throw const VirtualFailure('PROMPT_IN_PROGRESS', 'CodeX is still replying.');
    var selection = _selection(log);
    selection ??= asJsonMap((await _catalog())['default']);
    final permission = _permission(log);
    if (!log.blank) {
      await client.resume(threadId, model: asText(selection['model']), permissionPreset: permission);
    }
    log.pushUserText(text, requestId: asText(request['requestId']));
    live.localPrompt = true;
    if (log.title == null) log.setTitle(_titleFrom(text));
    emitStatus(log, true);
    _touchWorkspace(log);
    try {
      await client.prompt(
        threadId,
        text,
        model: asText(selection['model']),
        effort: asText(selection['reasoningEffort']),
        permissionPreset: permission,
      );
    } on Object catch (error) {
      final failure = failureOf(error);
      live.localPrompt = false;
      log.endTurn({
        'kind': 'error',
        'error': {'code': failure.code, 'message': failure.message},
      }, provider: _provider);
      emitStatus(log, false);
      rethrow;
    }
    return {'accepted': true};
  }
}

Map<String, Object?> _turnEndReason(Map<String, Object?> turn) {
  final status = asText(turn['status']);
  final error = turn['error'];
  if (status == 'failed' || error is Map) {
    final map = asJsonMap(error);
    return {
      'kind': 'error',
      'error': {
        'code': asText(map['code']) ?? 'CODEX_TURN_FAILED',
        'message': asText(map['message']) ?? 'CodeX failed to answer.',
      },
    };
  }
  if (const {'interrupted', 'cancelled', 'canceled', 'aborted'}.contains(status)) return {'kind': 'interrupted'};
  return {'kind': 'completed'};
}

bool _isTool(String? type) => const {
  'commandExecution',
  'mcpToolCall',
  'dynamicToolCall',
  'fileChange',
  'functionCallOutput',
  'hookPrompt',
  'collabAgentToolCall',
  'subAgentActivity',
  'webSearch',
  'imageView',
  'imageGeneration',
  'sleep',
  'enteredReviewMode',
  'exitedReviewMode',
  'contextCompaction',
}.contains(type);

String _toolName(String type) => switch (type) {
  'fileChange' => 'codex.fileChange',
  'commandExecution' => 'codex.command',
  'webSearch' => 'codex.webSearch',
  'collabAgentToolCall' || 'subAgentActivity' => 'codex.subagent',
  'imageView' || 'imageGeneration' => 'codex.image',
  'enteredReviewMode' || 'exitedReviewMode' => 'codex.reviewMode',
  'contextCompaction' => 'codex.compaction',
  _ => 'codex.$type',
};

Map<String, Object?> _toolArguments(Map<String, Object?> item) => switch (item['type']) {
  'commandExecution' => {'command': _commandText(item['command']) ?? ''},
  'fileChange' => {'changes': _fileChanges(item)},
  'webSearch' => {'query': asText(item['query']) ?? ''},
  'imageView' => {'path': asText(item['path']) ?? ''},
  'sleep' => {'durationMs': item['durationMs'] is int ? item['durationMs'] : 0},
  'contextCompaction' => const {},
  _ => {'name': item['name'] ?? item['tool'] ?? item['type'] ?? 'tool', 'arguments': item['arguments'] ?? item['input'] ?? const {}},
};

List<Map<String, Object?>> _fileChanges(Map<String, Object?> item) {
  final changes = item['changes'];
  return [
    for (final change in changes is List ? changes : const <Object?>[])
      if (change is Map && change['path'] is String)
        {
          'path': change['path'],
          'kind': ?(change['kind'] is String ? change['kind'] as String : asText(asJsonMap(change['kind'])['type'])),
        },
  ];
}

String _toolResultText(Map<String, Object?> item, String type, String? liveOutput) {
  switch (type) {
    case 'fileChange':
      final changes = _fileChanges(item).map((c) => [c['kind'], c['path']].whereType<String>().join(' ')).toList();
      return changes.isEmpty ? 'File changes completed.' : changes.join('\n');
    case 'webSearch':
      final results = item['results'];
      final count = results is List ? results.length : 0;
      return count == 0 ? 'Web search completed.' : 'Web search returned $count result${count == 1 ? '' : 's'}.';
    case 'contextCompaction':
      return 'CodeX compacted the conversation context.';
  }
  final text = _itemText(item);
  if (text != null && text.isNotEmpty) return text;
  if (liveOutput != null && liveOutput.isNotEmpty) return liveOutput;
  final result = item['result'] ?? item['output'];
  if (result is Map) return _itemText(result.cast<String, Object?>()) ?? jsonEncode(result);
  return asText(item['status']) ?? type;
}

String? _itemText(Map<String, Object?> value) {
  if (value['text'] is String) return value['text'] as String;
  if (value['content'] is String) return value['content'] as String;
  final content = value['content'] is List ? value['content'] as List : (value['input'] is List ? value['input'] as List : null);
  if (content != null) {
    final text = content
        .map((part) => part is String ? part : (asText(asJsonMap(part)['text']) ?? asText(asJsonMap(part)['content']) ?? ''))
        .where((s) => s.isNotEmpty)
        .join('\n');
    if (text.isNotEmpty) return text;
  }
  if (value['aggregatedOutput'] is String) return value['aggregatedOutput'] as String;
  if (value['output'] is String) return value['output'] as String;
  return null;
}

String? _commandText(Object? value) {
  if (value is String) return value;
  if (value is List) return value.whereType<String>().join(' ');
  final map = asJsonMap(value);
  return asText(map['command']) ?? asText(map['text']);
}

String _effortName(String effort) => switch (effort) {
  'none' => 'None',
  'minimal' => 'Minimal',
  'low' => 'Low',
  'medium' => 'Medium',
  'high' => 'High',
  'xhigh' => 'Extra High',
  'max' => 'Max',
  'ultra' => 'Ultra',
  _ => effort,
};

/// App Server times are epoch seconds; DSH uses epoch milliseconds.
int? _epochMs(Object? value) {
  if (value is num) return value < 100000000000 ? (value * 1000).round() : value.round();
  if (value is String) return DateTime.tryParse(value)?.millisecondsSinceEpoch;
  return null;
}

bool _contains(String root, String candidate) {
  String norm(String p) => p.replaceAll('\\', '/').replaceAll(RegExp(r'/+$'), '').toLowerCase();
  final r = norm(root);
  final c = norm(candidate);
  return c == r || c.startsWith('$r/');
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

String _basename(String path) {
  final parts = path.split(RegExp(r'[\\/]')).where((p) => p.isNotEmpty).toList();
  return parts.isEmpty ? path : parts.last;
}
