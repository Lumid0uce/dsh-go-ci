// Invariants the app's session follow relies on (see `_SessionWatch`):
// event seq is contiguous from the snapshot cursor, assistant-stream
// revisions increase by exactly one from the snapshot's revision.
import 'package:dsh_remote/src/agents/virtual_link.dart';
import 'package:test/test.dart';

void main() {
  group('VirtualSessionLog', () {
    test('turn projection keeps seq contiguous and commits the stream', () {
      final log = VirtualSessionLog('claude:s1', cwd: '/w');
      log.pushUserText('hi', requestId: 'r1');
      log.appendDelta('reasoning', 'think');
      log.appendDelta('text', 'hel');
      log.appendDelta('text', 'lo');
      log.finishAssistant(provider: 'claude');
      log.pushToolCall('c1', 'Read', {'path': 'a'});
      log.pushToolResult('c1', 'ok');
      log.appendDelta('text', 'done');
      log.endTurn({'kind': 'completed'}, provider: 'claude');

      for (var i = 0; i < log.events.length; i++) {
        expect(log.events[i]['seq'], i);
      }
      final types = [for (final e in log.events) e['type']];
      expect(types, [
        'turn/start',
        'step/start',
        'user/message',
        'assistant/message',
        'tool/call',
        'tool/result',
        'assistant/message',
        'step/end',
        'turn/end',
      ]);
      final first = log.events[3]['data'] as Map;
      final content = (first['message'] as Map)['content'] as List;
      expect(content, [
        {'type': 'reasoning', 'text': 'think'},
        {'type': 'text', 'text': 'hello'},
      ]);
      expect(log.stepOpen, isFalse);
      expect(log.cursor, log.events.length - 1);
    });

    test('snapshot carries the active attempt and revision', () {
      final log = VirtualSessionLog('codex:t1');
      log.pushUserText('q');
      log.appendDelta('text', 'a');
      final snapshot = log.snapshot();
      expect(snapshot['cursor'], log.cursor);
      final stream = snapshot['assistantStream'] as Map;
      expect(stream['revision'], log.revision);
      // start + block-start + text-delta
      expect(log.revision, 3);
      final attempt = stream['activeAttempt'] as Map;
      expect(attempt['nextIndex'], 2);
      expect((attempt['stream'] as List).length, 2);
    });

    test('resetHistory renumbers seq and tracks the last turn', () {
      final log = VirtualSessionLog('claude:s2');
      log.resetHistory([
        {
          'type': 'event',
          'event': {'type': 'turn/start', 'seq': 7, 'time': 0, 'data': {'turn': 3}},
        },
        {'type': 'user/message', 'seq': 9, 'time': 0, 'data': <String, Object?>{}},
      ]);
      expect([for (final e in log.events) e['seq']], [0, 1]);
      expect(log.turn, 3);
      expect(log.blank, isFalse);
      log.pushUserText('next');
      expect(log.events[2]['data'], {'turn': 4});
    });

    test('page returns older events before beforeSeq', () {
      final log = VirtualSessionLog('claude:s3');
      for (var i = 0; i < 3; i++) {
        log.pushUserText('m$i');
        log.endTurn({'kind': 'completed'});
      }
      final page = log.page(throughSeq: log.cursor, beforeSeq: 5, maxMessages: 1);
      final records = page['records'] as List;
      expect((records.first as Map)['event']['type'], 'turn/start');
      expect((records.last as Map)['event']['seq'], 4);
      expect(page['hasMore'], isFalse);
    });
  });
}
