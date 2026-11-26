// SPDX-FileCopyrightText: 2026 DSH Remote contributors
// SPDX-License-Identifier: MIT
// See THIRD_PARTY_NOTICES.md.
// `harness.transport.describe.workspaceTypes` parsing and the backend choices
// (DSH / Claude / Codex) offered under a Remote Host.
library;

/// Protocol bound on `workspaceTypes` entries.
const int maxRemoteWorkspaceTypeEntries = 32;

/// One `workspaceTypes` entry: `{id, name, capability, available}`.
class RemoteWorkspaceType {
  const RemoteWorkspaceType({required this.id, required this.name, required this.capability, required this.available});

  final String id;
  final String name;
  final String capability;
  final bool available;

  @override
  bool operator ==(Object other) =>
      other is RemoteWorkspaceType &&
      other.id == id &&
      other.name == name &&
      other.capability == capability &&
      other.available == available;

  @override
  int get hashCode => Object.hash(id, name, capability, available);
}

/// Malformed entries are dropped; an oversized or non-list value yields [].
/// Returns null when the Host omitted the field (older Host).
List<RemoteWorkspaceType>? parseRemoteWorkspaceTypes(Object? value, {bool present = true}) {
  if (!present) return null;
  if (value is! List || value.length > maxRemoteWorkspaceTypeEntries) return const [];
  return [
    for (final item in value)
      if (item is Map &&
          item['id'] is String &&
          (item['id'] as String).isNotEmpty &&
          item['name'] is String &&
          (item['name'] as String).isNotEmpty &&
          item['capability'] is String &&
          (item['capability'] as String).isNotEmpty &&
          item['available'] is bool)
        RemoteWorkspaceType(
          id: item['id'] as String,
          name: item['name'] as String,
          capability: item['capability'] as String,
          available: item['available'] as bool,
        ),
  ];
}

/// Same rule as the plugin's `remoteWorkspaceTypeAvailable`: the capability
/// must be declared; when `workspaceTypes` exists, exactly one matching entry
/// with that capability must report `available`.
bool remoteWorkspaceTypeAvailable(
  List<String> capabilities,
  List<RemoteWorkspaceType>? workspaceTypes,
  String id,
  String capability,
) {
  if (!capabilities.contains(capability)) return false;
  if (workspaceTypes == null) return true;
  final entries = workspaceTypes.where((t) => t.id == id).toList();
  return entries.length == 1 && entries.single.capability == capability && entries.single.available;
}

/// Which agent runs new sessions on a computer.
enum HostBackend {
  dsh('dsh'),
  claude('claude'),
  codex('codex');

  const HostBackend(this.id);

  /// Persisted id; also the `workspaceTypes` id for Claude / Codex.
  final String id;

  static HostBackend fromId(String? id) =>
      HostBackend.values.firstWhere((b) => b.id == id, orElse: () => HostBackend.dsh);

  /// Product name shown in menus (not translated).
  String get label => switch (this) {
    HostBackend.dsh => 'DSH',
    HostBackend.claude => 'Claude',
    HostBackend.codex => 'Codex',
  };

  /// Capability the Host must declare for this backend.
  String get capability => switch (this) {
    HostBackend.dsh => 'harness.remote.v3',
    HostBackend.claude => 'agent.acp.claude.v1',
    HostBackend.codex => 'codex.appserver.v1',
  };
}

/// Availability of each [HostBackend] on one connected Host.
class HostBackendAvailability {
  const HostBackendAvailability(this._available);

  /// Nothing known yet (not connected): only DSH is offered.
  static const unknown = HostBackendAvailability({HostBackend.dsh});

  factory HostBackendAvailability.of(List<String> capabilities, List<RemoteWorkspaceType>? workspaceTypes) {
    final out = <HostBackend>{};
    if (capabilities.contains('harness.remote.v3')) out.add(HostBackend.dsh);
    if (capabilities.contains('agent.acp.v1') &&
        remoteWorkspaceTypeAvailable(capabilities, workspaceTypes, 'claude', HostBackend.claude.capability)) {
      out.add(HostBackend.claude);
    }
    if (remoteWorkspaceTypeAvailable(capabilities, workspaceTypes, 'codex', HostBackend.codex.capability)) {
      out.add(HostBackend.codex);
    }
    return HostBackendAvailability(out);
  }

  final Set<HostBackend> _available;

  bool isAvailable(HostBackend backend) => _available.contains(backend);

  @override
  bool operator ==(Object other) =>
      other is HostBackendAvailability &&
      other._available.length == _available.length &&
      other._available.containsAll(_available);

  @override
  int get hashCode => Object.hashAllUnordered(_available);
}
