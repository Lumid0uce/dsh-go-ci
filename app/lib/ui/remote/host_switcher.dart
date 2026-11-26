import 'dart:async';

import 'package:dsh_remote/dsh_remote.dart' show HostBackend, HostBackendAvailability;
import 'package:flutter/material.dart';
import 'package:flutter_riverpod/flutter_riverpod.dart';

import '../../l10n/l10n.dart';
import '../../runtime/local_footprint.dart';
import '../../runtime/runtime_state.dart';
import '../../state/connection_state.dart';
import '../../state/hosts_state.dart';
import '../common/app_sheet.dart';
import '../common/sheet_title_bar.dart';
import '../common/status_chip.dart';
import '../companion/companion_page.dart';
import '../onboarding/local_install.dart';
import '../settings/settings_widgets.dart';
import '../theme/app_icons.dart';
import '../theme/theme.dart';
import 'host_key_dialog.dart';
import 'host_status.dart';

/// Drawer-header chip: `● 本机 · 运行中 ⌄` / `● 电脑 · 已连接 ⌄`. Opens the
/// host sheet.
class HostChip extends ConsumerWidget {
  const HostChip({super.key, required this.onTap});

  final VoidCallback onTap;

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final l10n = context.l10n;
    final theme = Theme.of(context);
    final scheme = theme.colorScheme;
    final host = ref.watch(activeHostProvider);
    final backend = ref.watch(hostBackendProvider(host));
    final baseName = ref.watch(hostNameProvider(host)) ?? (host.isLocal ? l10n.commonThisPhone : l10n.commonRemoteHost);
    final name = backend == HostBackend.dsh ? baseName : '$baseName · ${backend.label}';
    final view = watchHostStatus(ref, host);
    return Semantics(
      button: true,
      onTap: onTap,
      label: l10n.remoteHostSwitcherSemantics(name, view.label),
      child: ExcludeSemantics(
        child: GlassSurface(
          tier: GlassTier.pill,
          stadium: true,
          onTap: onTap,
          padding: const EdgeInsets.fromLTRB(10, 6, 8, 6),
          child: Row(
            mainAxisSize: MainAxisSize.min,
            children: [
              AnimatedStatusDot(tone: view.tone, size: 7),
              const SizedBox(width: 7),
              Flexible(
                child: Text(
                  '$name · ${view.label}',
                  maxLines: 1,
                  overflow: TextOverflow.ellipsis,
                  style: AppType.tabular(theme.textTheme.labelMedium!.copyWith(color: scheme.onSurfaceVariant)),
                ),
              ),
              const SizedBox(width: 2),
              Icon(AppIcons.dropdown, size: 16, color: DshColors.of(context).textTertiary),
            ],
          ),
        ),
      ),
    );
  }
}

/// "切换主机": 本机, saved SSH hosts and the DSH Remote account's hosts, each
/// with its state; picking one makes it active. [onManage] opens the remote
/// page.
Future<void> showHostSheet(BuildContext context, {required VoidCallback onManage}) {
  return showAppSheet<void>(context, isScrollControlled: true, builder: (sheet) => HostSheet(onManage: onManage));
}

class HostSheet extends ConsumerWidget {
  const HostSheet({super.key, required this.onManage});

  final VoidCallback onManage;

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final l10n = context.l10n;
    final active = ref.watch(activeHostProvider);
    final ssh = ref.watch(hostsProvider.select((s) => s.ssh));
    final account = ref.watch(relayAccountProvider).value;
    final relayHosts = account?.signedIn == true ? account!.hosts : const <RelayHost>[];

    Future<void> pick(HostRef host) async {
      if (host == active) {
        Navigator.of(context).pop();
        return;
      }
      final navigator = Navigator.of(context);
      final ok = await activateHost(context, ref, host);
      if (ok && navigator.mounted) navigator.pop();
    }

    // 本机 without its runtime: the row installs it, and switches to it once
    // installed (leaving the install page keeps the current host). A build
    // without the runtime has no 本机 row at all.
    final localMissing = ref.watch(localRuntimeProvider.select((s) => s.status.runtimeMissing));
    final localAvailable = ref.watch(localRuntimeProvider.select((s) => s.available));
    void installLocal() {
      final navigator = Navigator.of(context);
      navigator.pop();
      unawaited(navigator.push(MaterialPageRoute<void>(builder: (_) => const LocalInstallPage(useWhenDone: true))));
    }

    return SafeArea(
      top: false,
      child: ConstrainedBox(
        constraints: BoxConstraints(maxHeight: MediaQuery.sizeOf(context).height * 0.8),
        child: SingleChildScrollView(
          child: Column(
            mainAxisSize: MainAxisSize.min,
            crossAxisAlignment: CrossAxisAlignment.stretch,
            children: [
              SheetTitleBar(l10n.remoteSwitchHost),
              SheetGroup(
                margin: const EdgeInsets.fromLTRB(SheetTitleBar.inset, 0, SheetTitleBar.inset, 12),
                children: [
                  if (localAvailable)
                    _HostRow(
                      host: HostRef.local,
                      title: l10n.commonThisPhone,
                      subtitle: l10n.remoteThisPhoneDsh,
                      active: active.isLocal,
                      onTap: localMissing ? installLocal : () => pick(HostRef.local),
                    ),
                  for (final h in ssh)
                    _HostRow(
                      host: HostRef.ssh(h.id),
                      title: h.name,
                      subtitle: 'SSH · ${h.address}',
                      active: active == HostRef.ssh(h.id),
                      onTap: () => pick(HostRef.ssh(h.id)),
                    ),
                  for (final h in relayHosts)
                    _RelayHostEntry(
                      host: HostRef.relay(h.id, name: h.name),
                      relay: h,
                      subtitle: h.platform == null
                          ? l10n.remoteRemoteLabel
                          : l10n.remoteRemotePlatform(RelayHost.platformLabel(h.platform!)),
                      active: active == HostRef.relay(h.id),
                      onPick: (backend) async {
                        final host = HostRef.relay(h.id, name: h.name);
                        final navigator = Navigator.of(context);
                        final ok = host == active || await activateHost(context, ref, host);
                        if (!ok) return;
                        ref.read(hostBackendsProvider.notifier).select(host, backend);
                        if (navigator.mounted) navigator.pop();
                      },
                    ),
                ],
              ),
              if (relayHosts.length > 1)
                Padding(
                  padding: const EdgeInsets.fromLTRB(SheetTitleBar.inset + 8, 0, SheetTitleBar.inset + 8, 12),
                  child: Text(
                    l10n.remoteRelaySwitchWarning,
                    style: Theme.of(context).textTheme.bodySmall?.copyWith(color: DshColors.of(context).textTertiary),
                  ),
                ),
              SheetGroup(
                margin: const EdgeInsets.fromLTRB(SheetTitleBar.inset, 0, SheetTitleBar.inset, 12),
                children: [
                  SettingsTile(
                    icon: AppIcons.pet,
                    tinted: false,
                    title: l10n.companionTitle,
                    subtitle: l10n.companionEntrySubtitle,
                    onTap: () {
                      final navigator = Navigator.of(context);
                      navigator.pop();
                      unawaited(navigator.push(MaterialPageRoute<void>(builder: (_) => const CompanionPage(sections: true))));
                    },
                  ),
                  SettingsTile(
                    icon: ssh.isEmpty && relayHosts.isEmpty ? AppIcons.add : AppIcons.configure,
                    tinted: false,
                    title: ssh.isEmpty && relayHosts.isEmpty ? l10n.remoteConnectComputer : l10n.remoteManageHosts,
                    subtitle: ssh.isEmpty && relayHosts.isEmpty ? l10n.remoteConnectionMethods : null,
                    onTap: () {
                      Navigator.of(context).pop();
                      onManage();
                    },
                  ),
                ],
              ),
              const SizedBox(height: 4),
            ],
          ),
        ),
      ),
    );
  }
}

/// A relay computer: tapping it unfolds a second level to pick which agent
/// runs there (DSH / Claude / Codex). Unfolded from the start when it is the
/// active host. Agents the connected computer does not offer are greyed out;
/// before a connection nothing is known, so all three stay selectable.
class _RelayHostEntry extends ConsumerStatefulWidget {
  const _RelayHostEntry({
    required this.host,
    required this.relay,
    required this.subtitle,
    required this.active,
    required this.onPick,
  });

  final HostRef host;
  final RelayHost relay;
  final String subtitle;
  final bool active;
  final Future<void> Function(HostBackend backend) onPick;

  @override
  ConsumerState<_RelayHostEntry> createState() => _RelayHostEntryState();
}

class _RelayHostEntryState extends ConsumerState<_RelayHostEntry> {
  late bool _expanded = widget.active;
  bool _busy = false;

  Future<void> _pick(HostBackend backend) async {
    if (_busy) return;
    setState(() => _busy = true);
    try {
      await widget.onPick(backend);
    } finally {
      if (mounted) setState(() => _busy = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final scheme = Theme.of(context).colorScheme;
    final current = ref.watch(hostBackendProvider(widget.host));
    final availability = widget.active
        ? ref.watch(hostBackendAvailabilityProvider(widget.host)).value ?? HostBackendAvailability.unknown
        : HostBackendAvailability.unknown;
    final known = availability != HostBackendAvailability.unknown;
    final view = widget.active ? watchHostStatus(ref, widget.host) : HostStatusView.ofRelayHost(widget.relay);
    return Column(
      mainAxisSize: MainAxisSize.min,
      crossAxisAlignment: CrossAxisAlignment.stretch,
      children: [
        Semantics(
          key: ValueKey('host-row-${widget.host.kind.name}-${widget.host.id}'),
          selected: widget.active,
          expanded: _expanded,
          child: SettingsTile(
            icon: hostKindIcon(widget.host.kind),
            tinted: false,
            title: widget.relay.name,
            subtitle: widget.active && current != HostBackend.dsh
                ? '${widget.subtitle} · ${current.label}'
                : widget.subtitle,
            onTap: () => setState(() => _expanded = !_expanded),
            value: Row(
              mainAxisSize: MainAxisSize.min,
              children: [
                AnimatedStatusDot(tone: view.tone, size: 7),
                const SizedBox(width: 6),
                Text(view.label),
              ],
            ),
            trailing: AnimatedRotation(
              turns: _expanded ? 0.5 : 0,
              duration: const Duration(milliseconds: 180),
              child: Icon(AppIcons.dropdown, size: 20, color: DshColors.of(context).textTertiary),
            ),
          ),
        ),
        AnimatedSize(
          duration: const Duration(milliseconds: 180),
          curve: Curves.easeOutCubic,
          alignment: Alignment.topCenter,
          child: !_expanded
              ? const SizedBox(width: double.infinity)
              : Padding(
                  padding: const EdgeInsets.only(left: 36),
                  child: Column(
                    mainAxisSize: MainAxisSize.min,
                    crossAxisAlignment: CrossAxisAlignment.stretch,
                    children: [
                      for (final backend in HostBackend.values)
                        Builder(
                          builder: (context) {
                            final enabled = !_busy && (!known || availability.isAvailable(backend));
                            final selected = widget.active && current == backend;
                            return Semantics(
                              key: ValueKey('host-backend-${widget.host.id}-${backend.id}'),
                              selected: selected,
                              inMutuallyExclusiveGroup: true,
                              enabled: enabled,
                              child: Opacity(
                                opacity: enabled || selected ? 1 : 0.4,
                                child: SettingsTile(
                                  title: backend.label,
                                  onTap: enabled ? () => _pick(backend) : null,
                                  chevron: false,
                                  trailing: selected
                                      ? Icon(AppIcons.selected, size: 20, color: scheme.primary)
                                      : const SizedBox(width: 20),
                                ),
                              ),
                            );
                          },
                        ),
                    ],
                  ),
                ),
        ),
      ],
    );
  }
}

class _HostRow extends ConsumerWidget {
  const _HostRow({
    required this.host,
    required this.title,
    required this.subtitle,
    required this.active,
    required this.onTap,
  });

  final HostRef host;
  final String title;
  final String subtitle;
  final bool active;
  final VoidCallback onTap;

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final scheme = Theme.of(context).colorScheme;
    // Live state for 本机 and the active remote host; nothing for idle SSH
    // hosts (connecting just to show a dot would cost a login).
    final HostStatusView? view = host.isLocal || active
        ? watchHostStatus(ref, host)
        : null;
    return Semantics(
      key: ValueKey('host-row-${host.kind.name}-${host.id}'),
      selected: active,
      inMutuallyExclusiveGroup: true,
      child: SettingsTile(
        icon: hostKindIcon(host.kind),
        tinted: false,
        title: title,
        subtitle: subtitle,
        onTap: onTap,
        value: view == null
            ? null
            : Row(
                mainAxisSize: MainAxisSize.min,
                children: [
                  AnimatedStatusDot(tone: view.tone, size: 7),
                  const SizedBox(width: 6),
                  Text(view.label),
                ],
              ),
        trailing: active ? Icon(AppIcons.selected, size: 20, color: scheme.primary) : const SizedBox(width: 20),
      ),
    );
  }
}
