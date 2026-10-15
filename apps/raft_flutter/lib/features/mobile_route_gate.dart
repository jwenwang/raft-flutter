import 'package:flutter/widgets.dart';

import '../data/raft_location.dart';
import '../data/workspace_controller.dart';

/// Live cover flag for one phone route. Set in the route's listener, which
/// runs before the page's listeners, so a covered chat does not apply the
/// workspace's new channel during that same notification.
class MobileRouteGate extends InheritedWidget {
  const MobileRouteGate({super.key, required super.child});

  static final _covered = Expando<bool>();
  static final _location = Expando<RaftLocation>();

  @override
  bool updateShouldNotify(MobileRouteGate oldWidget) => false;

  /// The route [State] is the parent of this gate. Element.parent is private
  /// in this Flutter version, so the walk starts at that parent.
  static State? _state(BuildContext context) {
    final element = context
        .getElementForInheritedWidgetOfExactType<MobileRouteGate>();
    if (element == null) return null;
    State? route;
    element.visitAncestorElements((ancestor) {
      if (ancestor is StatefulElement) {
        route = ancestor.state;
        return false;
      }
      return true;
    });
    return route;
  }

  static void write(
    State state, {
    required bool covered,
    required RaftLocation location,
  }) {
    _covered[state] = covered;
    _location[state] = location;
  }

  /// The location stored for the phone route that contains [context].
  static RaftLocation? locationOf(BuildContext context) {
    final state = _state(context);
    return state == null ? null : _location[state];
  }

  /// Covered, or a channel or DM page whose workspace channel is still the
  /// previous one.
  static bool holds(BuildContext context, WorkspaceController workspace) {
    final state = _state(context);
    if (state == null) return false;
    if (_covered[state] == true) return true;
    final location = _location[state];
    if (location == null) return false;
    if (location.route != RaftRoute.channel && location.route != RaftRoute.dm) {
      return false;
    }
    return workspace.channel?.id != location.entityId;
  }
}
