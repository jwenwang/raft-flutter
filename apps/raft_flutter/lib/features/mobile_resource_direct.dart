import 'package:flutter/widgets.dart';

import '../data/workspace_controller.dart';

/// Principal, page, and every value that changes how the list is built.
/// Messages and presence are not inputs.
Object mobileResourceInputs(
  WorkspaceController controller,
  String section, {
  required bool wide,
  required String? rootTab,
  required bool activityMasterDetail,
}) =>
    (
      controller.client.generation,
      controller.client.user?.id,
      controller.server?.id,
      controller.server?.string('role'),
      section,
      wide,
      rootTab,
      activityMasterDetail,
    );

/// Phone tasks and saved keep one list for the active page, and rebuild it
/// when [inputs] change. A covered route drops that list.
Widget mobileResourceBody({
  required String route,
  required WorkspaceController page,
  required WorkspaceController? outer,
  required bool direct,
  required bool Function() active,
  required bool wide,
  required String? rootTab,
  required bool activityMasterDetail,
  required Widget Function(WorkspaceController owner) buildList,
}) {
  final useOuter = direct && (route == 'tasks' || route == 'saved');
  if (!useOuter || outer == null) return buildList(page);
  return MobileResourceDirectPage(
    inputs: mobileResourceInputs(
      outer,
      route,
      wide: wide,
      rootTab: rootTab,
      activityMasterDetail: activityMasterDetail,
    ),
    active: active,
    buildList: () => buildList(outer),
  );
}

/// The active phone list. A covered route returns nothing, so it does not
/// keep a second list on the outer workspace.
class MobileResourceDirectPage extends StatefulWidget {
  const MobileResourceDirectPage({
    super.key,
    required this.inputs,
    required this.active,
    required this.buildList,
  });

  final Object? inputs;
  final bool Function() active;
  final Widget Function() buildList;

  @override
  State<MobileResourceDirectPage> createState() =>
      _MobileResourceDirectPageState();
}

class _MobileResourceDirectPageState extends State<MobileResourceDirectPage> {
  Widget? _leaf;
  Object? _inputs;

  @override
  Widget build(BuildContext context) {
    TickerMode.valuesOf(context);
    if (!widget.active()) {
      _leaf = null;
      _inputs = null;
      return const SizedBox.shrink();
    }
    if (_leaf != null && widget.inputs == _inputs) return _leaf!;
    _inputs = widget.inputs;
    _leaf = widget.buildList();
    return _leaf!;
  }
}
