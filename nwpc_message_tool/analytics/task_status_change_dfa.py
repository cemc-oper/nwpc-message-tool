from transitions import Machine

from .status_change_type import StatusChangeType

from .node_situation import (
    NodeSituation,
    TimePoint,
    TimePeriodType,
    TimePeriod,
)
from .situation_type import TaskSituationType
from .node_status_change_data import NodeStatusChangeData


class TaskStatusChangeDFA(object):
    """
    According to the NodeStatusChangeData data, analyze the running situation of the Task node.
    Implemented based on Deterministic Finite Automaton (DFA).
    """
    def __init__(self, name):
        self.name = name
        self.node_situation = NodeSituation()
        self.machine = Machine(
            model=self,
            states=TaskSituationType,
            initial=TaskSituationType.Initial,
            after_state_change=self.change_node_situation_type,
        )

        self._current_cycle = {
            StatusChangeType.Submit: None,
            StatusChangeType.Initial: None,
            StatusChangeType.Complete: None,
            StatusChangeType.Abort: None,
        }

        self._initial_transitions()

    def add_node_data(self, node_data: NodeStatusChangeData = None):
        if node_data is None:
            return
        self.node_situation.time_points.append(
            TimePoint(
                status=node_data.status,
                time=node_data.date_time,
            )
        )

    def enter_new_cycle(self, node_data: NodeStatusChangeData = None):
        if node_data is None:
            return
        self._current_cycle = {
            StatusChangeType.Submit: None,
            StatusChangeType.Initial: None,
            StatusChangeType.Complete: None,
            StatusChangeType.Abort: None,
        }

    def set_cycle_time_point(self, node_data: NodeStatusChangeData = None):
        if node_data is None:
            return
        self._current_cycle[node_data.status] = node_data.date_time

    def calculate_time_period(self, **kwargs):
        in_active = TimePeriod(
            period_type=TimePeriodType.InActive,
            start_time=self._current_cycle[StatusChangeType.Initial],
            end_time=self._current_cycle[StatusChangeType.Complete],
        )
        submitted_time = self._current_cycle[StatusChangeType.Submit]
        if submitted_time is None:
            in_all = TimePeriod(
                period_type=TimePeriodType.InAll,
                start_time=self._current_cycle[StatusChangeType.Initial],
                end_time=self._current_cycle[StatusChangeType.Complete],
            )
            self.node_situation.time_periods.extend([
                in_all,
                in_active,
            ])
        else:
            in_all = TimePeriod(
                period_type=TimePeriodType.InAll,
                start_time=self._current_cycle[StatusChangeType.Submit],
                end_time=self._current_cycle[StatusChangeType.Complete]
            )
            in_submitted = TimePeriod(
                period_type=TimePeriodType.InSubmitted,
                start_time=self._current_cycle[StatusChangeType.Submit],
                end_time=self._current_cycle[StatusChangeType.Initial]
            )
            self.node_situation.time_periods.extend([
                in_all,
                in_submitted,
                in_active,
            ])

    def change_node_situation_type(self, **kwargs):
        self.node_situation.situation = self.state

    def _initial_transitions(self):
        self._initial_transitions_for_init()
        self._initial_transitions_for_submit()
        self._initial_transitions_for_active()
        self._initial_transitions_for_unknown()

    def _initial_transitions_for_init(self):
        """Find NodeStatus.queued for Initial state and ignore others.
        """
        source = TaskSituationType.Initial


        # submitted enters Submit directly
        self.machine.add_transition(
            trigger=StatusChangeType.Submit.value,
            source=source,
            dest=TaskSituationType.Submit,
            before=self.add_node_data,
            after=[self.enter_new_cycle, self.set_cycle_time_point],
        )

        # all else is ignore.
        for t in (e.value for e in [
            StatusChangeType.Initial,
            StatusChangeType.Complete,
            StatusChangeType.Abort,
        ]):
            self.machine.add_transition(
                trigger=t,
                source=source,
                dest=TaskSituationType.Unknown,
            )


    def _initial_transitions_for_submit(self):
        source = TaskSituationType.Submit

        self.machine.add_transition(
            trigger=StatusChangeType.Initial.value,
            source=source,
            dest=TaskSituationType.Active,
            before=self.add_node_data,
            after=self.set_cycle_time_point,
        )

        self.machine.add_transition(
            trigger=StatusChangeType.Abort.value,
            source=source,
            dest=TaskSituationType.Error,
            before=self.add_node_data,
            after=self.set_cycle_time_point,
        )

        for s in (StatusChangeType.Complete, StatusChangeType.Submit):
            self.machine.add_transition(
                trigger=s.value,
                source=source,
                dest=TaskSituationType.Unknown,
            )

    def _initial_transitions_for_active(self):
        source = TaskSituationType.Active

        self.machine.add_transition(
            trigger=StatusChangeType.Complete.value,
            source=source,
            dest=TaskSituationType.Complete,
            before=self.add_node_data,
            after=[
                self.set_cycle_time_point,
                self.calculate_time_period,
            ],
        )

        self.machine.add_transition(
            trigger=StatusChangeType.Abort.value,
            source=source,
            dest=TaskSituationType.Error,
            before=self.add_node_data,
            after=self.set_cycle_time_point,
        )

        for s in (StatusChangeType.Submit, StatusChangeType.Initial):
            self.machine.add_transition(
                trigger=s.value,
                source=source,
                dest=TaskSituationType.Unknown,
            )

    def _initial_transitions_for_unknown(self):
        source = TaskSituationType.Unknown
        for t in (e.name for e in [
            StatusChangeType.Submit,
            StatusChangeType.Initial,
            StatusChangeType.Complete,
            StatusChangeType.Abort,
        ]):
            self.machine.add_transition(
                trigger=t,
                source=source,
                dest=source,
            )
