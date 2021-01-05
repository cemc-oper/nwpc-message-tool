from nwpc_message_tool.analytics.task_status_change_dfa import (
    TaskStatusChangeDFA, StatusChangeType
)
from nwpc_message_tool.analytics.situation_type import TaskSituationType


def test_node_situation_dfa():
    dfa = TaskStatusChangeDFA(name="test")
    dfa.trigger(StatusChangeType.Submit.value)
    assert dfa.state is TaskSituationType.Submit
    assert dfa.node_situation.situation is TaskSituationType.Submit

    dfa.trigger(StatusChangeType.Initial.value)
    assert dfa.state is TaskSituationType.Active
    assert dfa.node_situation.situation is TaskSituationType.Active

    dfa.trigger(StatusChangeType.Complete.value)
    assert dfa.state is TaskSituationType.Complete
    assert dfa.node_situation.situation is TaskSituationType.Complete


def test_node_situation_dfa_initial():
    dfa = TaskStatusChangeDFA(name="test")
    assert dfa.state is TaskSituationType.Initial

    dfa.trigger(StatusChangeType.Complete.value)
    assert dfa.state is TaskSituationType.Unknown

    dfa = TaskStatusChangeDFA(name="test")
    dfa.trigger(StatusChangeType.Submit.value)
    assert dfa.state is TaskSituationType.Submit
