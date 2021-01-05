import typing
import datetime

from loguru import logger
import pandas as pd

from .situation_type import TaskSituationType
from .node_situation import NodeSituation
from .record import StatusChangeEntry

from nwpc_message_tool.message.ecflow_client import EcflowClientMessage


class SituationRecord(object):
    def __init__(
            self,
            date,
            state: TaskSituationType,
            node_situation: NodeSituation,
            records: typing.List[EcflowClientMessage],
    ):
        self.date = date
        self.state = state
        self.node_situation = node_situation
        self.records = records


class SituationCalculator(object):
    """
    计算节点的运行状态 `NodeSituation`

    Attributes
    ----------
    _dfa_engine:
        用于计算节点运行状态的DFA类
    _stop_states: typing.Tuple
        停止计算DFA的运行状态
    _dfa_kwargs: dict
        创建DFA时的附加参数
    """
    def __init__(
            self,
            dfa_engine,
            stop_states: typing.Tuple,
            dfa_kwargs: dict = None,
    ):
        self._dfa_engine = dfa_engine
        self._stop_states = stop_states
        self._dfa_kwargs = dfa_kwargs
        if self._dfa_kwargs is None:
            self._dfa_kwargs = dict()

    def get_situations(
            self,
            records: typing.List[EcflowClientMessage],
            node_path: str,
            start_date: datetime.datetime,
            end_date: datetime.datetime,
    ) -> typing.List[SituationRecord]:
        """
        Get situations for some node in date range [start_date, end_date).

        Parameters
        ----------
        records
        node_path
        start_date
        end_date

        Returns
        -------
        typing.List[SituationRecord]

        """
        logger.info("Finding StatusLogRecord for {}", node_path)
        record_list = []
        for record in records:
            if record.node_name == node_path and record.command in ("submit", "init", "complete", "abort"):
                record_list.append(record)

        logger.info("Calculating node status change using DFA...")
        situations = []
        for current_date in pd.date_range(start=start_date, end=end_date, closed="left"):
            current_records = list(filter(lambda x: x.ecf_date == current_date, record_list))

            status_changes = [StatusChangeEntry(r) for r in current_records]

            dfa = self._dfa_engine(
                name=current_date,
                **self._dfa_kwargs,
            )

            for s in status_changes:
                dfa.trigger(
                    s.status.value,
                    node_data=s,
                )
                if dfa.state in self._stop_states:
                    break

            situations.append(SituationRecord(
                date=current_date,
                state=dfa.state,
                node_situation=dfa.node_situation,
                records=current_records,
            ))

        logger.info("Calculating node status change using DFA...Done")
        return situations
