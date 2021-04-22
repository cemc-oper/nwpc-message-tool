import pandas as pd

from nwpc_message_tool.message import ProductionEventMessage


def get_engine(engine_name: str):
    if engine_name == "nwpc_message":
        from nwpc_message_tool.source.production import nwpc_message
        engine = nwpc_message
    elif engine_name == "nmc_monitor":
        from nwpc_message_tool.source.production import nmc_monitor
        engine = nmc_monitor
    else:
        raise NotImplemented(f"engine is not supported: {engine_name}")
    return engine


def get_hour(message: ProductionEventMessage) -> int:
    return _get_hour_from_timedelta(message.forecast_time)


def _get_hour_from_timedelta(t: pd.Timedelta) -> int:
    return int(t.seconds / 3600) + t.days * 24
