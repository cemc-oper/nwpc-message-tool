from nwpc_message_tool.source.production import (
    nwpc_message,
    nmc_monitor
)
from nwpc_message_tool.message import ProductionEventMessage


def get_hour(message: ProductionEventMessage) -> int:
    return int(message.forecast_time.seconds/3600) + message.forecast_time.days * 24


def get_engine(engine_name: str):
    if engine_name == "nwpc_message":
        engine = nwpc_message
    elif engine_name == "nmc_monitor":
        engine = nmc_monitor
    else:
        raise NotImplemented(f"engine is not supported: {engine_name}")
    return engine
