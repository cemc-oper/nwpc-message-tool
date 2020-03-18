from nwpc_message_tool.message import ProductionEventMessage


def get_hour(message: ProductionEventMessage) -> int:
    return int(message.forecast_time.seconds/3600) + message.forecast_time.days * 24
