import os

from nwpc_message_tool.server.app import create_app


config_file_path = os.environ["NWPC_MESSAGE_TOOL_SERVER_CONFIG"]

app = create_app(config_file_path)