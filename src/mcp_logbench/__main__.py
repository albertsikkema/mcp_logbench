"""Entry point: python -m mcp_logbench"""

from mcp_logbench.config import load_config
from mcp_logbench.logging import setup_logging
from mcp_logbench.server import create_server

config = load_config()
setup_logging()
server = create_server(config)
server.run(transport="http", host=config.server.host, port=config.server.port)
