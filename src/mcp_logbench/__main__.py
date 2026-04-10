"""Entry point: python -m mcp_logbench"""

from mcp_logbench.config import load_config
from mcp_logbench.logging import setup_logging
from mcp_logbench.server import create_server

if __name__ == "__main__":
    from loguru import logger

    config = load_config()
    setup_logging()

    logger.info("Starting MCP LogBench", host=config.server.host, port=config.server.port)

    if not config.auth.tenant_id or not config.auth.client_id:
        logger.warning("Authentication not configured -- server is unauthenticated (see T-004)")

    server = create_server(config)
    server.run(transport="http", host=config.server.host, port=config.server.port)
