
import os
import sys
from utils.logger_utils import get_logger
logger = get_logger(__name__)

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

from core.config import FairdConfigManager
from services.server.faird_server import FairdServer

def main():
    ## Load configuration
    FairdConfigManager.load_config(os.path.join(current_dir, 'faird.conf'))
    # FairdConfigManager.load_config(os.path.join(current_dir, 'faird-dev.conf'))
    config = FairdConfigManager.get_config()
    if not config:
        logger.error("Failed to load configuration.")
        return
    logger.info("Application is starting up...")
    FairdServer.create(host=config.domain, port=config.port)

if __name__ == "__main__":
    main()