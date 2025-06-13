import logging

def get_logger(name):
    """
    获取主日志 logger（写入 faird.log）
    """
    logger = logging.getLogger(f"{name}_faird")
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # 主日志文件 handler
        log_handler = logging.FileHandler("faird.log")
        log_handler.setLevel(logging.INFO)
        log_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d] - %(message)s"
        ))

        # 控制台日志 handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d] - %(message)s"
        ))

        logger.addHandler(log_handler)
        logger.addHandler(console_handler)
    return logger

def get_access_logger(name):
    """
    获取访问日志 logger（写入 access.log）
    """
    logger = logging.getLogger(f"{name}_access")
    if not logger.handlers:
        logger.setLevel(logging.INFO)

        # 访问日志文件 handler
        access_handler = logging.FileHandler("access.log")
        access_handler.setLevel(logging.INFO)
        access_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d] - %(message)s"
        ))

        # 控制台日志 handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s [%(filename)s:%(lineno)d] - %(message)s"
        ))

        logger.addHandler(access_handler)
        logger.addHandler(console_handler)
    return logger