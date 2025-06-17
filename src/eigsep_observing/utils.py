import logging
from logging.handlers import RotatingFileHandler


def eig_logger(
    name,
    log_file="eigsep.log",
    level=logging.INFO,
    max_bytes=10 * 1024 * 1024,
    backup_count=5,
):
    """
    Configure a logger with a rotating file handler.

    Parameters
    ----------
    name : str
    log_file : str
        The name of the log file.
    level : int
    max_bytes : int
        The maximum size of the log file before rotation.
    backup_count : int
        The number of backup files to keep.

    Returns
    -------
    logging.Logger
        Configured logger instance.

    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        handler.setLevel(level)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
