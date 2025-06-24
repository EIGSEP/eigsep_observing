import functools
import logging
from logging.handlers import RotatingFileHandler


def configure_eig_logger(
    log_file="eigsep.log",
    level=logging.INFO,
    max_bytes=10 * 1024 * 1024,
    backup_count=5,
):
    """
    Configure a logger with a rotating file handler.

    Parameters
    ----------
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
    logger = logging.getLogger()  # get the root logger
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


def require_attr(attr_name, exception=AttributeError):
    """
    Decorator to ensure `self.<attr_name>` is True and not None.
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            if not getattr(self, attr_name):
                raise exception(
                    f"{self.__class__.__name__!r} needs `{attr_name}` set"
                    "before calling `{func.__name__}`"
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


require_panda = require_attr("panda_connected")
require_snap = require_attr("snap_connected")
