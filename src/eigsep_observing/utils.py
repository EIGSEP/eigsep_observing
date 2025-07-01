import functools
from importlib import resources
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union, Callable, Any


def get_path(dirname: Optional[Union[str, Path]] = None, fname: Optional[Union[str, Path]] = None) -> Path:
    """
    Get the path to a directory or file within the package.
    Default returns path to the package, <pkg_path>.
    If `dirname` is provided, return path to <pkg_path>/<dirname>.
    If `fname` is provided, return the full path to that file.

    Parameters
    ----------
    dirname : str or Path
        Name of the directory within the package.
    fname : str or Path
        Name of the file within the package or directory.

    Returns
    -------
    Path
        The path to the specified directory or file within the package.
    """
    path = resources.files(__package__)
    if dirname is not None:
        path = path.joinpath(dirname)
    if fname is not None:
        path = path.joinpath(fname)
    return path


def get_config_path(fname: Optional[Union[str, Path]] = None) -> Path:
    """
    Get the path to the configuration directory within the package.
    If `fname` is provided, return the full path to that file.
    """
    return get_path(dirname="config", fname=fname)


def configure_eig_logger(
    log_file: str = "eigsep.log",
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
) -> logging.Logger:
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


def require_attr(attr_name: str, exception: type = AttributeError) -> Callable:
    """
    Decorator to ensure `self.<attr_name>` is True and not None.
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            if not getattr(self, attr_name):
                raise exception(
                    f"{self.__class__.__name__!r} needs `{attr_name}` set "
                    f"before calling `{func.__name__}`"
                )
            return func(self, *args, **kwargs)

        return wrapper

    return decorator


require_panda: Callable = require_attr("panda_connected")
require_snap: Callable = require_attr("snap_connected")
