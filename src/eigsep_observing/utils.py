import functools
from importlib import resources
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union, Callable, Any

import numpy as np
import yaml


def get_path(
    dirname: Optional[Union[str, Path]] = None,
    fname: Optional[Union[str, Path]] = None,
) -> Path:
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


def get_data_path(fname: Optional[Union[str, Path]] = None) -> Path:
    """
    Get the path to the data directory within the package.
    If `fname` is provided, return the full path to that file.
    """
    return get_path(dirname="data", fname=fname)


def calc_freqs_dfreq(sample_rate_Hz, nchan):
    """Return frequencies and delta between frequencies for real-sampled
    spectra from the SNAP spectrometer/correlator."""
    dfreq = sample_rate_Hz / (2 * nchan)  # assumes real sampling
    freqs = np.arange(nchan) * dfreq
    return freqs, dfreq


def calc_inttime(sample_rate_Hz, acc_len, acc_bins=2):
    """Calculate time per integration [s] from sample_freq and acc_len."""
    inttime = 1 / sample_rate_Hz * acc_len * acc_bins
    return inttime


def calc_times(acc_cnt, inttime, sync_time):
    """Calculate integration times [s] from acc_cnt using sync time."""
    times = acc_cnt * inttime + sync_time
    return times


def calc_integration_len(itemsize, acc_bins, nchan, pairs):
    """
    Calculate the number of bytes for an integration of ``acc_bins`` bins.
    Cross-correlations have double length since there's a real and
    imaginary part.

    Parameters
    ----------
    itemsize : int
        Size of data type in bytes.
    acc_bins : int
        Number of accumulations per integration.
    nchan : int
        Number of frequency channels per spectrum.
    pairs : list of str
        List of correlation pairs. Length 1 for autos, 2 for cross.

    Returns
    -------
    int_len : int
        Number of bytes for an integration of ``acc_bins`` bins.

    """
    n_auto = len([p for p in pairs if len(p) == 1])
    n_cross = len(pairs) - n_auto
    return itemsize * acc_bins * nchan * (n_auto + 2 * n_cross)


def load_config(name, compute_inttime=True):
    """
    Load a YAML configuration file.

    Parameters
    ----------
    name : str or Path
        Path to the configuration file.
    compute_inttime : bool
        If True, compute and inject ``integration_time`` from
        ``sample_rate``, ``corr_acc_len``, and ``acc_bins``.

    Returns
    -------
    dict
        Configuration parameters.

    """
    config_path = Path(name)
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    if compute_inttime:
        sample_rate = config["sample_rate"]
        corr_acc_len = config["corr_acc_len"]
        acc_bins = config["acc_bins"]
        config["integration_time"] = calc_inttime(
            sample_rate * 1e6,  # in Hz
            corr_acc_len,
            acc_bins=acc_bins,
        )
    return config


def configure_eig_logger(
    log_file: Optional[Union[str, Path]] = None,
    level: int = logging.INFO,
    max_bytes: int = 10 * 1024 * 1024,
    backup_count: int = 5,
    console: bool = True,
) -> logging.Logger:
    """
    Configure the root logger with a rotating file handler and
    (optionally) a console handler.

    Parameters
    ----------
    log_file : str or Path, optional
        Path to the log file. A relative path is resolved against
        ``~`` so the default location is deterministic regardless of
        CWD. Defaults to ``~/eigsep.log``.
    level : int
    max_bytes : int
        The maximum size of the log file before rotation.
    backup_count : int
        The number of backup files to keep.
    console : bool
        If True (default), also attach a ``StreamHandler`` so log
        lines appear on the terminal in addition to the rotating
        file.

    Returns
    -------
    logging.Logger
        Configured root logger.

    """
    if log_file is None:
        log_file = Path.home() / "eigsep.log"
    else:
        log_file = Path(log_file).expanduser()
        if not log_file.is_absolute():
            log_file = Path.home() / log_file

    logger = logging.getLogger()  # get the root logger
    logger.setLevel(level)
    if not logger.hasHandlers():
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_bytes, backupCount=backup_count
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        if console:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
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
