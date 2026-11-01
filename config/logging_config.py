"""Logging helpers: console + per-day file under logs/<stage>/."""
from __future__ import annotations

import logging
import sys
from pathlib import Path

_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def get_logger(name: str, log_dir: Path | None = None, stage: str | None = None,
               run_date: str | None = None) -> logging.Logger:
    """Return a logger. If `log_dir` and `stage` are given, also log to
    <log_dir>/<stage>/<stage>_<run_date>.log (idempotent: handlers are not duplicated)."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not any(getattr(h, "_sp_console", False) for h in logger.handlers):
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(logging.Formatter(_FORMAT))
        console._sp_console = True  # type: ignore[attr-defined]
        logger.addHandler(console)

    if log_dir and stage:
        path = Path(log_dir) / stage / f"{stage}_{(run_date or 'adhoc').replace('-', '')}.log"
        if not any(getattr(h, "_sp_path", None) == str(path) for h in logger.handlers):
            path.parent.mkdir(parents=True, exist_ok=True)
            fh = logging.FileHandler(path, encoding="utf-8")
            fh.setFormatter(logging.Formatter(_FORMAT))
            fh._sp_path = str(path)  # type: ignore[attr-defined]
            logger.addHandler(fh)
    return logger
