"""Loafer — AI-assisted ETL and ELT pipelines."""

__version__ = "0.1.0"


def _daemon_entry() -> None:
    """Entry point for the background scheduler daemon."""
    import logging
    import sys

    from loafer.scheduler import PipelineScheduler

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    scheduler = PipelineScheduler()
    scheduler.start()

    try:
        import time

        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.stop()
