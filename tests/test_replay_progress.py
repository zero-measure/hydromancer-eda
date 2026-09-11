import asyncio
import logging

from hydromancer_eda.ingestion.progress import ReplayProgress


class Connection:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def test_progress_reports_chunks_and_completion(caplog) -> None:
    progress = ReplayProgress("allFills", logging.getLogger("test.progress"), 1, 10)

    with caplog.at_level(logging.INFO):
        progress.start("existing session resumed")
        progress.advance(1, 2, 25, False, 3)
        progress.advance(2, 2, 10, False, 4)
        progress.finish(4)

    assert "backfill started" in caplog.text
    assert "1/2 (50%)" in caplog.text
    assert "35 items persisted" in caplog.text
    assert "backfill finished" in caplog.text


def test_monitor_reports_stall_and_closes_connection(caplog) -> None:
    async def run() -> Connection:
        progress = ReplayProgress("allFills", logging.getLogger("test.stall"), 0.005, 0.01)
        connection = Connection()
        progress.start("test")
        await progress.monitor(connection, asyncio.Event(), lambda: 2)
        assert progress.stalled
        return connection

    with caplog.at_level(logging.INFO):
        connection = asyncio.run(run())

    assert connection.closed
    assert "backfill status" in caplog.text
    assert "backfill stalled" in caplog.text
    assert "durable cursor" in caplog.text


def test_monitor_marks_no_replay_as_current_after_grace(caplog) -> None:
    async def run() -> tuple[ReplayProgress, bool]:
        progress = ReplayProgress("quiet", logging.getLogger("test.quiet"), 0.005, 1)
        connection = Connection()
        completed = False

        async def finish() -> None:
            nonlocal completed
            completed = True
            progress.finish(0, "no replay required")

        progress.start("test")
        task = asyncio.create_task(
            progress.monitor(connection, asyncio.Event(), lambda: 0, 0.01, finish)
        )
        while not completed:
            await asyncio.sleep(0.005)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return progress, connection.closed

    with caplog.at_level(logging.INFO):
        progress, closed = asyncio.run(run())

    assert not progress.active
    assert not closed
    assert "no replay required" in caplog.text
