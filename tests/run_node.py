"""Run the installed CLI with the named configer and cancel it when stdin closes."""

from __future__ import annotations

import asyncio
import sys
import threading

from bazaar_compute_node.cli import dispatch
from blockbuster import blockbuster_ctx


async def serve():
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(
        dispatch(
            ("run",),
            prog_name="bcn",
            configer=sys.argv[1],
            configer_options=(sys.argv[2],),
        )
    )

    def stop():
        sys.stdin.readline()
        loop.call_soon_threadsafe(task.cancel)

    await asyncio.to_thread(threading.Thread(target=stop, daemon=True).start)
    try:
        await task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    with blockbuster_ctx():
        asyncio.run(serve())
