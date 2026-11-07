from __future__ import annotations

import asyncio
from pathlib import Path


async def test_wheel_and_sdist_register_the_channel_builder(tmp_path: Path):
    root = (await asyncio.to_thread(Path(__file__).resolve)).parents[1]
    output = tmp_path / "distributions"
    process = await asyncio.create_subprocess_exec(
        "uv", "build", str(root), "--no-sources", "--out-dir", str(output), cwd=tmp_path
    )
    assert await process.wait() == 0
    script = tmp_path / "register.py"
    await asyncio.to_thread(
        script.write_text,
        """import asyncio
from contextlib import asynccontextmanager
from importlib.metadata import entry_points
from pathlib import Path

from bazaar_core import Composition, Context, Contrib, ContribEntry, ContribHub
from bazaar_contrib_channel import CHANNEL_BUILDERS, ChannelInput, channel_builder_key, channel_builders
from blockbuster import blockbuster_ctx

entries = entry_points(group="bazaar_compute_node.contribs", name="wecom-builder")
entry = next(iter(entries))
# the node loads contribs outside its event loop
definition = entry.load()
selected = []
workspace = Path.cwd()

@asynccontextmanager
async def consumer(ctx: Context):
    builder = ctx.accept(channel_builder_key("wecom"))
    selected.append(builder)
    for options in (
        {"bot_id": "editor-bot", "secret_env": "EDITOR_WECOM_SECRET"},
        {"bot_id": "editor-bot", "secret": "configer-supplied-secret"},
    ):
        composition = builder.compose(
            options,
            ChannelInput(agent_id="editor", agent_name="Editor", workspace=workspace),
        )
        assert composition.entries
    assert builder in ctx.accept(CHANNEL_BUILDERS).all()
    yield

async def main():
    composition = Composition(entries=(
        ContribEntry(name="channel-builders", definition=channel_builders),
        ContribEntry(name="wecom-builder", definition=definition),
        ContribEntry(name="consumer", definition=Contrib(
            accepts=[CHANNEL_BUILDERS, channel_builder_key("wecom")], lifespan=consumer
        )),
    ))
    async with ContribHub() as hub, hub.enable(composition):
        assert selected

with blockbuster_ctx():
    asyncio.run(main())
""",
    )
    for suffix in ("*.whl", "*.tar.gz"):
        artifact = next(iter(await asyncio.to_thread(list, output.glob(suffix))))
        process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--isolated",
            "--no-project",
            "--python",
            "3.15",
            "--prerelease",
            "allow",
            "--with",
            str(artifact),
            "--with",
            "blockbuster>=1.5.29,<1.6",
            "python",
            "-I",
            str(script),
            cwd=tmp_path,
        )
        assert await process.wait() == 0
