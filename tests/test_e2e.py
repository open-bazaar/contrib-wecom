"""Drive an installed node from a second bot and assert what WeCom delivers."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import sys
import time
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiohttp
import pytest
import tomli_w
from yarl import URL

from bazaar_contrib_wecom.identity import TopicAddress

API = "https://qyapi.weixin.qq.com/cgi-bin/aibot/"
WAIT_SECONDS = 180


class Bot:
    def __init__(self, session: aiohttp.ClientSession, bot_id: str, secret: str):
        self.session = session
        self.bot_id = bot_id
        self.secret = secret
        self.token: str | None = None

    async def post(
        self, operation: str, body: dict[str, Any], token: str | None
    ) -> dict[str, Any]:
        async with self.session.post(
            API + operation,
            params={"access_token": token} if token is not None else None,
            json=body,
        ) as response:
            result = await response.json(content_type=None)
        if result.get("errcode") != 0:
            raise RuntimeError(
                f"{operation} failed ({result.get('errcode')}): {result.get('errmsg')}"
            )
        return result

    async def call(self, operation: str, **body: Any) -> dict[str, Any]:
        if self.token is None:
            timestamp = int(time.time())
            nonce = uuid4().hex
            signature = hashlib.sha256(
                f"{self.secret}{self.bot_id}{timestamp}{nonce}".encode()
            ).hexdigest()
            self.token = (
                await self.post(
                    "get_token",
                    {
                        "bot_id": self.bot_id,
                        "time": timestamp,
                        "nonce": nonce,
                        "signature": signature,
                    },
                    None,
                )
            )["access_token"]
        return await self.post(operation, body, self.token)

    async def upload(self, name: str, data: bytes) -> str:
        upload = (
            await self.call(
                "upload_media_init",
                type="file",
                filename=name,
                total_size=len(data),
                total_chunks=1,
                md5=hashlib.md5(data, usedforsecurity=False).hexdigest(),
            )
        )["upload_id"]
        await self.call(
            "upload_media_chunk",
            upload_id=upload,
            chunk_index=0,
            base64_data=base64.b64encode(data).decode(),
        )
        return (await self.call("upload_media_finish", upload_id=upload))["media_id"]

    async def download(self, item: dict[str, Any]) -> bytes:
        cookie = item.get("cookie_name")
        async with self.session.get(
            URL(item["download_url"], encoded=True),
            headers={"Cookie": f"{cookie}={item['cookie_value']}"} if cookie else None,
            allow_redirects=False,
        ) as response:
            assert response.status == 200
            return await response.read()


def creator(item: dict[str, Any]) -> str | None:
    member = item.get("creator") or item.get("creator_info") or {}
    return member.get("aibotid") or member.get("botid") or member.get("userid")


async def journal(path: Path) -> list[dict[str, Any]]:
    if not await asyncio.to_thread(path.exists):
        return []
    text = await asyncio.to_thread(path.read_text)
    return [json.loads(line) for line in text.splitlines()]


@pytest.mark.e2e
async def test_installed_node_over_real_wecom(tmp_path: Path):
    tested_id = os.environ["WECOM_TEST_BOT_ID"]
    driver_id = os.environ["WECOM_DRIVER_BOT_ID"]
    driver_secret = os.environ["WECOM_DRIVER_BOT_SECRET"]
    topic = os.environ["WECOM_TEST_TOPIC_ID"]
    opening = "e2e-" + secrets.token_hex(6)
    source = (await asyncio.to_thread(Path(__file__).resolve)).parents[1]
    fixture = source / "tests/fixtures/harness"
    distributions = tmp_path / "distributions"
    config = tmp_path / "node.toml"
    events = tmp_path / "audit.jsonl"
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("BCN_", "BCC_", "CLAUDE_", "ANTHROPIC_", "OPENAI_"))
    } | {
        "BCN_HOME": str(tmp_path / "home"),
        "UV_TOOL_DIR": str(tmp_path / "tools"),
        "UV_TOOL_BIN_DIR": str(tmp_path / "bin"),
        "WECOM_E2E_MARKER": opening,
    }

    async def execute(label: str, *arguments: str) -> str:
        await asyncio.to_thread(print, "Run", label, flush=True)
        process = await asyncio.create_subprocess_exec(
            *arguments,
            cwd=source,
            env=environment,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        text = output.decode()
        await asyncio.to_thread((tmp_path / (label + ".log")).write_text, text)
        assert process.returncode == 0, text
        return text

    for path in (source, fixture):
        await execute(
            "build-" + path.name,
            "uv",
            "build",
            str(path),
            "--no-sources",
            "--wheel",
            "--out-dir",
            str(distributions),
        )
    (provider,) = await asyncio.to_thread(
        lambda: tuple(distributions.glob("bazaar_contrib_wecom-*.whl"))
    )
    (harness,) = await asyncio.to_thread(
        lambda: tuple(distributions.glob("bcn_wecom_test_harness-*.whl"))
    )
    await execute(
        "install",
        "uv",
        "tool",
        "install",
        "--python",
        "3.15",
        "--prerelease",
        "allow",
        "--refresh",
        "--default-index",
        "https://pypi.org/simple",
        "bazaar-compute-node>=0.3.0rc19,<0.4",
        "--with",
        str(provider),
        "--with",
        str(harness),
    )
    python = (
        await execute(
            "python",
            "uv",
            "python",
            "find",
            "--no-project",
            str(tmp_path / "tools/bazaar-compute-node"),
        )
    ).strip()
    node = (python, "-m", "bazaar_compute_node", "--configer-options", str(config))
    await execute("bootstrap", *node, "--help")
    document = tomllib.loads(await asyncio.to_thread(config.read_text))
    for package in document["contribs"]:
        for entry in package["enabled"]:
            if entry["entry"] == "agent":
                entry["config"]["agents"] = [
                    {
                        "id": "wecom-test",
                        "name": "WeCom Test",
                        "mode": "dangerous_individual",
                        "channel": [
                            {
                                "kind": "wecom",
                                "bot_id": tested_id,
                                "secret_env": "WECOM_TEST_BOT_SECRET",
                            }
                        ],
                        "harness": [{"kind": "wecom-test"}],
                    }
                ]
            elif entry["entry"] in {"sidecar-unix", "sidecar-windows"}:
                entry["config"] = {"endpoint": f"/tmp/bcn-wecom-{uuid4()}.sock"}
    document["contribs"] = [
        package
        for package in document["contribs"]
        if package["package"] not in {"bazaar-contrib-wecom", "bcn-wecom-test-harness"}
    ] + [
        {
            "package": "bazaar-contrib-wecom @ " + provider.as_uri(),
            "enabled": [{"entry": "wecom-builder"}],
        },
        {
            "package": "bcn-wecom-test-harness @ " + harness.as_uri(),
            "enabled": [
                {"entry": "test-harness-builder"},
                {"entry": "audit-journal", "config": {"path": str(events)}},
            ],
        },
    ]
    await asyncio.to_thread(config.write_text, tomli_w.dumps(document))
    await execute("activation", *node, "--help")

    async with aiohttp.ClientSession() as session:
        driver = Bot(session, driver_id, driver_secret)
        running: list[asyncio.subprocess.Process] = []

        async def start(configer: str) -> None:
            output = tmp_path / f"node-{configer}.log"
            log = await asyncio.to_thread(output.open, "wb")
            process = await asyncio.create_subprocess_exec(
                python,
                str(source / "tests/run_node.py"),
                configer,
                str(config),
                cwd=source,
                env=environment,
                stdin=asyncio.subprocess.PIPE,
                stdout=log,
                stderr=asyncio.subprocess.STDOUT,
            )
            await asyncio.to_thread(log.close)
            running.append(process)
            subscribed = len(await subscriptions())
            async with asyncio.timeout(WAIT_SECONDS):
                while len(await subscriptions()) == subscribed:
                    assert process.returncode is None, "the node exited before ready"
                    await asyncio.sleep(0.5)

        async def subscriptions() -> list[dict[str, Any]]:
            return [
                event
                for event in await journal(events)
                if event["event_name"] == "channel.connection.connected"
            ]

        async def stop() -> None:
            while running:
                process = running.pop()
                assert process.stdin is not None
                process.stdin.close()
                await process.wait()

        async def persisted(provider_thread_id: str) -> dict[str, Any]:
            async with asyncio.timeout(WAIT_SECONDS):
                while True:
                    for event in await journal(events):
                        if (
                            event["event_name"] == "channel.inbound.persisted"
                            and event["payload"]["correlation"].get(
                                "provider_thread_id"
                            )
                            == provider_thread_id
                        ):
                            return event
                    await asyncio.sleep(0.5)

        async def posted(match: Callable[[dict[str, Any]], bool]) -> dict[str, Any]:
            async with asyncio.timeout(WAIT_SECONDS):
                while True:
                    posts = await driver.call(
                        "topic/get_post_list",
                        topic_id=topic,
                        type=1,
                        sort_type=1,
                        limit=50,
                    )
                    for item in posts["post_list"]:
                        if match(item):
                            return item
                    await asyncio.sleep(2)

        async def replies(post_id: str) -> list[dict[str, Any]]:
            collected: list[dict[str, Any]] = []
            cursor = ""
            while True:
                page = await driver.call(
                    "topic/get_reply_list",
                    topic_id=topic,
                    post_id=post_id,
                    type=1,
                    cursor=cursor,
                    limit=50,
                )
                collected.extend(page["reply_list"])
                if not page.get("has_more"):
                    return collected
                cursor = page["next_cursor"]

        async def replied(
            post_id: str, match: Callable[[dict[str, Any]], bool]
        ) -> dict[str, Any]:
            async with asyncio.timeout(WAIT_SECONDS):
                while True:
                    for item in await replies(post_id):
                        if creator(item) == tested_id and match(item):
                            return item
                    await asyncio.sleep(2)

        try:
            await start("config-filesystem")
            connected = (await subscriptions())[-1]
            assert connected["context"]["connection"] == "wecom:0"
            assert connected["payload"]["identity"]["id"] == tested_id

            await driver.call(
                "topic/send_post", topic_id=topic, content=f"{opening} open"
            )
            root = TopicAddress(tested_id, topic).provider_thread_id
            async with asyncio.timeout(WAIT_SECONDS):
                while True:
                    threads = [
                        event["context"]["thread_id"]
                        for event in await journal(events)
                        if event["event_name"] == "channel.received"
                        and event["payload"]["target"]["thread_id"] == root
                    ]
                    if threads:
                        break
                    await asyncio.sleep(0.5)
            await execute("review", *node, "agent", "review", "approve", threads[0])
            second = await driver.call(
                "topic/send_post", topic_id=topic, content=f"{opening} second"
            )
            opened = await persisted(
                TopicAddress(tested_id, topic, second["post_id"]).provider_thread_id
            )
            assert opened["payload"]["metadata"]["review"] == "approved"

            started = await posted(
                lambda item: (
                    creator(item) == tested_id
                    and item.get("content") == f"{opening} post"
                )
            )
            post_id = started["post_id"]

            marker = "e2e-" + secrets.token_hex(6)
            asked = await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} reply",
            )
            answer = await replied(
                post_id, lambda item: item.get("content") == f"{marker} reply"
            )
            assert answer.get("parent_reply_id") == asked["reply_id"]

            marker = "e2e-" + secrets.token_hex(6)
            original = await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} original",
            )
            await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} quote",
                parent_reply_id=original["reply_id"],
            )
            await replied(
                post_id, lambda item: item.get("content") == f"{marker} original"
            )

            marker = "e2e-" + secrets.token_hex(6)
            await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} long",
            )
            end = await replied(
                post_id, lambda item: f"END {marker}" in item.get("content", "")
            )
            parts = [
                item["content"]
                for item in await replies(post_id)
                if creator(item) == tested_id and marker in item.get("content", "")
            ]
            sections = [
                int(number)
                for part in parts
                for number in re.findall(rf"{marker}-(\d{{4}})", part)
            ]
            assert len(parts) > 1
            assert sections == list(range(1, int(end["content"].rsplit(" ", 1)[1]) + 1))

            marker = "e2e-" + secrets.token_hex(6)
            expected = await asyncio.to_thread(
                (fixture / "src/bcn_wecom_test_harness/attachment.txt").read_bytes
            )
            await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} attach",
            )
            attached = await replied(
                post_id, lambda item: bool(item.get("inline_file_list"))
            )
            assert await driver.download(attached["inline_file_list"][0]) == expected

            marker = "e2e-" + secrets.token_hex(6)
            payload = secrets.token_bytes(4096)
            media = await driver.upload(f"{marker}.bin", payload)
            await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} echo ${{inline_file:echo}}",
                inline_file_list=[{"placeholder": "echo", "media_id": media}],
            )
            echoed = await replied(
                post_id,
                lambda item: (
                    bool(item.get("inline_file_list"))
                    and item["reply_id"] != attached["reply_id"]
                ),
            )
            (file,) = echoed["inline_file_list"]
            assert await driver.download(file) == payload

            # Another connection subscribing as the same bot takes it over, and
            # the node reports that it will not reconnect.
            before = len(await journal(events))
            async with session.ws_connect("wss://openws.work.weixin.qq.com") as other:
                await other.send_str(
                    json.dumps(
                        {
                            "cmd": "aibot_subscribe",
                            "headers": {"req_id": f"aibot_subscribe-{uuid4()}"},
                            "body": {
                                "bot_id": tested_id,
                                "secret": os.environ["WECOM_TEST_BOT_SECRET"],
                            },
                        }
                    )
                )
                await other.receive()
                async with asyncio.timeout(WAIT_SECONDS):
                    while True:
                        failures = [
                            event
                            for event in (await journal(events))[before:]
                            if event["event_name"] == "channel.connection.failed"
                        ]
                        if failures:
                            break
                        await asyncio.sleep(0.5)
            assert failures[0]["context"]["connection"] == "wecom:0"
            assert failures[0]["payload"]["retry_in_ms"] is None
            assert failures[0]["error"]["kind"] == "wecom_disconnected_event"

            await stop()
            await start("config-env-token")
            marker = "e2e-" + secrets.token_hex(6)
            asked = await driver.call(
                "topic/send_reply",
                topic_id=topic,
                post_id=post_id,
                content=f"{marker} reply",
            )
            answer = await replied(
                post_id, lambda item: item.get("content") == f"{marker} reply"
            )
            assert answer.get("parent_reply_id") == asked["reply_id"]
        finally:
            await stop()
            await asyncio.to_thread(sys.stdout.flush)
