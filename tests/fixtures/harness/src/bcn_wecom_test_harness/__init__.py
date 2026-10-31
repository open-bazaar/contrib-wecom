"""A Harness that sends what marked messages ask for, an Audit journal and a configer.

The configer turns each WeCom `secret_env` into a `secret` held only in memory.
"""

# Harness methods implement the public Harness call budgets.
# ruff: noqa: ASYNC109

from __future__ import annotations

import asyncio
import json
import os
import sys
import tomllib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, ClassVar
from uuid import uuid7

from bazaar_contrib_audit import AUDIT_EVENT, AuditEvent
from bazaar_contrib_config import CONFIG_STORE
from bazaar_contrib_config.memory import MemoryConfigStore
from bazaar_contrib_harness import (
    HARNESS,
    HARNESS_BUILDERS,
    HARNESS_INPUT,
    ApprovalHandler,
    ApprovalRequest,
    ContentDelta,
    ContentDeltaKind,
    HarnessBuilderConfig,
    HarnessEventPayload,
    HarnessExpire,
    HarnessFailure,
    HarnessFailureReason,
    HarnessInspection,
    HarnessOutput,
    HarnessSession,
    HarnessSessionReconciliation,
    HarnessSessionUnavailable,
    HarnessTurn,
    ProviderCallResult,
    ProviderCallStatus,
    SessionInput,
    TurnCompleted,
    TurnFailed,
    TurnStarted,
    harness_builder_key,
)
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict


class Turn:
    """One worker process reported as a started, active and finished turn."""

    def __init__(
        self,
        session: HarnessSession,
        turn: HarnessTurn,
        process: asyncio.subprocess.Process,
        approvals: ApprovalHandler,
        timeout: float,
    ) -> None:
        self.session = session
        self.turn = turn
        self.process = process
        self.approvals = approvals
        self.timeout = timeout
        self.outputs = self.run()

    def output(self, payload: HarnessEventPayload) -> HarnessOutput:
        return HarnessOutput(
            session_id=self.session.id, turn_id=self.turn.id, payload=payload
        )

    async def run(self) -> AsyncGenerator[HarnessOutput]:
        yield self.output(TurnStarted(event_name="test.turn.started"))
        yield self.output(
            ContentDelta(kind=ContentDeltaKind.AGENT_MESSAGE, text="sending")
        )
        assert self.process.stdin is not None and self.process.stdout is not None
        async for line in self.process.stdout:
            if not line.startswith(b"{"):
                continue
            result = await self.approvals.request_approval(
                ApprovalRequest(
                    request_id=str(uuid7()),
                    session_id=self.session.id,
                    turn_id=self.turn.id,
                    action="command_execution",
                    details=json.loads(line)["approval"],
                ),
                timeout=self.timeout,
            )
            self.process.stdin.write(
                json.dumps({"decision": result.decision}).encode() + b"\n"
            )
            await self.process.stdin.drain()
        returncode = await self.process.wait()
        if returncode:
            yield self.output(
                TurnFailed(
                    event_name="test.turn.failed",
                    failure=HarnessFailure(
                        HarnessFailureReason.EXECUTION,
                        f"The sending worker exited with {returncode}",
                    ),
                )
            )
            return
        yield self.output(TurnCompleted(event_name="test.turn.completed"))

    def __aiter__(self) -> Turn:
        return self

    async def __anext__(self) -> HarnessOutput:
        return await anext(self.outputs)

    async def aclose(self) -> None:
        await self.outputs.aclose()
        if self.process.returncode is None:
            self.process.kill()
            await self.process.wait()


class SendingHarness:
    """Run the sending worker with each session's environment."""

    def __init__(self) -> None:
        self.sessions: dict[str, SessionInput] = {}
        self.events: asyncio.Queue[HarnessExpire] = asyncio.Queue()

    async def receive_event(self) -> HarnessExpire:
        return await self.events.get()

    async def start_session(
        self, request: SessionInput, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        self.sessions[request.id] = request
        return ProviderCallResult(
            status=ProviderCallStatus.CONFIRMED,
            value=HarnessSession(id=request.id, provider_thread_id=request.id),
        )

    async def reconcile_session(
        self,
        session: HarnessSession,
        turn: HarnessTurn | None,
        approval_handler: ApprovalHandler | None,
        *,
        timeout: float,
    ) -> ProviderCallResult[HarnessSessionReconciliation]:
        if session.id not in self.sessions:
            return ProviderCallResult(
                status=ProviderCallStatus.FAILED,
                failure=HarnessFailure(
                    HarnessFailureReason.UNAVAILABLE,
                    "Test sessions live only in this process",
                ),
            )
        return ProviderCallResult(
            status=ProviderCallStatus.CONFIRMED,
            value=HarnessSessionReconciliation(session=session),
        )

    async def start_turn(
        self,
        session: HarnessSession,
        turn: HarnessTurn,
        approval_handler: ApprovalHandler,
        *,
        timeout: float,
    ) -> Turn:
        request = self.sessions.get(session.id)
        if request is None:
            raise HarnessSessionUnavailable("Test sessions live only in this process")
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "bcn_wecom_test_harness.worker",
            cwd=request.workspace,
            env=os.environ | request.environment,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
        )
        return Turn(session, turn, process, approval_handler, timeout)

    async def steer_turn(
        self,
        session: HarnessSession,
        turn: HarnessTurn,
        input_text: str,
        *,
        timeout: float,
    ) -> bool:
        return False

    async def has_background_job(
        self, session: HarnessSession, *, timeout: float
    ) -> bool:
        return False

    async def stop_session(
        self, session: HarnessSession, *, timeout: float
    ) -> ProviderCallResult[HarnessSession]:
        self.sessions.pop(session.id, None)
        return ProviderCallResult(status=ProviderCallStatus.CONFIRMED, value=session)


class Config(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


@asynccontextmanager
async def lifespan(ctx: Context, config: Config):
    ctx.offer(HARNESS, SendingHarness())
    yield


harness = Contrib(
    config=Config,
    accepts=[HARNESS_INPUT],
    offers=[HARNESS],
    lifespan=lifespan,
)


class BuilderConfig(HarnessBuilderConfig):
    kind: ClassVar[str] = "wecom-test"
    entity_model: ClassVar[type[BaseModel]] = Config
    entry: ClassVar[Contrib[Any]] = harness

    @classmethod
    async def inspect(cls, workspace: Path, *, timeout: float) -> HarnessInspection:
        return HarnessInspection(available=True)


@asynccontextmanager
async def builder_lifespan(ctx: Context, config: BuilderConfig):
    with ctx.accept(HARNESS_BUILDERS).register(config.builder()) as builder:
        ctx.offer(harness_builder_key(builder.kind), builder)
        yield


harness_builder = Contrib(
    config=BuilderConfig,
    accepts=[HARNESS_BUILDERS],
    offers=[harness_builder_key("wecom-test")],
    lifespan=builder_lifespan,
)


class JournalConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    path: Path


journals: list[Path] = []


@asynccontextmanager
async def journal_lifespan(ctx: Context, config: JournalConfig):
    journals.append(config.path)
    try:
        yield
    finally:
        journals.remove(config.path)


def append(path: Path, line: str) -> None:
    with path.open("a") as stream:
        stream.write(line + "\n")


async def record(ctx: Context, event: AuditEvent) -> None:
    line = event.model_dump_json()
    for path in journals:
        await asyncio.to_thread(append, path, line)


journal = Contrib(
    config=JournalConfig, lifespan=journal_lifespan, listen={AUDIT_EVENT: record}
)


class EnvTokenConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    options: tuple[Path]


@asynccontextmanager
async def env_token_lifespan(ctx: Context, config: EnvTokenConfig):
    (path,) = config.options
    document = tomllib.loads(await asyncio.to_thread(path.read_text))
    for package in document["contribs"]:
        for entry in package["enabled"]:
            if entry["entry"] == "agent":
                for agent in entry["config"]["agents"]:
                    for channel in agent["channel"]:
                        if channel["kind"] == "wecom":
                            channel["secret"] = os.environ[channel.pop("secret_env")]
    store = MemoryConfigStore()
    await store.write(document, expected_revision=None)
    ctx.offer(CONFIG_STORE, store)
    yield


env_token = Contrib(
    config=EnvTokenConfig, offers=[CONFIG_STORE], lifespan=env_token_lifespan
)
