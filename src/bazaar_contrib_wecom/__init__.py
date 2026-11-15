"""Register WeCom and connect each selected agent's Channel."""

from __future__ import annotations

import asyncio
import json
import os
import tomllib
from contextlib import asynccontextmanager
from importlib.resources import files
from pathlib import Path
from typing import Any, ClassVar, cast

from bazaar_contrib_audit import AUDIT
from bazaar_contrib_channel import (
    CHANNEL,
    CHANNEL_BUILDERS,
    CHANNEL_INPUT,
    ChannelBuilderConfig,
    ChannelTarget,
    channel_builder_key,
)
from bazaar_contrib_collaboration import CONTACT_REVIEWED, ContactReviewed
from bazaar_contrib_messages import MESSAGES
from bazaar_contrib_timer import TIMER
from bazaar_core import Context, Contrib
from pydantic import BaseModel, ConfigDict, Field

from .channel import WeComChannel
from .identity import TopicAddress, parse_provider_thread_id

_LOCALES = Path(str(files(__name__))) / "resources" / "locales"


class Config(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        json_schema_extra={
            "x-locales": {
                path.stem: tomllib.loads(path.read_text(encoding="utf-8"))
                for path in sorted(_LOCALES.glob("*.toml"))
            }
        },
    )
    bot_id: str = Field(min_length=1)
    secret_env: str | None = Field(default=None, min_length=1)
    secret: str | None = Field(default=None, json_schema_extra={"writeOnly": True})
    websocket_url: str = Field(default="wss://openws.work.weixin.qq.com", min_length=1)


@asynccontextmanager
async def lifespan(ctx: Context, config: Config):
    secret = config.secret
    if secret is None and config.secret_env is not None:
        secret = os.environ.get(config.secret_env)
    if secret is None or not secret.strip():
        raise ValueError("wecom credential is missing: configure secret or secret_env")
    async with asyncio.TaskGroup() as group:
        instance = WeComChannel(
            ctx.accept(CHANNEL_INPUT),
            ctx.accept(TIMER),
            ctx.accept(AUDIT),
            ctx.accept(MESSAGES),
            group,
            bot_id=config.bot_id,
            secret=secret,
            websocket_url=config.websocket_url,
        )
        await instance.start(timeout=60)
        ctx.offer(CHANNEL, instance)
        try:
            yield
        finally:
            await instance.stop(timeout=5)


async def reviewed(ctx: Context, event: ContactReviewed) -> None:
    cast(WeComChannel, ctx.accept(CHANNEL)).forget_review(event)


channel = Contrib(
    config=Config,
    accepts=[CHANNEL_INPUT, TIMER, AUDIT, MESSAGES],
    offers=[CHANNEL],
    lifespan=lifespan,
    listen={CONTACT_REVIEWED: reviewed},
)


class BuilderConfig(ChannelBuilderConfig):
    kind: ClassVar[str] = "wecom"
    entity_model: ClassVar[type[BaseModel]] = Config
    entry: ClassVar[Contrib[Any]] = channel

    @classmethod
    def conversation_key(
        cls, target: ChannelTarget, configs: tuple[dict[str, Any], ...] = ()
    ) -> str | None:
        if target.kind != "group":
            return None
        peer: tuple[str | None, ...]
        topic = TopicAddress.parse(target.thread_id)
        if topic is None:
            bot_id, chat_id = parse_provider_thread_id(target.thread_id)
            peer = ("chat", chat_id)
        else:
            bot_id = topic.bot_id
            peer = ("topic", topic.topic_id, topic.post_id)
        endpoints = {
            config.websocket_url
            for values in configs
            if (config := Config.model_validate(values)).bot_id == bot_id
        }
        if len(endpoints) != 1:
            return None
        return json.dumps((endpoints.pop(), *peer))


@asynccontextmanager
async def builder_lifespan(ctx: Context, config: BuilderConfig):
    with ctx.accept(CHANNEL_BUILDERS).register(config.builder()) as builder:
        ctx.offer(channel_builder_key(builder.kind), builder)
        yield


builder = Contrib(
    config=BuilderConfig,
    accepts=[CHANNEL_BUILDERS],
    offers=[channel_builder_key("wecom")],
    lifespan=builder_lifespan,
)
