from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote, unquote

_PROVIDER_PREFIX = "wecom"


def conversation_thread_id(bot_id: str, conversation: str) -> str:
    """Address a conversation by the bot it lives on and WeCom's own chat id."""

    return f"{_PROVIDER_PREFIX}:{bot_id}:{conversation}"


def parse_provider_thread_id(value: str) -> tuple[str, str]:
    """Split a provider thread id back into the bot id and the WeCom chat id."""

    if not isinstance(value, str) or not value:
        raise ValueError("WeCom provider_thread_id must be non-empty")
    parts = value.split(":", 2)
    if len(parts) != 3 or parts[0] != _PROVIDER_PREFIX or not parts[1] or not parts[2]:
        raise ValueError("WeCom provider_thread_id has invalid format")
    return parts[1], parts[2]


@dataclass(frozen=True, slots=True)
class TopicAddress:
    bot_id: str
    topic_id: str
    post_id: str | None = None

    @property
    def provider_thread_id(self) -> str:
        value = (
            f"wecom:{quote(self.bot_id, safe='')}:topic:{quote(self.topic_id, safe='')}"
        )
        if self.post_id is not None:
            value += f":post:{quote(self.post_id, safe='')}"
        return value

    @classmethod
    def parse(cls, value: str) -> TopicAddress | None:
        parts = value.split(":")
        if len(parts) not in {4, 6} or parts[0] != "wecom" or parts[2] != "topic":
            return None
        if not parts[1] or not parts[3]:
            raise ValueError("WeCom topic address requires a bot and channel")
        if len(parts) == 6 and (parts[4] != "post" or not parts[5]):
            raise ValueError("WeCom topic address requires a post id")
        return cls(
            unquote(parts[1]),
            unquote(parts[3]),
            unquote(parts[5]) if len(parts) == 6 else None,
        )


def post_message_id(post_id: str) -> str:
    return "post:" + quote(post_id, safe="")


def reply_message_id(post_id: str, reply_id: str) -> str:
    return "reply:" + quote(post_id, safe="") + ":" + quote(reply_id, safe="")


__all__ = [
    "TopicAddress",
    "conversation_thread_id",
    "parse_provider_thread_id",
    "post_message_id",
    "reply_message_id",
]
