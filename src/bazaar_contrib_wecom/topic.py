"""Project WeCom channels and their posts onto the public Channel model."""

# Existing WeCom request and lifecycle budgets are retained.
# ruff: noqa: ASYNC109
from __future__ import annotations

import asyncio
import base64
import math
import mimetypes
import re
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path
from time import time_ns
from urllib.parse import unquote
from uuid import NAMESPACE_URL, uuid5

from bazaar_contrib_audit import AuditError
from bazaar_contrib_channel import (
    ChannelConnectionError,
    ChannelDeliveryReceipt,
    ChannelError,
    ChannelRateLimitError,
    ChannelRejectedError,
    ChannelSendAttempt,
    ChannelSendPart,
    ChannelSendPhase,
    ChannelSendRequest,
    ChannelTarget,
    ChannelTargetPresentation,
    InboundAttachment,
    InboundMessage,
    SenderIdentity,
)
from bazaar_contrib_collaboration import ContactReviewed
from bazaar_contrib_messages import Agent, Messages
from bazaar_contrib_messages.states import Review
from bazaar_contrib_timer import Timer, wait_for
from pydantic import JsonValue, ValidationError

from .identity import TopicAddress, post_message_id, reply_message_id
from .markdown import split_markdown, utf8_bytes
from .outbound import AttachmentReader, PreparedAttachment, prepare_attachments
from .topic_api import (
    ApiResponse,
    Content,
    Download,
    Member,
    Post,
    Reply,
    SentPost,
    SentReply,
    TopicApiError,
    TopicClient,
    TopicEvent,
    Topics,
    UploadFinished,
    UploadStarted,
)

_CONTENT_BYTES = 20_480
_ROOT_MESSAGE_ID = "channel"
_MARKDOWN_LITERAL = re.compile(
    r"(?m)^ {0,3}(?P<fence>`{3,}|~{3,})[^\n]*\n.*?"
    r"^ {0,3}(?P=fence)[`~]*[ \t]*(?:\n|$)"
    r"|(?m:^ {4}[^\n]*(?:\n|$))"
    r"|(?P<ticks>`+)(?!`).*?(?<!`)(?P=ticks)(?!`)"
    r"|!?\[[^\]\n]*\]\([^\n]*?\)"
    r"|<[^>\n]+>",
    re.DOTALL,
)

type Publish = Callable[[str, JsonValue, AuditError | None], Awaitable[None]]


def mention_content(
    body: str, members: tuple[Member, ...]
) -> tuple[str, list[dict[str, object]]]:
    """Turn explicit human handles outside Markdown literals into native mentions."""

    ids = {member.id for member in members if member.type == 0}
    if not ids:
        return body, []
    pattern = re.compile(
        r"(?<![A-Za-z0-9_@\\])@("
        + "|".join(re.escape(id) for id in sorted(ids, key=len, reverse=True))
        + r")(?![A-Za-z0-9_.-])"
    )
    mentions: list[dict[str, object]] = []
    placeholders: dict[str, str] = {}

    def replace(match: re.Match[str]) -> str:
        userid = match[1]
        if userid not in placeholders:
            if len(placeholders) == 8:
                raise ValueError(
                    "WeCom supports at most eight distinct mentions per message"
                )
            placeholder = f"mention_{len(placeholders)}"
            placeholders[userid] = placeholder
            mentions.append(
                {"placeholder": placeholder, "member": {"type": 0, "userid": userid}}
            )
        return "${inline_mention:" + placeholders[userid] + "}"

    result: list[str] = []
    start = 0
    for literal in _MARKDOWN_LITERAL.finditer(body):
        result.append(pattern.sub(replace, body[start : literal.start()]))
        result.append(literal[0])
        start = literal.end()
    result.append(pattern.sub(replace, body[start:]))
    return "".join(result), mentions


def topic_message(
    address: TopicAddress,
    *,
    provider_id: str,
    body: str,
    creator: Member,
    occurred_at_s: int,
    name: str,
    attachments: tuple[InboundAttachment, ...] = (),
    parent_id: str | None = None,
    mentions_agent: bool = False,
    notifies_runtime: bool = True,
    approved: bool = False,
    edit_version: str | None = None,
) -> InboundMessage:
    """Project native content into the same conversation and message model as chat."""

    identity = address.provider_thread_id
    sender_kind = "human" if creator.type == 0 else "agent"
    metadata: dict[str, JsonValue] = {
        "sender_kind": sender_kind,
        "threaded": address.post_id is not None,
    }
    if approved:
        metadata["review"] = "approved"
    if parent_id is not None:
        metadata["provider_reply_to_message_id"] = parent_id
    return InboundMessage(
        id=str(uuid5(NAMESPACE_URL, f"bcn:{identity}:message:{provider_id}")),
        thread_id=str(uuid5(NAMESPACE_URL, f"bcn:{identity}")),
        channel_session_id=str(uuid5(NAMESPACE_URL, identity)),
        target=ChannelTarget(
            kind="group", thread_id=identity, reply_to_message_id=parent_id
        ),
        sender=SenderIdentity(id=creator.id, kind=sender_kind),
        body=body,
        target_presentation=ChannelTargetPresentation(display_name=name),
        provider_chat_id=address.topic_id,
        provider_message_id=provider_id,
        edit_version=edit_version,
        occurred_at_ms=occurred_at_s * 1000,
        received_at_ms=time_ns() // 1_000_000,
        attachments=attachments,
        mentions_agent=mentions_agent,
        notifies_runtime=notifies_runtime,
        metadata=metadata,
    )


def channel_error(error: TopicApiError, *, phase: ChannelSendPhase) -> ChannelError:
    if error.code == "http_429":
        return ChannelRateLimitError(str(error), phase=phase, code=str(error.code))
    if error.transient:
        return ChannelConnectionError(str(error), phase=phase, delivery="not_sent")
    return ChannelRejectedError(str(error), phase=phase, code=str(error.code))


@dataclass(slots=True)
class _TopicSend:
    """What the operations of one request learn while it goes out."""

    address: TopicAddress
    parent_reply_id: str | None
    attachments: tuple[PreparedAttachment, ...]
    members: tuple[Member, ...] | None = None
    files: list[dict[str, object]] = field(default_factory=list)
    images: list[dict[str, object]] = field(default_factory=list)


class TopicConversations:
    def __init__(
        self,
        workspace: Path,
        timer: Timer,
        messages: Messages,
        *,
        agent_id: str,
        bot_id: str,
        secret: str,
        emit: Callable[[InboundMessage], Awaitable[None]],
        publish: Publish,
    ) -> None:
        self._workspace = workspace
        self._timer = timer
        self._messages = messages
        self._agent_id = agent_id
        self._bot_id = bot_id
        self._emit = emit
        self._publish = publish
        self.client = TopicClient(bot_id, secret)
        self._events: asyncio.Queue[TopicEvent] = asyncio.Queue()
        self._worker: asyncio.Task[None] | None = None
        self._names: dict[str, str] = {}
        self._roots: set[str] = set()
        self._reviews: dict[str, Review] = {}
        self._root_threads: dict[str, str] = {}
        self._send_lock = asyncio.Lock()
        self._in_flight: set[asyncio.Future[None]] = set()
        self._stopping = True

    def start(self, group: asyncio.TaskGroup) -> None:
        self.client.start()
        self._stopping = False
        self._worker = group.create_task(self._run(), name="bcn-wecom-topics")

    async def stop(self, *, timeout: float) -> None:
        self._stopping = True
        worker = self._worker
        self._worker = None
        if worker is not None:
            worker.cancel()
        pending = (*(() if worker is None else (worker,)), *self._in_flight)
        try:
            if pending:
                await wait_for(
                    self._timer,
                    asyncio.wait(pending),
                    math.ceil(max(0.0, timeout) * 1000),
                )
        except TimeoutError:
            pass
        finally:
            await self.client.close()

    def forget_review(self, event: ContactReviewed) -> None:
        if event.agent.id != self._agent_id:
            return
        topic_id = self._root_threads.pop(event.thread.id, None)
        if topic_id is not None:
            self._reviews.pop(topic_id, None)

    async def receive(self, body: dict[str, object]) -> bool:
        if "topic_id" not in body:
            return False
        if self._stopping:
            return True
        try:
            event = TopicEvent.model_validate(body)
        except ValidationError:
            await self._publish(
                "channel.topic.event_filtered", {"reason": "invalid_event"}, None
            )
            return True
        if event.aibotid != self._bot_id:
            await self._publish(
                "channel.topic.event_filtered",
                {"reason": "identity_mismatch", "msgid": event.msgid},
                None,
            )
            return True
        self._events.put_nowait(event)
        return True

    async def _run(self) -> None:
        while True:
            event = await self._events.get()
            try:
                for delay in (1, 2, 4, None):
                    try:
                        await self._receive(event)
                    except TopicApiError as error:
                        if delay is None or not error.transient:
                            raise
                        await self._timer.create(delay * 1000).wait()
                    else:
                        break
            except TopicApiError as error:
                await self._publish(
                    "channel.topic.event_failed",
                    {"msgid": event.msgid},
                    AuditError(kind=f"wecom_topic_{error.code}", message=str(error)),
                )
            except Exception as error:  # noqa: BLE001
                # One failed event must not stop ingress or reveal provider credentials.
                await self._publish(
                    "channel.topic.event_failed",
                    {"msgid": event.msgid},
                    AuditError(kind=type(error).__name__, message=type(error).__name__),
                )
            finally:
                self._events.task_done()

    async def _approved(self, topic_id: str) -> bool:
        review = self._reviews.get(topic_id)
        if review is None:
            root = TopicAddress(self._bot_id, topic_id).provider_thread_id
            session = await self._messages.storage(
                Agent(self._agent_id)
            ).get_channel_session(
                self._stored_id("channel-session", str(uuid5(NAMESPACE_URL, root)))
            )
            if session is None:
                return False
            review = session.review
            self._reviews[topic_id] = review
            thread = str(uuid5(NAMESPACE_URL, f"bcn:{root}"))
            self._root_threads[self._stored_id("bcn-session", thread)] = topic_id
        return review is Review.APPROVED

    def _stored_id(self, kind: str, channel_id: str) -> str:
        """Name an id the way the agent stores the ids this Channel gives it."""

        return str(uuid5(NAMESPACE_URL, f"bcn:{self._agent_id}:{kind}:{channel_id}"))

    async def _refresh_name(self, topic_id: str) -> str:
        response = await self.client.call(
            "topic/get_topic_list", {"type": 0, "topic_id_list": [topic_id]}, Topics
        )
        self._names.update(
            {topic.topic_id: topic.topic_name for topic in response.topic_list}
        )
        return self._names.get(topic_id, topic_id)

    @staticmethod
    def _creator(event: TopicEvent) -> Member:
        return (
            Member(type=0, userid=event.actor.userid)
            if event.actor.userid is not None
            else Member(type=1, aibotid=event.actor.aibotid)
        )

    async def _root(self, event: TopicEvent) -> None:
        topic_id = event.topic_id
        if topic_id in self._roots:
            return
        name = await self._refresh_name(topic_id)
        approved = await self._approved(topic_id)
        await self._emit(
            topic_message(
                TopicAddress(self._bot_id, topic_id),
                provider_id=_ROOT_MESSAGE_ID,
                body=name,
                creator=self._creator(event),
                occurred_at_s=event.create_time,
                name=name,
                notifies_runtime=not approved,
                mentions_agent=True,
            )
        )
        self._roots.add(topic_id)

    async def _receive(self, event: TopicEvent) -> None:
        native = event.event
        if (
            native.topic_bot_join_leave is not None
            and native.topic_bot_join_leave.change_type == 2
        ):
            self._roots.discard(event.topic_id)
            self._names.pop(event.topic_id, None)
            self._reviews.pop(event.topic_id, None)
            self._root_threads = {
                thread: topic
                for thread, topic in self._root_threads.items()
                if topic != event.topic_id
            }
            return
        await self._root(event)
        if native.eventtype == "topic_info_change":
            name = await self._refresh_name(event.topic_id)
            await self._emit(
                topic_message(
                    TopicAddress(self._bot_id, event.topic_id),
                    provider_id="channel:" + event.msgid,
                    body=name,
                    creator=self._creator(event),
                    occurred_at_s=event.create_time,
                    name=name,
                    notifies_runtime=False,
                    mentions_agent=True,
                )
            )
            return
        detail = native.topic_at_bot or native.topic_reply_event or native.topic_event
        if detail is None or event.actor.aibotid == self._bot_id:
            return
        address = TopicAddress(self._bot_id, event.topic_id, detail.post_id)
        provider_id = (
            reply_message_id(detail.post_id, detail.reply_id)
            if detail.reply_id
            else post_message_id(detail.post_id)
        )
        change_type = getattr(detail, "type", 1)
        name = self._names.get(event.topic_id, event.topic_id) + " · " + detail.post_id
        if change_type == 3:
            await self._emit(
                topic_message(
                    address,
                    provider_id=provider_id + ":event:" + event.msgid,
                    body="[Reply deleted]" if detail.reply_id else "[Post deleted]",
                    creator=self._creator(event),
                    occurred_at_s=event.create_time,
                    name=name,
                    notifies_runtime=False,
                    approved=await self._approved(event.topic_id),
                )
            )
            return
        current: Post | Reply
        parent_id: str | None = None
        if detail.reply_id is None:
            current = await self.client.post(event.topic_id, detail.post_id)
        else:
            current = await self.client.reply(
                event.topic_id, detail.post_id, detail.reply_id
            )
            parent_id = (
                reply_message_id(detail.post_id, current.parent_reply_id)
                if current.parent_reply_id
                else post_message_id(detail.post_id)
            )
            if current.parent_reply_id:
                try:
                    parent = await self.client.reply(
                        event.topic_id, detail.post_id, current.parent_reply_id
                    )
                    await self._content(
                        address, parent, parent_id, event, name=name, quiet=True
                    )
                except TopicApiError as error:
                    if error.transient:
                        raise
                    await self._publish(
                        "channel.topic.reference_unavailable",
                        {"msgid": event.msgid},
                        AuditError(
                            kind=f"wecom_topic_{error.code}", message=str(error)
                        ),
                    )
        await self._content(
            address,
            current,
            provider_id,
            event,
            name=name,
            parent_id=parent_id,
            mention=native.topic_at_bot is not None,
            modified=change_type == 2,
        )

    async def _content(
        self,
        address: TopicAddress,
        content: Content,
        provider_id: str,
        event: TopicEvent,
        *,
        name: str,
        parent_id: str | None = None,
        quiet: bool = False,
        mention: bool = False,
        modified: bool = False,
    ) -> None:
        own = content.creator.type == 1 and content.creator.id == self._bot_id
        if not quiet and own:
            return
        body = content.content.replace("${inline_mention_all}", "@all")
        for item in content.inline_mention_list:
            body = body.replace(
                "${inline_mention:" + item.placeholder + "}", "@" + item.member.id + " "
            )
            mention |= item.member.type == 1 and item.member.id == self._bot_id
        attachments: list[InboundAttachment] = []
        for item in content.inline_file_list:
            body = body.replace(
                "${inline_file:" + item.placeholder + "}", f"[File: {item.file_name}]"
            )
            attachments.append(
                await self._download(item, name=item.file_name, kind="file")
            )
        for index, image in enumerate(content.image_list):
            attachments.append(
                await self._download(image, name=f"image-{index + 1}.jpg", kind="image")
            )
        edit_version: str | None = None
        occurred_at_s = content.create_time
        if content.is_recalled:
            body = "[Content recalled]"
            if modified:
                provider_id += ":event:" + event.msgid
        elif modified:
            occurred_at_s = content.modify_time or event.create_time
            edit_version = str(occurred_at_s)
        await self._emit(
            topic_message(
                address,
                provider_id=provider_id,
                body=body,
                creator=content.creator,
                occurred_at_s=occurred_at_s,
                name=name,
                attachments=tuple(attachments),
                parent_id=parent_id,
                mentions_agent=mention and not quiet,
                notifies_runtime=not quiet and not content.is_recalled,
                approved=await self._approved(address.topic_id),
                edit_version=edit_version,
            )
        )

    async def _download(
        self, item: Download, *, name: str, kind: str
    ) -> InboundAttachment:
        media_type = await self.client.media_type(item) if kind == "image" else None
        if media_type:
            suffix = mimetypes.guess_extension(media_type.split(";", 1)[0])
            if suffix:
                name = name.rsplit(".", 1)[0] + suffix
        return InboundAttachment(
            name=name,
            kind=kind,
            media_type=media_type,
            source=aclosing(self.client.download(item)),
        )

    async def prepare(
        self, request: ChannelSendRequest, address: TopicAddress
    ) -> tuple[ChannelSendPart, ...]:
        parent_reply_id: str | None = None
        reference = request.target.reply_to_message_id
        if reference is not None:
            parts = reference.removeprefix("context:").split(":")
            if parts[0] in {"post", "reply"}:
                post_id = unquote(parts[1])
                if address.post_id is not None and address.post_id != post_id:
                    raise ChannelRejectedError(
                        "WeCom reply reference belongs to another post", phase="prepare"
                    )
                address = TopicAddress(address.bot_id, address.topic_id, post_id)
                if parts[0] == "reply":
                    parent_reply_id = unquote(parts[2])
        try:
            # Reserve room for expanded mention placeholders and attachment markers.
            batches = (
                list(
                    split_markdown(
                        request.body, limit=_CONTENT_BYTES - 1024, measure=utf8_bytes
                    )
                )
                if request.body.strip()
                else [""]
            )
            attachments = await asyncio.to_thread(
                prepare_attachments, self._workspace, request.attachments
            )
        except (OSError, ValueError) as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_message"
            ) from error
        if (
            sum(item.media_type == "image" for item in attachments) > 8
            or sum(item.media_type != "image" for item in attachments) > 8
        ):
            raise ChannelRejectedError(
                "WeCom supports at most eight files and eight images per message",
                phase="prepare",
            )
        if not request.body.strip() and not attachments:
            raise ChannelRejectedError(
                "WeCom outbound message must not be empty", phase="prepare"
            )
        state = _TopicSend(
            address=address, parent_reply_id=parent_reply_id, attachments=attachments
        )
        operations = [
            ChannelSendPart(send=partial(self._upload, state, index), message=None)
            for index in range(len(attachments))
        ]
        for index, batch in enumerate(batches):
            operations.append(
                ChannelSendPart(
                    send=partial(
                        self._send_part,
                        request,
                        state,
                        batch,
                        index == len(batches) - 1,
                    ),
                    message=request.model_copy(
                        update={
                            "body": batch,
                            "attachments": request.attachments
                            if index == len(batches) - 1
                            else (),
                        }
                    ),
                )
            )
        return tuple(operations)

    async def _upload(
        self, state: _TopicSend, index: int, attempt: ChannelSendAttempt
    ) -> None:
        del attempt
        async with self._sending():
            attachment = state.attachments[index]
            try:
                media_id = await self._upload_media(attachment)
            except TopicApiError as error:
                raise channel_error(error, phase="upload") from error
            except (OSError, ValueError) as error:
                raise ChannelRejectedError(
                    str(error), phase="upload", code="invalid_attachment"
                ) from error
            if attachment.media_type == "image":
                state.images.append({"media_id": media_id})
            else:
                state.files.append(
                    {"placeholder": f"file_{index}", "media_id": media_id}
                )

    async def _upload_media(self, attachment: PreparedAttachment) -> str:
        media_type = "image" if attachment.media_type == "image" else "file"
        result = await self.client.call(
            "upload_media_init",
            {
                "type": media_type,
                "filename": attachment.descriptor.name,
                "total_size": attachment.size_bytes,
                "total_chunks": attachment.total_chunks,
                "md5": attachment.md5,
            },
            UploadStarted,
        )
        reader = await asyncio.to_thread(AttachmentReader.open, attachment)
        try:
            for index in range(attachment.total_chunks):
                chunk = await asyncio.to_thread(reader.read_chunk)
                await self.client.call(
                    "upload_media_chunk",
                    {
                        "upload_id": result.upload_id,
                        "chunk_index": index,
                        "base64_data": base64.b64encode(chunk).decode(),
                    },
                    ApiResponse,
                )
        finally:
            await asyncio.to_thread(reader.close)
        finished = await self.client.call(
            "upload_media_finish", {"upload_id": result.upload_id}, UploadFinished
        )
        return finished.media_id

    async def _send_part(
        self,
        request: ChannelSendRequest,
        state: _TopicSend,
        batch: str,
        last: bool,
        attempt: ChannelSendAttempt,
    ) -> ChannelDeliveryReceipt:
        del attempt
        async with self._sending():
            if last:
                for item in state.files:
                    batch += "\n${inline_file:" + str(item["placeholder"]) + "}"
            if not batch.strip() and last and state.images:
                batch = "\n".join(
                    attachment.descriptor.name
                    for attachment in state.attachments
                    if attachment.media_type == "image"
                )
            try:
                if state.members is None:
                    state.members = (
                        await self.client.members(state.address.topic_id)
                        if "@" in request.body
                        else ()
                    )
                body, mentions = mention_content(batch, state.members)
                if utf8_bytes(body) > _CONTENT_BYTES:
                    raise ValueError("WeCom expanded Markdown exceeds 20480 bytes")
                payload: dict[str, object] = {
                    "topic_id": state.address.topic_id,
                    "content": body,
                }
                if mentions:
                    payload["inline_mention_list"] = mentions
                if last:
                    if state.files:
                        payload["inline_file_list"] = [
                            dict(item) for item in state.files
                        ]
                    if state.images:
                        payload["image_list"] = list(state.images)
                if state.address.post_id is None:
                    result = await self.client.call(
                        "topic/send_post", payload, SentPost
                    )
                    state.address = TopicAddress(
                        state.address.bot_id, state.address.topic_id, result.post_id
                    )
                    provider_id = post_message_id(result.post_id)
                    await self._expose_post(state, provider_id, batch, last)
                else:
                    payload["post_id"] = state.address.post_id
                    if state.parent_reply_id is not None:
                        payload["parent_reply_id"] = state.parent_reply_id
                    result = await self.client.call(
                        "topic/send_reply", payload, SentReply
                    )
                    provider_id = reply_message_id(
                        state.address.post_id, result.reply_id
                    )
            except TopicApiError as error:
                raise channel_error(error, phase="send") from error
            except ValueError as error:
                raise ChannelRejectedError(
                    str(error), phase="send", code="invalid_message"
                ) from error
            return ChannelDeliveryReceipt(
                status="confirmed",
                provider_message_id=provider_id,
                provider_thread_id=state.address.provider_thread_id,
            )

    async def _expose_post(
        self, state: _TopicSend, provider_id: str, batch: str, last: bool
    ) -> None:
        """Expose a newly created post through the existing group target."""

        address = state.address
        assert address.post_id is not None
        body = batch
        for index, attachment in enumerate(state.attachments):
            body = body.replace(
                "${inline_file:file_" + str(index) + "}",
                f"[File: {attachment.descriptor.name}]",
            )
        await self._emit(
            topic_message(
                address,
                provider_id=provider_id,
                body=body,
                creator=Member(type=1, aibotid=self._bot_id),
                occurred_at_s=time_ns() // 1_000_000_000,
                name=self._names.get(address.topic_id, address.topic_id)
                + " · "
                + address.post_id,
                notifies_runtime=False,
                mentions_agent=True,
                approved=await self._approved(address.topic_id),
                attachments=tuple(
                    InboundAttachment(
                        name=attachment.descriptor.name,
                        kind="image" if attachment.media_type == "image" else "file",
                        media_type=attachment.descriptor.media_type,
                        source=aclosing(_read(attachment)),
                    )
                    for attachment in state.attachments
                )
                if last
                else (),
            )
        )

    @asynccontextmanager
    async def _sending(self) -> AsyncIterator[None]:
        """Hold the send lock and let stop wait for the operation in flight."""

        if self._stopping:
            raise ChannelConnectionError(
                "WeCom channel is stopping", phase="connect", delivery="not_sent"
            )
        done = asyncio.get_running_loop().create_future()
        self._in_flight.add(done)
        try:
            async with self._send_lock:
                yield
        finally:
            self._in_flight.discard(done)
            done.set_result(None)


async def _read(attachment: PreparedAttachment) -> AsyncGenerator[bytes]:
    reader = await asyncio.to_thread(AttachmentReader.open, attachment)
    try:
        while chunk := await asyncio.to_thread(reader.read_chunk):
            yield chunk
    finally:
        await asyncio.to_thread(reader.close)


__all__ = ["TopicConversations", "channel_error", "mention_content", "topic_message"]
