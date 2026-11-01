"""Connect a WeCom intelligent robot over its single long connection."""

# Existing WeCom request and lifecycle budgets are retained.
# ruff: noqa: ASYNC109
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import random
from collections.abc import AsyncGenerator, AsyncIterator, Coroutine, Mapping
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, field
from email.message import Message as EmailMessage
from functools import partial
from time import time_ns
from typing import Literal
from urllib.parse import unquote, urlsplit
from uuid import NAMESPACE_URL, uuid4, uuid5

import aiohttp
from bazaar_contrib_audit import Audit, AuditContext, AuditError
from bazaar_contrib_channel import (
    ChannelApprovalRequest,
    ChannelConnectionError,
    ChannelDeliveryReceipt,
    ChannelIdentity,
    ChannelInput,
    ChannelRejectedError,
    ChannelSendAttempt,
    ChannelSendPart,
    ChannelSendPhase,
    ChannelSendRequest,
    ChannelTarget,
    ChannelTimeoutError,
    DmAddress,
    DmRequest,
    InboundAttachment,
    InboundMessage,
    SenderIdentity,
)
from bazaar_contrib_collaboration import ContactReviewed
from bazaar_contrib_harness import ApprovalResult
from bazaar_contrib_messages import Messages
from bazaar_contrib_timer import Timer, wait_for
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pydantic import JsonValue

from .identity import TopicAddress, conversation_thread_id, parse_provider_thread_id
from .markdown import split_markdown, utf8_bytes
from .outbound import (
    AttachmentReader,
    PreparedAttachment,
    encode_request,
    prepare_attachments,
    visible_message_body,
)
from .topic import TopicConversations

_STOP = object()
_MAX_MEDIA_BYTES = 25 * 1024 * 1024
_MAX_MARKDOWN_BYTES = 20_480
_MAX_ERROR_DETAIL = 256
_MEDIA_TIMEOUT = aiohttp.ClientTimeout(total=60, connect=10)
_CARD_UPDATE_TIMEOUT_SECONDS = 5.0
_APPROVE_KEY = "bcn_approve"
_REJECT_KEY = "bcn_reject"
_RESOLVED_KEY = "bcn_resolved"

TEXT = {
    "approval.action.command_execution": "command execution",
    "approval.action.file_change": "file change",
    "approval.action.permissions": "permissions",
    "approval.button.approve": "✅ Approve",
    "approval.button.reject": "❎ Reject",
    "approval.card.status.approved": "Approved",
    "approval.card.status.rejected": "Rejected",
    "approval.prompt.title": "Approval required",
}


@dataclass(frozen=True, slots=True)
class _InboundRoute:
    provider_message_id: str
    sender_id: str
    message_type: str
    conversation: str
    target_kind: Literal["dm", "group"]
    mentions_agent: bool


@dataclass(frozen=True, slots=True)
class _InboundContent:
    body: str
    attachments: tuple[InboundAttachment, ...]
    fingerprint: str


@dataclass(slots=True)
class _PendingApproval:
    request: ChannelApprovalRequest
    task_id: str
    future: asyncio.Future[ApprovalResult]


@dataclass(slots=True)
class _Uploads:
    """Media ids the upload operations of one request leave for its messages."""

    media_ids: dict[int, str] = field(default_factory=dict)


class WeComChannel:
    def __init__(
        self,
        input: ChannelInput,
        timer: Timer,
        audit: Audit,
        messages: Messages,
        group: asyncio.TaskGroup,
        *,
        bot_id: str,
        secret: str,
        websocket_url: str,
    ) -> None:
        self._input = input
        self._timer = timer
        self._audit = audit
        self._group = group
        self._bot_id = bot_id
        self._secret = secret
        self._websocket_url = websocket_url
        self._inbound: asyncio.Queue[InboundMessage | object] = asyncio.Queue()
        self._ready = asyncio.Event()
        self._stopping = asyncio.Event()
        self._startup_finished = asyncio.Event()
        self._startup_error: Exception | None = None
        self._runner: asyncio.Task[None] | None = None
        self._connection: aiohttp.ClientWebSocketResponse | None = None
        self._send_lock = asyncio.Lock()
        self._pending_acks: dict[str, asyncio.Future[Mapping[str, object]]] = {}
        self._pending_approvals: dict[str, _PendingApproval] = {}
        self._approval_task_ids_by_request: dict[str, str] = {}
        self._approval_card_update_tasks: set[asyncio.Task[None]] = set()
        self._degraded = False
        self._heartbeat_ack = ""
        self._network_attempt = 0
        self._auth_attempt = 0
        self._frames: asyncio.Queue[
            tuple[aiohttp.ClientWebSocketResponse, Mapping[str, object]]
        ] = asyncio.Queue()
        self._ingress: asyncio.Task[None] | None = None
        self._topics = TopicConversations(
            input.workspace,
            timer,
            messages,
            agent_id=input.agent_id,
            bot_id=bot_id,
            secret=secret,
            emit=self._inbound.put,
            publish=self._publish,
        )

    @property
    def name(self) -> str:
        return "wecom"

    async def _publish(
        self,
        event_name: str,
        payload: JsonValue,
        error: AuditError | None = None,
    ) -> None:
        await self._audit.publish(
            event_name,
            context=AuditContext(agent_id=self._input.agent_id, channel=self.name),
            payload=payload,
            error=error,
        )

    def get_identity(self) -> ChannelIdentity:
        return ChannelIdentity(id=self._bot_id)

    def backfill_provider_thread_id(
        self, provider_thread_id: str, *, channel: str, channel_identity: str
    ) -> str:
        del channel, channel_identity
        return conversation_thread_id(self._bot_id, provider_thread_id)

    def dm_address(self, request: DmRequest) -> DmAddress | None:
        if request.sender_id is None:
            return None
        identity = f"wecom:bot:{self._bot_id}:dm:{request.sender_id}"
        return DmAddress(
            channel_session_id=str(uuid5(NAMESPACE_URL, identity)),
            thread_id=str(uuid5(NAMESPACE_URL, f"bcn:{identity}")),
            provider_thread_id=conversation_thread_id(self._bot_id, request.sender_id),
        )

    def forget_review(self, event: ContactReviewed) -> None:
        self._topics.forget_review(event)

    async def start(self, *, timeout: float) -> None:
        if self._runner is not None:
            return
        self._stopping.clear()
        self._startup_finished.clear()
        self._startup_error = None
        self._topics.start(self._group)
        self._ingress = self._group.create_task(
            self._process_frames(), name="bcn-wecom-ingress"
        )
        self._runner = self._group.create_task(self._run(), name="bcn-wecom-channel")
        try:
            await wait_for(
                self._timer,
                self._startup_finished.wait(),
                math.ceil(max(0.0, timeout) * 1000),
            )
        except BaseException:
            self._stopping.set()
            self._runner.cancel()
            self._ingress.cancel()
            await asyncio.gather(self._runner, self._ingress, return_exceptions=True)
            self._runner = None
            self._ingress = None
            await self._topics.stop(timeout=timeout)
            raise
        if self._startup_error is not None:
            self._ingress.cancel()
            await asyncio.gather(self._ingress, return_exceptions=True)
            self._ingress = None
            await self._topics.stop(timeout=timeout)
            raise self._startup_error

    async def stop(self, *, timeout: float) -> None:
        self._stopping.set()
        deadline = asyncio.get_running_loop().time() + timeout
        shutdown = self._group.create_task(
            self._topics.stop(timeout=timeout), name="bcn-wecom-topics-stop"
        )
        try:
            for pending in tuple(self._pending_approvals.values()):
                if not pending.future.done():
                    pending.future.set_result(
                        ApprovalResult(
                            request_id=pending.request.approval.request_id,
                            decision="rejected",
                            reason="channel_stopped",
                        )
                    )
            self._pending_approvals.clear()
            self._approval_task_ids_by_request.clear()
            await self._cancel_approval_card_updates()
            try:
                await wait_for(
                    self._timer, self._send_lock.acquire(), self._remaining_ms(deadline)
                )
            except TimeoutError:
                pass
            else:
                self._send_lock.release()
            connection = self._connection
            if connection is not None:
                await connection.close()
            runner = self._runner
            if runner is not None:
                try:
                    await wait_for(
                        self._timer,
                        asyncio.shield(runner),
                        self._remaining_ms(deadline),
                    )
                except TimeoutError:
                    runner.cancel()
                    await asyncio.gather(runner, return_exceptions=True)
        finally:
            self._runner = None
            ingress = self._ingress
            self._ingress = None
            if ingress is not None:
                ingress.cancel()
                await asyncio.gather(ingress, return_exceptions=True)
            await shutdown
            await self._inbound.put(_STOP)

    async def receive(self) -> AsyncIterator[InboundMessage]:
        while True:
            item = await self._inbound.get()
            if item is _STOP:
                return
            if not isinstance(item, InboundMessage):
                raise TypeError("WeCom inbound queue contained an invalid message")
            yield item

    async def prepare_send(
        self, request: ChannelSendRequest
    ) -> tuple[ChannelSendPart, ...]:
        try:
            address = TopicAddress.parse(request.target.thread_id)
        except ValueError as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_topic_target"
            ) from error
        if address is not None:
            if address.bot_id != self._bot_id:
                raise ChannelRejectedError(
                    "WeCom topic belongs to another bot",
                    phase="prepare",
                    code="invalid_topic_target",
                )
            return await self._topics.prepare(request, address)
        try:
            _, target_id = parse_provider_thread_id(request.target.thread_id)
        except ValueError as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_target"
            ) from error
        try:
            batches = (
                split_markdown(
                    request.body, limit=_MAX_MARKDOWN_BYTES, measure=utf8_bytes
                )
                if request.body.strip()
                else ()
            )
        except ValueError as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_markdown"
            ) from error
        try:
            attachments = await asyncio.to_thread(
                prepare_attachments, self._input.workspace, request.attachments
            )
        except (OSError, ValueError) as error:
            raise ChannelRejectedError(
                str(error), phase="prepare", code="invalid_attachment"
            ) from error
        if not batches and not attachments:
            raise ChannelRejectedError(
                "WeCom outbound message must not be empty",
                phase="prepare",
                code="empty_message",
            )
        kind = request.target.kind
        operations = [
            ChannelSendPart(
                send=partial(self._send_markdown, target_id, kind, batch),
                message=request.model_copy(update={"body": batch, "attachments": ()}),
            )
            for batch in batches
        ]
        uploads = _Uploads()
        for index, attachment in enumerate(attachments):
            operations.append(
                ChannelSendPart(
                    send=partial(self._upload_attachment, uploads, index, attachment),
                    message=None,
                )
            )
            operations.append(
                ChannelSendPart(
                    send=partial(
                        self._send_media, uploads, index, attachment, target_id, kind
                    ),
                    message=request.model_copy(
                        update={
                            "body": "",
                            "attachments": (request.attachments[index],),
                        }
                    ),
                )
            )
        return tuple(operations)

    async def _send_markdown(
        self,
        target_id: str,
        target_kind: Literal["dm", "group"],
        content: str,
        attempt: ChannelSendAttempt,
    ) -> ChannelDeliveryReceipt:
        async with self._sending() as connection:
            request_id, _ = await self._request(
                connection,
                command="aibot_send_msg",
                body=visible_message_body(
                    target_id=target_id,
                    target_kind=target_kind,
                    message_type="markdown",
                    content={"content": content},
                ),
                timeout=attempt.timeout,
                phase="send",
            )
        return self._receipt(request_id, part_type="markdown")

    async def _send_media(
        self,
        uploads: _Uploads,
        index: int,
        attachment: PreparedAttachment,
        target_id: str,
        target_kind: Literal["dm", "group"],
        attempt: ChannelSendAttempt,
    ) -> ChannelDeliveryReceipt:
        async with self._sending() as connection:
            request_id, _ = await self._request(
                connection,
                command="aibot_send_msg",
                body=visible_message_body(
                    target_id=target_id,
                    target_kind=target_kind,
                    message_type=attachment.media_type,
                    content={"media_id": uploads.media_ids[index]},
                ),
                timeout=attempt.timeout,
                phase="send",
            )
        return self._receipt(
            request_id,
            part_type=attachment.media_type,
            attachment_name=attachment.descriptor.name,
        )

    async def _upload_attachment(
        self,
        uploads: _Uploads,
        index: int,
        attachment: PreparedAttachment,
        attempt: ChannelSendAttempt,
    ) -> None:
        deadline = asyncio.get_running_loop().time() + attempt.timeout
        async with self._sending() as connection:
            _, init = await self._request(
                connection,
                command="aibot_upload_media_init",
                body={
                    "type": attachment.media_type,
                    "filename": attachment.descriptor.name,
                    "total_size": attachment.size_bytes,
                    "total_chunks": attachment.total_chunks,
                    "md5": attachment.md5,
                },
                timeout=self._remaining(deadline),
                phase="upload",
            )
            upload_id = init.get("upload_id")
            if not isinstance(upload_id, str) or not upload_id:
                raise ChannelRejectedError(
                    "WeCom upload initialization omitted upload_id",
                    phase="upload",
                    code="invalid_upload_ack",
                )
            try:
                reader = await asyncio.to_thread(AttachmentReader.open, attachment)
            except (OSError, ValueError) as error:
                raise ChannelRejectedError(
                    str(error), phase="upload", code="attachment_read_failed"
                ) from error
            try:
                for chunk_index in range(attachment.total_chunks):
                    try:
                        chunk = await asyncio.to_thread(reader.read_chunk)
                    except OSError as error:
                        raise ChannelRejectedError(
                            str(error), phase="upload", code="attachment_read_failed"
                        ) from error
                    if not chunk:
                        raise ChannelRejectedError(
                            "WeCom attachment ended before all chunks were read",
                            phase="upload",
                            code="attachment_read_failed",
                        )
                    await self._request(
                        connection,
                        command="aibot_upload_media_chunk",
                        body={
                            "upload_id": upload_id,
                            "chunk_index": chunk_index,
                            "base64_data": base64.b64encode(chunk).decode("ascii"),
                        },
                        timeout=self._remaining(deadline),
                        phase="upload",
                    )
            finally:
                await asyncio.to_thread(reader.close)
            _, finish = await self._request(
                connection,
                command="aibot_upload_media_finish",
                body={"upload_id": upload_id},
                timeout=self._remaining(deadline),
                phase="upload",
            )
        media_id = finish.get("media_id")
        if not isinstance(media_id, str) or not media_id:
            raise ChannelRejectedError(
                "WeCom upload completion omitted media_id",
                phase="upload",
                code="invalid_upload_ack",
            )
        uploads.media_ids[index] = media_id

    @asynccontextmanager
    async def _sending(self) -> AsyncIterator[aiohttp.ClientWebSocketResponse]:
        """Hold the send lock on a live socket for one operation."""

        async with self._send_lock:
            connection = self._connection
            if (
                self._stopping.is_set()
                or connection is None
                or connection.closed
                or not self._ready.is_set()
            ):
                raise ChannelConnectionError(
                    "WeCom connection is unavailable",
                    phase="connect",
                    delivery="not_sent",
                )
            yield connection

    def _receipt(self, request_id: str, **receipt: JsonValue) -> ChannelDeliveryReceipt:
        return ChannelDeliveryReceipt(
            status="confirmed",
            provider_receipt_ref=request_id,
            receipt={"provider_request_id": request_id, **receipt},
        )

    async def _request(
        self,
        connection: aiohttp.ClientWebSocketResponse,
        *,
        command: str,
        body: Mapping[str, object],
        timeout: float,
        phase: ChannelSendPhase,
    ) -> tuple[str, Mapping[str, object]]:
        """Send one request and return its acknowledged body."""

        request_id = f"{command}-{uuid4()}"
        future: asyncio.Future[Mapping[str, object]] = (
            asyncio.get_running_loop().create_future()
        )
        self._pending_acks[request_id] = future
        try:
            if timeout <= 0:
                raise TimeoutError
            deadline = asyncio.get_running_loop().time() + timeout
            await wait_for(
                self._timer,
                connection.send_str(encode_request(command, request_id, body)),
                self._remaining_ms(deadline),
            )
            frame = await wait_for(self._timer, future, self._remaining_ms(deadline))
        except TimeoutError:
            raise ChannelTimeoutError(
                f"WeCom {command} acknowledgement timed out",
                phase=phase,
                delivery="not_sent",
            ) from None
        except (aiohttp.ClientError, ConnectionError, OSError) as error:
            raise ChannelConnectionError(
                f"WeCom {command} transport failed: {self._describe(error)}",
                phase=phase,
                delivery="not_sent",
            ) from None
        finally:
            self._pending_acks.pop(request_id, None)
            future.cancel()
        error_code = frame.get("errcode")
        if not isinstance(error_code, int) or isinstance(error_code, bool):
            raise ChannelRejectedError(
                f"WeCom {command} returned an invalid acknowledgement",
                phase=phase,
                code="invalid_ack",
            )
        if error_code != 0:
            error_message = frame.get("errmsg")
            raise ChannelRejectedError(
                f"WeCom {command} rejected ({error_code})"
                + (
                    f": {error_message[:_MAX_ERROR_DETAIL]}"
                    if isinstance(error_message, str) and error_message
                    else ""
                ),
                phase=phase,
                code=str(error_code),
            )
        response_body = frame.get("body")
        return request_id, response_body if isinstance(response_body, Mapping) else {}

    def _describe(self, error: BaseException) -> str:
        message = str(error).replace(self._secret, "<redacted>")[:_MAX_ERROR_DETAIL]
        return f"{type(error).__name__}: {message}" if message else type(error).__name__

    async def request_approval(
        self, request: ChannelApprovalRequest, *, timeout: float
    ) -> ApprovalResult:
        if TopicAddress.parse(request.target.thread_id) is not None:
            return ApprovalResult(
                request_id=request.approval.request_id,
                decision="rejected",
                reason="WeCom native topics do not support approval cards",
            )
        request_id = request.approval.request_id
        if timeout <= 0:
            raise TimeoutError("WeCom approval card delivery timed out")
        if request_id in self._approval_task_ids_by_request:
            raise ValueError("WeCom approval request is already pending")
        pending = _PendingApproval(
            request=request,
            task_id=uuid4().hex,
            future=asyncio.get_running_loop().create_future(),
        )
        self._pending_approvals[pending.task_id] = pending
        self._approval_task_ids_by_request[request_id] = pending.task_id
        try:
            deadline = asyncio.get_running_loop().time() + timeout
            try:
                await wait_for(
                    self._timer, self._send_lock.acquire(), self._remaining_ms(deadline)
                )
            except TimeoutError as error:
                raise TimeoutError(
                    "WeCom approval card delivery timed out waiting for the send lock"
                ) from error
            try:
                connection = self._connection
                if connection is None or connection.closed or not self._ready.is_set():
                    raise RuntimeError("WeCom channel is not ready for approvals")
                await self._request(
                    connection,
                    command="aibot_send_msg",
                    body=visible_message_body(
                        target_id=parse_provider_thread_id(request.target.thread_id)[1],
                        target_kind=request.target.kind,
                        message_type="template_card",
                        content=self._approval_card(pending),
                    ),
                    timeout=self._remaining(deadline),
                    phase="send",
                )
            finally:
                self._send_lock.release()
            return await pending.future
        finally:
            self._pending_approvals.pop(pending.task_id, None)
            if self._approval_task_ids_by_request.get(request_id) == pending.task_id:
                self._approval_task_ids_by_request.pop(request_id, None)

    async def _run(self) -> None:
        while not self._stopping.is_set() and not self._degraded:
            try:
                await self._connect_once()
            except _AuthenticationError as error:
                self._auth_attempt += 1
                auth_attempt = self._auth_attempt
                if not self._startup_finished.is_set() or auth_attempt >= 3:
                    await self._publish(
                        "channel.connection.failed",
                        {"attempt": auth_attempt},
                        AuditError(kind="wecom_authentication", message=str(error)),
                    )
                    self._startup_error = error
                    self._startup_finished.set()
                    return
                await self._backoff(auth_attempt, error)
            except Exception as error:  # noqa: BLE001
                self._network_attempt += 1
                network_attempt = self._network_attempt
                if not self._startup_finished.is_set() and network_attempt >= 6:
                    await self._publish(
                        "channel.connection.failed",
                        {"attempt": network_attempt},
                        AuditError(
                            kind=type(error).__name__, message=self._describe(error)
                        ),
                    )
                    self._startup_error = RuntimeError(
                        f"WeCom connection failed: {self._describe(error)}"
                    )
                    self._startup_finished.set()
                    return
                await self._backoff(network_attempt, error)

    async def _authenticate(self, connection: aiohttp.ClientWebSocketResponse) -> None:
        """Subscribe as this bot and confirm the provider accepted it."""

        subscribe_id = f"aibot_subscribe-{uuid4()}"
        await connection.send_str(
            json.dumps(
                {
                    "cmd": "aibot_subscribe",
                    "headers": {"req_id": subscribe_id},
                    "body": {"bot_id": self._bot_id, "secret": self._secret},
                },
                separators=(",", ":"),
            )
        )
        response = await wait_for(self._timer, connection.receive(), 10_000)
        frame = self._frame(self._message_data(response))
        if self._request_id(frame) != subscribe_id or frame.get("errcode") != 0:
            raise _AuthenticationError(
                f"WeCom authentication failed ({frame.get('errcode')})"
            )

    async def _read_frames(self, connection: aiohttp.ClientWebSocketResponse) -> None:
        """Read frames until the socket closes or the provider says it is done."""

        async for response in connection:
            if response.type in {
                aiohttp.WSMsgType.CLOSE,
                aiohttp.WSMsgType.CLOSED,
            }:
                break
            if response.type is aiohttp.WSMsgType.ERROR:
                raise ConnectionError("WeCom WebSocket reader failed")
            frame = self._frame(self._message_data(response))
            if self._is_disconnected_event(frame):
                self._degraded = True
                await self._publish(
                    "channel.connection.failed",
                    {"reason": "disconnected_event"},
                    AuditError(
                        kind="wecom_disconnected_event",
                        message="Another connection took over this WeCom bot",
                    ),
                )
                await connection.close()
                return
            request_id = self._request_id(frame)
            if request_id.startswith("ping-") and frame.get("errcode") == 0:
                self._heartbeat_ack = request_id
                continue
            pending = self._pending_acks.get(request_id)
            if pending is not None and not pending.done():
                pending.set_result(frame)
                continue
            if frame.get("cmd") in {
                "aibot_msg_callback",
                "aibot_event_callback",
                "aibot_topic_event_callback",
            }:
                self._frames.put_nowait((connection, frame))

    async def _process_frames(self) -> None:
        """Handle callbacks in arrival order, away from the socket's control traffic."""

        while True:
            connection, frame = await self._frames.get()
            try:
                await self._receive_message(connection, frame)
            except Exception as error:  # noqa: BLE001
                # One failed callback must not stop ingress or reveal its content.
                await self._publish(
                    "channel.message.filtered",
                    {"reason": "processing_failed"},
                    AuditError(kind=type(error).__name__, message=type(error).__name__),
                )

    async def _release_connection(self, heartbeat: asyncio.Task[None]) -> None:
        """Let go of a connection, failing whatever was still waiting on it."""

        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)
        await self._cancel_approval_card_updates()
        for pending in self._pending_acks.values():
            if not pending.done():
                pending.set_exception(
                    ConnectionError("WeCom connection closed before ack")
                )
        self._pending_acks.clear()
        self._connection = None
        self._ready.clear()

    async def _connect_once(self) -> None:
        timeout = aiohttp.ClientTimeout(total=None, connect=10)
        async with (
            aiohttp.ClientSession(timeout=timeout) as session,
            session.ws_connect(
                self._websocket_url,
                heartbeat=20,
                autoclose=True,
                autoping=True,
                max_msg_size=2 * 1024 * 1024,
            ) as connection,
        ):
            self._connection = connection
            await self._authenticate(connection)
            self._network_attempt = 0
            self._auth_attempt = 0
            self._ready.set()
            await self._publish("channel.connection.subscribed", {})
            self._startup_finished.set()
            heartbeat = self._group.create_task(
                self._heartbeat(connection), name="bcn-wecom-heartbeat"
            )
            try:
                await self._read_frames(connection)
                if not self._stopping.is_set() and not self._degraded:
                    raise ConnectionError("WeCom WebSocket closed")
            finally:
                await self._release_connection(heartbeat)

    async def _heartbeat(self, connection: aiohttp.ClientWebSocketResponse) -> None:
        missed = 0
        last_request = ""
        while not self._stopping.is_set():
            await self._timer.create(30_000).wait()
            if last_request and self._heartbeat_ack != last_request:
                missed += 1
            else:
                missed = 0
            if missed >= 2:
                await connection.close(code=1011, message=b"heartbeat timeout")
                return
            last_request = f"ping-{uuid4()}"
            try:
                await connection.send_str(
                    json.dumps(
                        {"cmd": "ping", "headers": {"req_id": last_request}},
                        separators=(",", ":"),
                    )
                )
            except aiohttp.ClientError, ConnectionError, OSError:
                return

    def _route(self, body: Mapping[str, object]) -> _InboundRoute | str:
        """Read where a message belongs, or say why it cannot be taken."""

        provider_message_id = body.get("msgid")
        if not isinstance(provider_message_id, str) or not provider_message_id:
            return "missing_message_id"
        sender = body.get("from")
        sender_id = sender.get("userid") if isinstance(sender, dict) else None
        if not isinstance(sender_id, str) or not sender_id:
            return "missing_sender"
        message_type = body.get("msgtype")
        if not isinstance(message_type, str) or not message_type:
            return "missing_message_type"
        chat_type = body.get("chattype")
        target_kind: Literal["dm", "group"]
        if chat_type == "group":
            conversation = body.get("chatid")
            target_kind = "group"
            mentions_agent = True
        elif chat_type == "single":
            conversation = sender_id
            target_kind = "dm"
            mentions_agent = False
        else:
            return "unsupported_chat_type"
        if not isinstance(conversation, str) or not conversation:
            return "missing_conversation"
        return _InboundRoute(
            provider_message_id=provider_message_id,
            sender_id=sender_id,
            message_type=message_type,
            conversation=conversation,
            target_kind=target_kind,
            mentions_agent=mentions_agent,
        )

    async def _filter_message(
        self, reason: str, *, provider_message_id: object = None
    ) -> None:
        await self._publish(
            "channel.message.filtered",
            {
                "reason": reason,
                "provider_message_id": provider_message_id
                if isinstance(provider_message_id, str)
                else None,
            },
        )

    async def _queue_quoted_message(
        self,
        quote: object,
        *,
        provider_message_id: str,
        route: _InboundRoute,
        session_id: str,
        channel_session_id: str,
        received_at_ms: int,
    ) -> str | None:
        """Queue the message being quoted, so the reply has something to point at."""

        if not isinstance(quote, dict):
            return None
        quote_type = quote.get("msgtype")
        if not isinstance(quote_type, str) or not quote_type:
            await self._publish(
                "channel.message.reference_unresolved",
                {
                    "provider_message_id": provider_message_id,
                    "reason": "missing_message_type",
                },
            )
            return None
        quote_content = await self._content(quote, quote_type)
        conversation = route.conversation
        await self._inbound.put(
            InboundMessage(
                id=str(
                    uuid5(
                        NAMESPACE_URL,
                        f"bcn:wecom:{self._bot_id}:quoted-message:{conversation}:"
                        f"{quote_content.fingerprint}",
                    )
                ),
                thread_id=session_id,
                channel_session_id=channel_session_id,
                target=ChannelTarget(
                    kind=route.target_kind,
                    thread_id=conversation_thread_id(self._bot_id, conversation),
                ),
                provider_chat_id=conversation,
                provider_message_id=quote_content.fingerprint,
                received_at_ms=received_at_ms,
                sender=SenderIdentity(name="unknown", kind="unknown"),
                message_type=quote_type,
                body=quote_content.body,
                mentions_agent=False,
                notifies_runtime=False,
                attachments=quote_content.attachments,
                metadata={"sender_kind": "unknown"},
            )
        )
        return quote_content.fingerprint

    def _update_approval_card(
        self,
        connection: aiohttp.ClientWebSocketResponse,
        frame: Mapping[str, object],
        pending: _PendingApproval,
        decision: Literal["approved", "rejected"],
    ) -> None:
        """Redraw the card the decision came from, if its connection still stands."""

        request_id = self._request_id(frame)
        if connection.closed or not request_id:
            self._spawn_card_update(
                self._publish(
                    "channel.approval.edit_failed",
                    {"task_id": pending.task_id},
                    AuditError(
                        kind="ConnectionError",
                        message="WeCom connection is unavailable for the card update",
                    ),
                ),
                pending.task_id,
            )
            return
        self._spawn_card_update(
            self._send_approval_card_update(
                connection,
                encode_request(
                    "aibot_respond_update_msg",
                    request_id,
                    {
                        "response_type": "update_template_card",
                        "template_card": self._approval_card(
                            pending, decision=decision
                        ),
                    },
                ),
                task_id=pending.task_id,
            ),
            pending.task_id,
        )

    def _spawn_card_update(
        self, update: Coroutine[object, object, None], task_id: str
    ) -> None:
        task = self._group.create_task(
            update, name=f"bcn-wecom-approval-card-update-{task_id}"
        )
        self._approval_card_update_tasks.add(task)
        task.add_done_callback(self._approval_card_update_tasks.discard)

    async def _decide_approval(
        self,
        connection: aiohttp.ClientWebSocketResponse,
        frame: Mapping[str, object],
        event: Mapping[str, object],
    ) -> bool:
        """Settle a pending approval from a card press, or say why it did not."""

        card_event = event.get("template_card_event")
        task_id = card_event.get("task_id") if isinstance(card_event, dict) else None
        event_key = (
            card_event.get("event_key") if isinstance(card_event, dict) else None
        )
        pending = (
            self._pending_approvals.get(task_id) if isinstance(task_id, str) else None
        )
        if (
            pending is not None
            and not pending.future.done()
            and event_key in {_APPROVE_KEY, _REJECT_KEY}
        ):
            decision: Literal["approved", "rejected"] = (
                "approved" if event_key == _APPROVE_KEY else "rejected"
            )
            pending.future.set_result(
                ApprovalResult(
                    request_id=pending.request.approval.request_id, decision=decision
                )
            )
            self._update_approval_card(connection, frame, pending, decision)
            return True
        reason = (
            "invalid_template_card_event"
            if not isinstance(card_event, dict)
            else "missing_task_id"
            if not isinstance(task_id, str) or not task_id
            else "unknown_task_id"
            if pending is None
            else "already_resolved"
            if pending.future.done()
            else "invalid_event_key"
        )
        await self._publish("channel.approval.event_ignored", {"reason": reason})
        return False

    async def _receive_event(
        self, connection: aiohttp.ClientWebSocketResponse, frame: Mapping[str, object]
    ) -> None:
        """Route native topic events and ordinary chat approval decisions."""

        body = frame.get("body")
        event = body.get("event") if isinstance(body, dict) else None
        event_type = event.get("eventtype") if isinstance(event, dict) else None
        if isinstance(body, dict) and await self._topics.receive(body):
            return
        if event_type == "template_card_event" and isinstance(event, dict):
            await self._decide_approval(connection, frame, event)

    async def _receive_message(
        self, connection: aiohttp.ClientWebSocketResponse, frame: Mapping[str, object]
    ) -> None:
        command = frame.get("cmd")
        body = frame.get("body")
        if command in {"aibot_event_callback", "aibot_topic_event_callback"}:
            await self._receive_event(connection, frame)
            return
        if not isinstance(body, dict):
            await self._filter_message("invalid_body")
            return
        compatibility = body.get("compat_info")
        if isinstance(compatibility, dict) and compatibility.get("source") == "channel":
            await self._filter_message(
                "native_topic_projection", provider_message_id=body.get("msgid")
            )
            return
        route = self._route(body)
        if isinstance(route, str):
            await self._filter_message(route, provider_message_id=body.get("msgid"))
            return
        prefix = "dm" if route.target_kind == "dm" else "group"
        identity = f"wecom:bot:{self._bot_id}:{prefix}:{route.conversation}"
        channel_session_id = str(uuid5(NAMESPACE_URL, identity))
        session_id = str(uuid5(NAMESPACE_URL, f"bcn:{identity}"))
        received_at_ms = time_ns() // 1_000_000
        content = await self._content(body, route.message_type)
        metadata: dict[str, JsonValue] = {"sender_kind": "human"}
        create_time = body.get("create_time")
        if isinstance(create_time, int) and not isinstance(create_time, bool):
            metadata["provider_create_time"] = create_time
        reference = await self._queue_quoted_message(
            body.get("quote"),
            provider_message_id=route.provider_message_id,
            route=route,
            session_id=session_id,
            channel_session_id=channel_session_id,
            received_at_ms=received_at_ms,
        )
        if reference is not None:
            metadata["provider_reply_to_message_id"] = reference
        await self._inbound.put(
            InboundMessage(
                id=str(
                    uuid5(
                        NAMESPACE_URL,
                        f"bcn:wecom:{self._bot_id}:message:{route.provider_message_id}",
                    )
                ),
                thread_id=session_id,
                channel_session_id=channel_session_id,
                target=ChannelTarget(
                    kind=route.target_kind,
                    thread_id=conversation_thread_id(self._bot_id, route.conversation),
                    reply_to_message_id=reference,
                ),
                provider_chat_id=route.conversation,
                provider_message_id=route.provider_message_id,
                received_at_ms=received_at_ms,
                sender=SenderIdentity(id=route.sender_id),
                message_type=route.message_type,
                body=content.body,
                mentions_agent=route.mentions_agent,
                attachments=content.attachments,
                metadata=metadata,
            )
        )

    async def _send_approval_card_update(
        self,
        connection: aiohttp.ClientWebSocketResponse,
        payload: str,
        *,
        task_id: str,
    ) -> None:
        try:
            await wait_for(
                self._timer,
                connection.send_str(payload),
                math.ceil(_CARD_UPDATE_TIMEOUT_SECONDS * 1000),
            )
        except Exception as error:  # noqa: BLE001
            await self._publish(
                "channel.approval.edit_failed",
                {"task_id": task_id},
                AuditError(kind=type(error).__name__, message=self._describe(error)),
            )

    async def _cancel_approval_card_updates(self) -> None:
        tasks = tuple(self._approval_card_update_tasks)
        self._approval_card_update_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _approval_card(
        self,
        pending: _PendingApproval,
        *,
        decision: Literal["approved", "rejected"] | None = None,
    ) -> Mapping[str, object]:
        approval = pending.request.approval
        action = TEXT.get(
            f"approval.action.{approval.action}", approval.action.replace("_", " ")
        )
        if decision is None:
            buttons = [
                {"text": TEXT["approval.button.approve"], "key": _APPROVE_KEY},
                {"text": TEXT["approval.button.reject"], "key": _REJECT_KEY},
            ]
        else:
            buttons = [
                {
                    "text": TEXT[f"approval.card.status.{decision}"],
                    "key": _RESOLVED_KEY,
                }
            ]
        card: dict[str, object] = {
            "card_type": "button_interaction",
            "main_title": {"title": TEXT["approval.prompt.title"], "desc": action},
            "button_list": buttons,
            "task_id": pending.task_id,
        }
        details = approval.details
        description = "\n".join(
            f"{label}{details[key]}"
            for key, label in (
                ("reason", ""),
                ("command", "Command: "),
                ("grant_root", "Grant root: "),
                ("cwd", "Working directory: "),
                ("tool_name", "Tool: "),
                ("tool_input", "Input: "),
                ("permissions", "Permissions: "),
            )
            if details.get(key)
        ).strip()
        if len(description) > 4000:
            description = description[:3999] + "…"
        if description:
            card["sub_title_text"] = description
        return card

    async def _mixed_content(
        self, body: Mapping[str, object], message_type: str
    ) -> _InboundContent:
        """Read a mixed message as its text runs and the images between them."""

        mixed = body.get("mixed")
        items = mixed.get("msg_item") if isinstance(mixed, dict) else None
        texts: list[str] = []
        attachments: list[InboundAttachment] = []
        fingerprint_items: list[object] = []
        for item in items[:20] if isinstance(items, list) else ():
            if not isinstance(item, dict):
                continue
            if item.get("msgtype") == "text":
                text = item.get("text")
                content = text.get("content") if isinstance(text, dict) else None
                if isinstance(content, str):
                    texts.append(content)
                    fingerprint_items.append({"message_type": "text", "body": content})
            elif item.get("msgtype") == "image":
                image = item.get("image")
                if isinstance(image, dict):
                    attachment, fingerprint = await self._media(image, "image")
                    attachments.append(attachment)
                    fingerprint_items.append(fingerprint)
        return _InboundContent(
            body="\n".join(texts),
            attachments=tuple(attachments),
            fingerprint=self._content_fingerprint(
                {"message_type": message_type, "items": fingerprint_items}
            ),
        )

    async def _content(
        self, body: Mapping[str, object], message_type: str
    ) -> _InboundContent:
        if message_type in {"text", "voice"}:
            part = body.get(message_type)
            content = part.get("content") if isinstance(part, dict) else None
            text = content if isinstance(content, str) else ""
            return _InboundContent(
                body=text,
                attachments=(),
                fingerprint=self._content_fingerprint(
                    {"message_type": message_type, "body": text}
                ),
            )
        if message_type == "mixed":
            return await self._mixed_content(body, message_type)
        if message_type in {"image", "file", "video"}:
            media = body.get(message_type)
            if isinstance(media, dict):
                attachment, fingerprint = await self._media(media, message_type)
                return _InboundContent(
                    body="",
                    attachments=(attachment,),
                    fingerprint=self._content_fingerprint(
                        {"message_type": message_type, "media": fingerprint}
                    ),
                )
        unsupported = f"[unsupported WeCom message type: {message_type}]"
        return _InboundContent(
            body=unsupported,
            attachments=(),
            fingerprint=self._content_fingerprint(
                {"message_type": message_type, "body": unsupported}
            ),
        )

    async def _media(
        self, media: Mapping[str, object], kind: str
    ) -> tuple[InboundAttachment, object]:
        url = media.get("url")
        aes_key = media.get("aeskey")
        if not isinstance(url, str) or not url:
            return (
                InboundAttachment(
                    state="failed",
                    name=f"{kind}.bin",
                    kind=kind,
                    error="missing_media_url",
                ),
                {"kind": kind, "error": "missing_media_url"},
            )
        name, media_type = await _media_headers(url, kind)
        source = urlsplit(url)
        return (
            InboundAttachment(
                name=name,
                kind=kind,
                media_type=media_type,
                source=aclosing(
                    _download_media(
                        url, aes_key if isinstance(aes_key, str) and aes_key else None
                    )
                ),
            ),
            {"kind": kind, "source": f"{source.netloc}{source.path}"},
        )

    @staticmethod
    def _content_fingerprint(value: object) -> str:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _frame(raw: str | bytes) -> dict[str, object]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise TypeError("WeCom frame must be a JSON object")
        return payload

    @staticmethod
    def _message_data(message: aiohttp.WSMessage) -> str | bytes:
        if message.type not in {aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY}:
            raise TypeError("WeCom frame must be text or binary")
        if not isinstance(message.data, str | bytes):
            raise TypeError("WeCom frame payload is invalid")
        return message.data

    @staticmethod
    def _request_id(frame: Mapping[str, object]) -> str:
        headers = frame.get("headers")
        request_id = headers.get("req_id") if isinstance(headers, dict) else None
        return request_id if isinstance(request_id, str) else ""

    @staticmethod
    def _is_disconnected_event(frame: Mapping[str, object]) -> bool:
        if frame.get("cmd") != "aibot_event_callback":
            return False
        body = frame.get("body")
        event = body.get("event") if isinstance(body, dict) else None
        return (
            isinstance(event, dict) and event.get("eventtype") == "disconnected_event"
        )

    async def _backoff(self, attempt: int, error: Exception) -> None:
        delay = min(2 ** max(attempt - 1, 0), 30)
        delay += random.uniform(0, min(delay * 0.2, 1))
        retry_in_ms = math.ceil(delay * 1000)
        await self._publish(
            "channel.connection.failed",
            {"attempt": attempt, "retry_in_ms": retry_in_ms},
            AuditError(kind=type(error).__name__, message=self._describe(error)),
        )
        await self._timer.create(retry_in_ms).wait()

    @staticmethod
    def _remaining(deadline: float) -> float:
        return max(0.0, deadline - asyncio.get_running_loop().time())

    def _remaining_ms(self, deadline: float) -> int:
        return math.ceil(self._remaining(deadline) * 1000)


async def _media_headers(url: str, kind: str) -> tuple[str, str | None]:
    """Name a download from its response headers, leaving the body unread."""

    try:
        async with (
            aiohttp.ClientSession(timeout=_MEDIA_TIMEOUT) as session,
            session.get(url, allow_redirects=True) as response,
        ):
            response.raise_for_status()
            disposition = response.headers.get("Content-Disposition")
            media_type = response.headers.get("Content-Type")
    except aiohttp.ClientError, TimeoutError, OSError:
        return f"{kind}.bin", None
    if disposition:
        message = EmailMessage()
        message["Content-Disposition"] = disposition
        filename = message.get_filename()
        if filename:
            return unquote(filename), media_type
    return f"{kind}.bin", media_type


class MediaDecryptor:
    """Decrypt a WeCom media download in pieces, holding back the padded tail."""

    def __init__(self, aes_key: str) -> None:
        key = base64.b64decode(aes_key + "=" * (-len(aes_key) % 4))
        if len(key) != 32:
            raise ValueError("invalid encrypted media")
        self._decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
        self._held = b""
        self._size = 0

    def update(self, chunk: bytes) -> bytes:
        self._size += len(chunk)
        plain = self._held + self._decryptor.update(chunk)
        self._held = plain[-32:]
        return plain[:-32]

    def finalize(self) -> bytes:
        if not self._size or self._size % 16:
            raise ValueError("invalid encrypted media")
        padded = self._held + self._decryptor.finalize()
        padding = padded[-1]
        if (
            padding < 1
            or padding > 32
            or padded[-padding:] != bytes([padding]) * padding
        ):
            raise ValueError("invalid encrypted media padding")
        return padded[:-padding]


async def _download_media(url: str, aes_key: str | None) -> AsyncGenerator[bytes]:
    """Stream a media download, decrypting it when the callback gave its key."""

    decryptor = None if aes_key is None else MediaDecryptor(aes_key)
    async with (
        aiohttp.ClientSession(timeout=_MEDIA_TIMEOUT) as session,
        session.get(url, allow_redirects=True) as response,
    ):
        response.raise_for_status()
        size = 0
        async for chunk in response.content.iter_chunked(64 * 1024):
            size += len(chunk)
            if size > _MAX_MEDIA_BYTES:
                raise ValueError("media_too_large")
            if decryptor is None:
                yield chunk
            elif plain := decryptor.update(chunk):
                yield plain
    if decryptor is not None:
        yield decryptor.finalize()


class _AuthenticationError(RuntimeError):
    pass


__all__ = ["TEXT", "MediaDecryptor", "WeComChannel"]
