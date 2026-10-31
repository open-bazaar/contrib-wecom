from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Literal

from bazaar_contrib_channel import OutboundAttachment

CHUNK_SIZE = 512 * 1024
MIN_MEDIA_BYTES = 5
MAX_FILENAME_BYTES = 256

_MEDIA_RULES = {
    ".amr": ("voice", 2 * 1024 * 1024),
    ".gif": ("image", 10 * 1024 * 1024),
    ".jpeg": ("image", 10 * 1024 * 1024),
    ".jpg": ("image", 10 * 1024 * 1024),
    ".mp4": ("video", 10 * 1024 * 1024),
    ".png": ("image", 10 * 1024 * 1024),
}
_FILE_LIMIT = 20 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class PreparedAttachment:
    descriptor: OutboundAttachment
    media_type: str
    workspace: Path
    size_bytes: int
    total_chunks: int
    md5: str


class AttachmentReader:
    def __init__(self, descriptor: int) -> None:
        self._descriptor = descriptor

    @classmethod
    def open(cls, attachment: PreparedAttachment) -> AttachmentReader:
        descriptor, file_stat = _open_regular_file(
            attachment.workspace, attachment.descriptor
        )
        maximum_size = media_type_for_filename(attachment.descriptor.name)[1]
        if file_stat.st_size > maximum_size:
            os.close(descriptor)
            raise ValueError(
                f"WeCom {attachment.media_type} attachment exceeds its size limit: "
                f"{attachment.descriptor.name}"
            )
        return cls(descriptor)

    def read_chunk(self) -> bytes:
        return os.read(self._descriptor, CHUNK_SIZE)

    def close(self) -> None:
        os.close(self._descriptor)


@dataclass(frozen=True, slots=True)
class UploadResult:
    media_id: str | None
    receipts: tuple[dict[str, object], ...]
    error_kind: str | None = None
    error_message: str | None = None


def media_type_for_filename(filename: str) -> tuple[str, int]:
    suffix = PurePosixPath(filename).suffix.lower()
    return _MEDIA_RULES.get(suffix, ("file", _FILE_LIMIT))


def encode_request(command: str, request_id: str, body: Mapping[str, object]) -> str:
    return json.dumps(
        {
            "cmd": command,
            "headers": {"req_id": request_id},
            "body": body,
        },
        separators=(",", ":"),
    )


def visible_message_body(
    *,
    target_id: str,
    target_kind: Literal["dm", "group"],
    message_type: str,
    content: Mapping[str, object],
) -> Mapping[str, object]:
    return {
        "chatid": target_id,
        "chat_type": 1 if target_kind == "dm" else 2,
        "msgtype": message_type,
        message_type: content,
    }


def prepare_attachments(
    workspace: Path, attachments: tuple[OutboundAttachment, ...]
) -> tuple[PreparedAttachment, ...]:
    root = workspace.resolve(strict=True)
    prepared: list[PreparedAttachment] = []
    for attachment in attachments:
        filename_size = len(attachment.name.encode("utf-8"))
        if filename_size > MAX_FILENAME_BYTES:
            raise ValueError(
                f"WeCom attachment filename exceeds 256 bytes: {attachment.name}"
            )
        media_type, maximum_size = media_type_for_filename(attachment.name)
        descriptor, _ = _open_regular_file(root, attachment)
        md5 = hashlib.md5(usedforsecurity=False)
        size_bytes = 0
        try:
            while chunk := os.read(descriptor, CHUNK_SIZE):
                size_bytes += len(chunk)
                if size_bytes > maximum_size:
                    raise ValueError(
                        f"WeCom {media_type} attachment exceeds its size limit: "
                        f"{attachment.name}"
                    )
                md5.update(chunk)
        finally:
            os.close(descriptor)
        if size_bytes < MIN_MEDIA_BYTES:
            raise ValueError(
                f"WeCom attachment must contain at least 5 bytes: {attachment.name}"
            )
        prepared.append(
            PreparedAttachment(
                descriptor=attachment,
                media_type=media_type,
                workspace=root,
                size_bytes=size_bytes,
                total_chunks=(size_bytes + CHUNK_SIZE - 1) // CHUNK_SIZE,
                md5=md5.hexdigest(),
            )
        )
    return tuple(prepared)


def _open_regular_file(
    workspace: Path, attachment: OutboundAttachment
) -> tuple[int, os.stat_result]:
    relative = (
        attachment.path.relative_to(workspace)
        if attachment.path.is_absolute()
        else attachment.path
    )
    current = workspace
    for part in relative.parts:
        current /= part
        if current.is_symlink():
            raise ValueError(
                f"WeCom attachment path must not contain symlinks: {attachment.name}"
            )
    resolved = current.resolve(strict=True)
    try:
        resolved.relative_to(workspace)
    except ValueError as error:
        raise ValueError(
            f"WeCom attachment path leaves the workspace: {attachment.name}"
        ) from error
    descriptor = os.open(resolved, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        file_stat = os.fstat(descriptor)
        opened_stat = os.stat(resolved, follow_symlinks=False)
        if (
            file_stat.st_dev != opened_stat.st_dev
            or file_stat.st_ino != opened_stat.st_ino
        ):
            raise ValueError(
                f"WeCom attachment changed while it was opened: {attachment.name}"
            )
        if not stat.S_ISREG(file_stat.st_mode):
            raise ValueError(
                f"WeCom attachment must be a regular file: {attachment.name}"
            )
    except BaseException:
        os.close(descriptor)
        raise
    return descriptor, file_stat


__all__ = [
    "CHUNK_SIZE",
    "AttachmentReader",
    "PreparedAttachment",
    "UploadResult",
    "encode_request",
    "media_type_for_filename",
    "prepare_attachments",
    "visible_message_body",
]
