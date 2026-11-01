from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Fence:
    marker: str
    opening: str


def advance_fence(
    fence: Fence | None,
    segment: str,
    *,
    initial_line_boundary: bool,
    terminal_line_complete: bool,
) -> Fence | None:
    lines = segment.splitlines(keepends=True)
    for index, line in enumerate(lines):
        complete = (index > 0 or initial_line_boundary) and (
            line.endswith(("\n", "\r"))
            or (index == len(lines) - 1 and terminal_line_complete)
        )
        if not complete:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if fence is None:
            marker_char = stripped[0]
            if marker_char not in {"`", "~"}:
                continue
            marker_length = len(stripped) - len(stripped.lstrip(marker_char))
            if marker_length < 3:
                continue
            marker = marker_char * marker_length
            info = stripped[marker_length:]
            if marker_char == "`" and "`" in info:
                continue
            fence = Fence(marker=marker, opening=stripped)
            continue
        if stripped[0] != fence.marker[0]:
            continue
        marker_length = len(stripped) - len(stripped.lstrip(fence.marker[0]))
        if marker_length >= len(fence.marker) and not stripped[marker_length:].strip():
            fence = None
    return fence


def closing_suffix(content: str, fence: Fence | None) -> str:
    if fence is None:
        return ""
    separator = "" if content.endswith(("\n", "\r")) else "\n"
    return f"{separator}{fence.marker}"


def preferred_boundary(content: str, minimum: int, end: int) -> int | None:
    """Find the latest place at or after minimum where a split reads naturally."""

    for separator in ("\n\n", "\n"):
        index = content.rfind(separator, minimum, end)
        if index >= minimum:
            return index + len(separator)
    for index in range(end - 1, minimum - 1, -1):
        if content[index].isspace():
            return index + 1
    return None


def utf8_bytes(text: str) -> int:
    return len(text.encode("utf-8"))


def code_points(text: str) -> int:
    return len(text)


def split_markdown(
    content: str,
    *,
    limit: int,
    measure: Callable[[str], int] = code_points,
) -> tuple[str, ...]:
    """Split markdown to fit a provider's limit, reopening any fence it cuts."""

    if limit <= 0:
        raise ValueError("limit must be positive")
    if not content:
        return (content,)

    chunks: list[str] = []
    cursor = 0
    fence: Fence | None = None
    while cursor < len(content):
        prefix = f"{fence.opening}\n" if fence is not None else ""
        end = cursor
        size = measure(prefix)
        while end < len(content):
            step = measure(content[end])
            if size + step > limit:
                break
            size += step
            end += 1
        if end == cursor:
            raise ValueError("markdown fence overhead exceeds the provider limit")

        line_start = fence is not None or cursor == 0 or content[cursor - 1] in "\r\n"
        while True:
            next_fence = advance_fence(
                fence,
                content[cursor:end],
                initial_line_boundary=line_start,
                terminal_line_complete=(end == len(content) or content[end] in "\r\n"),
            )
            suffix = closing_suffix(prefix + content[cursor:end], next_fence)
            if measure(prefix + content[cursor:end] + suffix) <= limit:
                break
            end -= 1
            if end == cursor:
                raise ValueError("markdown fence closure exceeds the provider limit")

        if end < len(content):
            minimum = cursor + max(1, (end - cursor) // 2)
            preferred = preferred_boundary(content, minimum, end)
            if preferred is not None:
                preferred_fence = advance_fence(
                    fence,
                    content[cursor:preferred],
                    initial_line_boundary=line_start,
                    terminal_line_complete=(
                        preferred == len(content) or content[preferred] in "\r\n"
                    ),
                )
                preferred_suffix = closing_suffix(
                    prefix + content[cursor:preferred], preferred_fence
                )
                if (
                    measure(prefix + content[cursor:preferred] + preferred_suffix)
                    <= limit
                ):
                    end = preferred
                    next_fence = preferred_fence
                    suffix = preferred_suffix

        chunks.append(prefix + content[cursor:end] + suffix)
        cursor = end
        fence = next_fence

    return tuple(chunks)


__all__ = [
    "Fence",
    "advance_fence",
    "closing_suffix",
    "code_points",
    "preferred_boundary",
    "split_markdown",
    "utf8_bytes",
]
