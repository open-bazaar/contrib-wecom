from __future__ import annotations

import base64
import os
from pathlib import Path

from bazaar_contrib_channel import OutboundAttachment
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from bazaar_contrib_wecom.channel import MediaDecryptor
from bazaar_contrib_wecom.identity import TopicAddress, post_message_id
from bazaar_contrib_wecom.markdown import split_markdown, utf8_bytes
from bazaar_contrib_wecom.outbound import AttachmentReader, prepare_attachments
from bazaar_contrib_wecom.topic import mention_content, topic_message
from bazaar_contrib_wecom.topic_api import Member


def test_long_editorial_message_fits_the_wecom_limit():
    paragraphs = [
        f"第 {index} 期编辑记录：请保留每份稿件的标题、作者和发布时间。"
        for index in range(1, 1200)
    ]
    body = "\n\n".join(paragraphs) + '\n\n```python\nprint("Friday edition")\n```'
    parts = split_markdown(body, limit=20_480, measure=utf8_bytes)
    delivered = "\n\n".join(parts)
    for paragraph in paragraphs:
        assert paragraph in delivered
    assert 'print("Friday edition")' in delivered
    assert all(utf8_bytes(part) <= 20_480 for part in parts)


def test_member_handles_become_native_mentions_outside_code():
    body, mentions = mention_content(
        "@zhangsan 请审阅这份稿件，代码里的 `@zhangsan` 保持原样。",
        (Member(type=0, userid="zhangsan"), Member(type=1, aibotid="editor-bot")),
    )
    assert body.startswith("${inline_mention:mention_0}")
    assert "`@zhangsan`" in body
    assert mentions[0]["member"] == {"type": 0, "userid": "zhangsan"}


def test_channel_attachment_opens_the_public_absolute_path(tmp_path: Path):
    path = tmp_path / "editor-note.txt"
    path.write_text("Please retain this editorial note for Friday's edition.")
    attachments = prepare_attachments(
        tmp_path,
        (OutboundAttachment(name=path.name, path=path, media_type="text/plain"),),
    )
    for attachment in attachments:
        reader = AttachmentReader.open(attachment)
        try:
            assert b"Friday's edition" in reader.read_chunk()
        finally:
            reader.close()


def test_encrypted_media_decrypts_across_uneven_chunks():
    key = os.urandom(32)
    plaintext = b"Friday edition photo bytes. " * 300
    padding = 32 - len(plaintext) % 32
    encryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).encryptor()
    ciphertext = (
        encryptor.update(plaintext + bytes([padding]) * padding) + encryptor.finalize()
    )
    decryptor = MediaDecryptor(base64.b64encode(key).decode().rstrip("="))
    delivered = b"".join(
        decryptor.update(ciphertext[start : start + 1000])
        for start in range(0, len(ciphertext), 1000)
    )
    assert delivered + decryptor.finalize() == plaintext


def test_edited_post_keeps_the_original_message_identity():
    address = TopicAddress("editor-bot", "friday-channel", "post-1")
    author = Member(type=0, userid="zhangsan")
    original = topic_message(
        address,
        provider_id=post_message_id("post-1"),
        body="第一版稿件",
        creator=author,
        occurred_at_s=1_700_000_000,
        name="编辑部 · post-1",
    )
    edited = topic_message(
        address,
        provider_id=post_message_id("post-1"),
        body="第二版稿件",
        creator=author,
        occurred_at_s=1_700_000_100,
        name="编辑部 · post-1",
        edit_version="1700000100",
    )
    assert edited.id == original.id
    assert edited.provider_message_id == original.provider_message_id
    assert edited.edit_version == "1700000100"
    assert edited.body == "第二版稿件"
