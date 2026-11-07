# WeCom Channel provider

Migrate the WeCom implementation from BCN `main` commit
`6f5c9927865b9f945bb1502b711068244ef5af03`, under
`src/bazaar_compute_node/contrib/wecom/`, into this independent provider.
Use the published core, audit, channel, harness, timer, messages and
collaboration contracts from `0.3.0rc11`.
All implementation, tests, metadata and documentation belong to this repository.

## Task 1: Provider implementation and package validation

Register `wecom-builder` through `ChannelBuilderConfig`, `CHANNEL_BUILDERS`
and `channel_builder_key("wecom")`. Accept `bot_id`, an optional
`websocket_url` defaulting to `wss://openws.work.weixin.qq.com`, and either
`secret_env` or a direct `secret` from the configer; a supplied `secret` takes
precedence. The selected Channel owns its aiohttp session, long connection and
tasks in its contrib lifespan, accepts `CHANNEL_INPUT`, `TIMER`, `AUDIT` and
`MESSAGES`, listens to `CONTACT_REVIEWED`, and offers `CHANNEL` after
connecting. Retain the existing request and lifecycle budgets through the
public Timer contract.

Preserve one long connection per bot with its subscription, heartbeat and
reconnection. In ordinary conversations, preserve group and direct identities,
stable message IDs, group mentions, quote backfill, text, voice, image, file,
video and mixed content with decrypted media downloads, chunked media uploads,
Markdown split at 20,480 bytes, proactive DM addressing, receipts and
template-card approvals. In channels, preserve the channel and post
conversations, posts and replies fetched by id, native mentions, inline files
and images, recalls and deletions, starting a post from the channel
conversation, replies through `parent_reply_id`, and the rejection of
approvals. Content the platform also synchronizes into the group path with
`compat_info.source=channel` is still dropped there. Convert addressing and
attachments at the existing public Channel boundary. `prepare_send` returns
individual message operations; the existing shared sender owns retry policy and
receipts. Translate WeCom error codes and transport information into public
Channel errors. Keep the original English approval text.

User-approved changes:

- An updated post or reply (`type=2`) arrives as an inbound message whose
  `edit_version` is the fetched content's `modify_time`, or the event's
  `create_time` when that is absent. The 0.2 `[Content updated]` prefix and
  `:event:<msgid>` suffix are removed. Deletions and recalls keep their 0.2
  records.
- A channel conversation is no longer approved automatically. A post opens
  approved when its channel conversation is approved and pending otherwise; a
  post opened while the channel is pending is reviewed on its own. The Channel
  reads the channel conversation's review through `MESSAGES` and keeps it per
  channel in memory. `CONTACT_REVIEWED` for a channel conversation of this
  agent drops that entry, as does the bot leaving the channel, so the next post
  reads it again. Conversations stored by 0.2.3 keep their identities and their
  approved review, so posts in those channels need no review.
- The channel conversation receives one message per channel, with a fixed
  provider identity, when the bot joins the channel or on the channel's first
  event. While the channel conversation is not approved, that message notifies
  the runtime, so a configured `review.reply` is posted into the channel,
  provided the bot may post there proactively; after approval the agent reads
  it once. In a channel already approved, such as one stored by 0.2.3, it stays
  quiet. A change to the channel's information still adds a quiet message
  carrying its current name.
- A received image, file or video in an ordinary conversation is downloaded
  by the node once the conversation is approved, as BCN 0.3 does for every
  Channel. Its name still comes from the download's `Content-Disposition`,
  read on receipt by a request that leaves the body unread, and a channel
  image's extension still follows its download's `Content-Type`.
- A quoted message in an ordinary conversation, whose author WeCom does not
  name, arrives with an unknown sender, since 0.3 requires one. Its identity
  still follows the quoted content; media in it is identified by its download
  address without the signature, as the content is not downloaded on receipt.
- Callbacks are handled in arrival order by their own worker, so reading media
  headers or channel content never holds up acknowledgements and heartbeats on
  the long connection.
- A successful subscription resets the reconnect and authentication attempt
  counts, so failures separated by healthy connections no longer add up to the
  longest backoff or a permanent stop.
- A transport failure while sending is reported as not sent, so the shared
  sender retries it with its backoff, including a failure after the request may
  have reached WeCom: a duplicate message is acceptable, while an unknown
  outcome stops the retry. The error names the aiohttp or OS error and keeps
  its message with the secret and access token redacted.
- Publish provider-internal failures as Audit events instead of the 0.2 health
  counters and logs, which nothing in 0.3 reads: `channel.connection.failed`,
  `channel.message.filtered`, `channel.message.reference_unresolved`,
  `channel.approval.event_ignored`, `channel.approval.edit_failed`,
  `channel.topic.event_filtered`, `channel.topic.event_failed` and
  `channel.topic.reference_unavailable`. Each subscription of the long
  connection publishes `channel.connection.subscribed`. Received frames,
  queued messages and sent card updates are not published. Inbound, outbound,
  attachment and approval outcomes come from the existing framework events.

Keep README as short as the Telegram provider's setup and configuration
reference. Verify content, addressing and real filesystem attachments, then
independently install both wheel and sdist and enable the actual registered
builder through a real ContribHub with both credential configuration forms.
Enable blockbuster in pytest and the isolated installation subprocesses. Run
Ruff, format, Pyright and three-platform CI.

## Task 2: Real WeCom behavior tests

Test the WeCom Channel through a real installed node against the real
platform, in the manner of the Telegram provider. A scripted TestHarness stands
in for the model and a second bot drives the conversation from the WeCom side.
Tests are marked `e2e`, run in their own CI job, and read their configuration
from the environment; a missing value fails the run.

- `WECOM_TEST_BOT_ID` (variable) and `WECOM_TEST_BOT_SECRET` (secret): the bot
  under test.
- `WECOM_DRIVER_BOT_ID` (variable) and `WECOM_DRIVER_BOT_SECRET` (secret): the
  driver bot.
- `WECOM_TEST_TOPIC_ID` (variable): the dedicated test channel. Both bots read
  all of its content and post in it proactively.

A bot cannot mention another bot, and a post that does not mention the agent
does not wake it. The agent is woken instead by the channel conversation's
message once the test approves that conversation, and then by replies in the
post it started, which it follows because it created it.

The fixture package `bcn-wecom-test-harness` under `tests/fixtures/harness`
registers `test-harness-builder` with the Telegram provider's sending-only
Harness and worker. For the channel conversation's message the worker starts a
post `<marker> post`; for every new message carrying the run marker it sends
what the instruction after the marker names:

- `reply`: a reply to that message.
- `long`: Markdown above the 20,480-byte content limit with headings, lists and
  a fenced block.
- `attach`: a fixed workspace file as an attachment.
- `quote`: the body of the reply this one answers.
- `echo`: the message's own attachment, as materialized in the workspace.
- `approve`: an approval request for a command, then the decision back to that
  target as a reply; only the local approval check uses it.

The package also offers `audit-journal` and `config-env-token`, which turns
each WeCom channel's `secret_env` into a direct `secret` held only in memory.
The test reads the journal only for what the channel interfaces cannot tell
it: which pending conversation is the test channel, the review each post
conversation opened with, from `channel.inbound.persisted`, and when the
Channel has subscribed. The node reports ready before its agents' Channels
connect, and WeCom keeps no event for a bot that is not connected, so each
start waits for `channel.connection.subscribed` before the driver posts. The
node is prepared like the Telegram acceptance and first runs with
`config-filesystem`.

The test script is the driver: it posts and replies in the test channel
through the channel interfaces, then reads the channel's posts and replies and
checks that what arrived is what the instruction asked for. Bots cannot delete
posts, so each run's posts stay in the test channel, tagged with its marker.

1. Review: `<marker> open` from the driver leaves the channel conversation and
   that post waiting for review. The test approves the channel conversation
   with `bcn agent review approve`; `<marker> second` then opens approved.
2. Proactive post: the agent starts `<marker> post` in the test channel.
3. Reply: `<marker> reply`, a driver reply in that post, is answered by a
   reply to it.
4. Quote backfill: `<marker> quote`, a driver reply to an earlier driver
   reply, is answered with that earlier reply's text.
5. Long rich text: `<marker> long` is answered by several replies whose
   numbered sections, in order, are the whole document.
6. Outbound attachment: `<marker> attach` is answered by a reply carrying a
   file whose bytes are the fixture file.
7. Inbound attachment: a driver reply carrying a file and `<marker> echo` is
   answered by a reply carrying a file with the same bytes.
8. Direct secret: after a restart with `config-env-token`, `<marker> reply` in
   that post is answered again.

Checked locally by hand rather than in CI: ordinary group and direct
conversations, which need a person to mention or message the bot, with quote
backfill and media there; template-card approvals; an edited post and reply,
since bots cannot edit; and a configured `review.reply` posted into a pending
channel. Record the CI run and the local check as acceptance evidence.

## Task 3: Manual acceptance and its fixes

A manual pass with a person on WeCom, against BCN `0.3.0rc12` and the released
`bazaar-contrib-wecom 0.3.0rc1` with the scripted test Harness, covered what CI
cannot drive. A direct message and an ordinary group each opened awaiting
review, were answered with the configured `review.reply`, and woke the agent
once `bcn agent review approve` let them in. Replies, quote backfill, a quoted
group image, approval cards approved and rejected in a group, the test
channel's review, and an edited reply in the agent's own post behaved as
specified. A bot's reply in a direct message cannot quote the message it
answers, and a group message cannot carry text and an image together; both are
platform limits.

Two defects surfaced and are fixed in this task:

- Images in direct and group conversations were stored as
  `application/octet-stream`, the type of the encrypted download, so a Harness
  that passes only `image/` attachments as images received them as files. When
  the download is encrypted, or the server names no specific type, the type now
  comes from the file name the download carries.
- The bot's new post was recorded as an inbound message even in a channel not
  yet let in, where the only post the bot makes is the root's review answer, so
  it appeared among the conversations awaiting review. Such a post is no longer
  recorded.

Files in channel posts also regain the type 0.2 gave them: their download's
`Content-Type`, read on receipt by a request that leaves the body unread, as
channel images already do. When the download names no specific type, the type
comes from the file's name, as in direct and group conversations.

The provider also stops capping direct and group media at 25 MB. The cap was a
0.2 choice with no platform basis; the download streams to disk, and what
arrives is bounded by the platform's own limits. BCN `0.3.0rc13` drops the
node's own per-file limit as well, so the development and `e2e` requirements on
`bazaar-compute-node` start there and the lock takes that release's packages.

A post in a channel not yet let in opens two conversations awaiting review,
the channel's root and the post, and each answers with `review.reply`; the
root's answer arrives as a separate post. This stays as it is. Each
conversation's request for notification is also what wakes the agent once that
conversation is approved, so silencing either answer would lose the wake after
the channel's approval or after the post's own.

Verification: the `e2e` job, whose review case and topic cases cover the
changed paths, and a manual rerun of an image in a direct message, a post in a
channel awaiting review and a file in a channel post.

## Task 4: Form words and connection events

BCN stage 13 draws every channel's configuration form from the schema of its
entity model and reports each connection's state from shared connection events,
per messages `25fe9d74-6015-5615-b26a-1021b11d8792`, `7ce989b2-cd0f-577c-9648-97f4153dc2c5`
and `beb133fa-5a23-56d3-b225-f18f5a665270`. The words of the form live in
`resources/locales/en.toml` and `zh-CN.toml`, keyed by field: `<field>.title`,
`<field>.description` and, for a field that can stay empty, `<field>.empty`.
`Config` carries both under `x-locales` at the top of its schema; `secret` is
marked `writeOnly`.

The Channel publishes `ChannelConnected` with the bot's identity once it is
subscribed, replacing `channel.connection.subscribed`, and
`ChannelConnectionFailed` with the error on every failure: with the backoff
before the next attempt, or without one when it stops trying, which is an
authentication rejected at startup or three times in a row, six network
failures at startup, or `disconnected_event` when another connection takes the
bot over. The agent names the connection on them, so the Channel does not.
`bazaar-contrib-channel` moves to `~=0.3.0rc20`, and the tests install
`bazaar-compute-node>=0.3.0rc20`. The `e2e` test checks that the node reports
itself connected on `wecom:0` as the bot under test, then subscribes as the
same bot from another connection: the node reports the failure without a retry
delay, with `wecom_disconnected_event`.

## Local validation

Ruff and format checks, Pyright CLI and the language-server check pass.
Six tests pass, including independent wheel and sdist installations that load
`wecom-builder` and register it in a real ContribHub with both credential
configuration forms. Blockbuster is active in pytest and both installed-package
subprocesses. Markdown splitting, native mentions, filesystem attachments,
media decryption and the identity of an edited post are exercised with real
data and files. Against the real platform, the test channel's posts and a
direct message's file and image were read through the bot under test: channel
files carry their names, and the encrypted downloads of a direct message name
the file in `Content-Disposition`.
The `e2e` test passes locally against the two dedicated bots and the test
channel in about 160 seconds with BCN `0.3.0rc11`: all eight cases, twice in a
row. Neither bot's secret appears in any file the run leaves, logs and journal
included.
Locally, a card in a direct message was approved and another rejected with its
buttons, each answering the sender and leaving only its status button.
Stopping the node with a third card pending cancelled the turn waiting on it;
the card kept its buttons, as WeCom updates a card only in reply to its button.
With BCN `0.3.0rc12`, which delivers turn events as `CHANNEL_TURN` and no longer
calls `accept_turn_event`, the Channel drops that empty method; the `e2e` test
passes again locally in about 135 seconds with every package in the lock at
`0.3.0rc12`.
After Task 3's fixes, a fresh node against BCN `0.3.0rc12` with the provider
built from the branch recorded an image in a direct message as `image/jpeg`;
another image, sent once the conversation was approved, was downloaded and
decrypted into a JPEG of that type. A post in a channel awaiting review opened
the channel conversation and the post for review, each answered once with
`review.reply`, and the bot's own review post was not recorded.
With the channel file change, a reply in that post carrying `skhd.md` was
recorded with that name and `text/markdown`, and a reply carrying an image as
`image-1.jpg` with its download's `image/jpeg; charset=UTF-8`.
