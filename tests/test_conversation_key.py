from bazaar_contrib_channel import ChannelTarget

from bazaar_contrib_wecom import BuilderConfig
from bazaar_contrib_wecom.identity import TopicAddress, conversation_thread_id


def test_groups_topics_posts_and_endpoints_have_their_own_scope():
    builder = BuilderConfig.builder()
    configs = ({"bot_id": "one"}, {"bot_id": "two"})
    first, second = (
        ChannelTarget(kind="group", thread_id=conversation_thread_id(bot, "room"))
        for bot in ("one", "two")
    )
    key = builder.conversation_key(first, configs)
    assert key is not None
    assert key == builder.conversation_key(second, configs)
    assert builder.conversation_key(first) is None
    assert builder.conversation_key(first, ({"bot_id": "two"},)) is None
    other_endpoint = ({"bot_id": "one", "websocket_url": "wss://example.com"},)
    assert key != builder.conversation_key(first, other_endpoint)
    assert builder.conversation_key(first, configs + other_endpoint) is None
    topic, peer, post, other_post = (
        ChannelTarget(
            kind="group", thread_id=TopicAddress(bot, "topic", post).provider_thread_id
        )
        for bot, post in (
            ("one", None),
            ("two", None),
            ("one", "post-a"),
            ("two", "post-b"),
        )
    )
    assert builder.conversation_key(topic, configs) == builder.conversation_key(
        peer, configs
    )
    assert (
        len(
            {
                key,
                builder.conversation_key(topic, configs),
                builder.conversation_key(post, configs),
                builder.conversation_key(other_post, configs),
            }
        )
        == 4
    )
    assert (
        builder.conversation_key(first.model_copy(update={"kind": "dm"}), configs)
        is None
    )
