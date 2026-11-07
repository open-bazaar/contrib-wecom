"""The configuration form the server draws from the schema."""

from bazaar_contrib_wecom import Config


def test_every_field_is_worded_in_both_languages_and_the_secret_is_write_only():
    schema = Config.model_json_schema()
    assert {"en", "zh-CN"} <= schema["x-locales"].keys()
    for texts in schema["x-locales"].values():
        for name in schema["properties"]:
            assert texts[f"{name}.title"] and texts[f"{name}.description"]
        assert texts["secret.empty"]
    assert schema["properties"]["secret"]["writeOnly"] is True
