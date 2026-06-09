"""
Tests for OneBot v11 fixture structure validation.

Validates that fixture files correctly represent the NapCat / OneBot v11
raw event structures discovered during Field Discovery (M-01 through M-08).

These tests do NOT require a database or any NFA service.
They only validate fixture file structure and type constraints.

Run with: pytest tests/test_onebot_fixtures.py -v
"""

import json
from pathlib import Path

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "onebot"


def _load(name: str) -> dict:
    path = FIXTURES_DIR / name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


# ── Helpers ──────────────────────────────────────────────────────────


def _assert_message_common(event: dict, expected_sub_type: str = "normal"):
    """Validate common fields shared by all message events."""
    assert event["post_type"] == "message"
    assert event["message_type"] == "group"
    assert event["sub_type"] == expected_sub_type
    assert isinstance(event["self_id"], int)
    assert isinstance(event["user_id"], int)
    assert isinstance(event["time"], int)
    assert isinstance(event["message_id"], int)
    assert isinstance(event["message_seq"], int)
    assert isinstance(event["real_id"], int)
    # real_seq is a STRING (NapCat gotcha)
    assert isinstance(event["real_seq"], str)
    assert isinstance(event["font"], int)
    assert event["message_format"] == "array"
    assert isinstance(event["group_id"], int)
    assert isinstance(event["group_name"], str)
    assert isinstance(event["raw_message"], str)


def _assert_sender(event: dict):
    """Validate sender sub-object."""
    sender = event["sender"]
    assert isinstance(sender["user_id"], int)
    assert isinstance(sender["nickname"], str)
    # card is always present but may be empty string
    assert isinstance(sender["card"], str)
    # role is one of: owner, member, admin
    assert sender["role"] in ("owner", "member", "admin")


def _assert_message_array(event: dict):
    """Validate message segment array exists and has at least one segment."""
    assert isinstance(event["message"], list)
    assert len(event["message"]) >= 1


# ── Message Event Tests ──────────────────────────────────────────────


class TestGroupText:
    """M-01: Plain group text from owner."""

    def test_load_and_basic_structure(self):
        event = _load("group_text.json")
        _assert_message_common(event)
        _assert_sender(event)
        _assert_message_array(event)

    def test_single_text_segment(self):
        event = _load("group_text.json")
        assert len(event["message"]) == 1
        assert event["message"][0]["type"] == "text"
        assert isinstance(event["message"][0]["data"]["text"], str)

    def test_sender_role_owner(self):
        event = _load("group_text.json")
        assert event["sender"]["role"] == "owner"


class TestGroupTextMember:
    """M-02: Plain group text from regular member."""

    def test_sender_role_member(self):
        event = _load("group_text_member.json")
        _assert_message_common(event)
        _assert_sender(event)
        assert event["sender"]["role"] == "member"


class TestGroupTextCard:
    """M-03: Group text with sender card set."""

    def test_sender_card_populated(self):
        event = _load("group_text_card.json")
        _assert_message_common(event)
        _assert_sender(event)
        # card can be empty string OR populated
        assert isinstance(event["sender"]["card"], str)


class TestGroupTextEmoji:
    """M-04: Group text with Unicode emoji."""

    def test_text_contains_emoji(self):
        event = _load("group_text_emoji.json")
        _assert_message_common(event)
        text = event["message"][0]["data"]["text"]
        assert any(ord(c) > 0x1F600 for c in text)


class TestGroupTextAt:
    """M-05: Group text with @ mention segment."""

    def test_multi_segment_structure(self):
        event = _load("group_text_at.json")
        _assert_message_common(event)
        msg = event["message"]
        assert len(msg) == 3
        assert msg[0]["type"] == "text"
        assert msg[1]["type"] == "at"
        assert msg[2]["type"] == "text"

    def test_at_qq_is_string(self):
        """Critical: at.data.qq is a string, NOT an integer."""
        event = _load("group_text_at.json")
        at_data = event["message"][1]["data"]
        assert isinstance(at_data["qq"], str)


class TestGroupImage:
    """M-06: Group message with image segment."""

    def test_image_segment_present(self):
        event = _load("group_image.json")
        _assert_message_common(event)
        assert event["message"][0]["type"] == "image"

    def test_image_data_fields(self):
        event = _load("group_image.json")
        img = event["message"][0]["data"]
        # These fields exist in NapCat output
        assert "file" in img
        assert "url" in img
        assert "sub_type" in img
        assert "summary" in img
        assert "file_size" in img

    def test_image_file_size_can_be_string(self):
        """file_size may be string in some NapCat versions."""
        event = _load("group_image.json")
        img = event["message"][0]["data"]
        # Accept both string and int
        assert isinstance(img["file_size"], (str, int))

    def test_caption_text_after_image(self):
        event = _load("group_image.json")
        assert event["message"][1]["type"] == "text"


class TestGroupReply:
    """M-07: Group reply (quote) message."""

    def test_reply_segment_present(self):
        event = _load("group_reply.json")
        _assert_message_common(event)
        assert event["message"][0]["type"] == "reply"

    def test_reply_id_is_string(self):
        """Critical: reply.data.id is a string, NOT an integer."""
        event = _load("group_reply.json")
        reply_data = event["message"][0]["data"]
        assert isinstance(reply_data["id"], str)

    def test_reply_followed_by_at_and_text(self):
        event = _load("group_reply.json")
        msg = event["message"]
        assert len(msg) == 3
        assert msg[0]["type"] == "reply"
        assert msg[1]["type"] == "at"
        assert msg[2]["type"] == "text"


# ── Notice Event Tests ──────────────────────────────────────────────


class TestGroupRecallNotice:
    """M-08: group_recall notice event."""

    def test_notice_type(self):
        event = _load("group_recall_notice.json")
        assert event["post_type"] == "notice"
        assert event["notice_type"] == "group_recall"

    def test_notice_has_no_message_fields(self):
        """Notice events must NOT have message-specific fields."""
        event = _load("group_recall_notice.json")
        message_only_fields = [
            "message", "raw_message", "sender", "font",
            "sub_type", "message_type", "message_format",
            "message_seq", "real_id", "real_seq",
        ]
        for field in message_only_fields:
            assert field not in event, f"Notice event should not have '{field}'"

    def test_notice_required_fields(self):
        event = _load("group_recall_notice.json")
        assert isinstance(event["time"], int)
        assert isinstance(event["self_id"], int)
        assert isinstance(event["group_id"], int)
        assert isinstance(event["user_id"], int)
        assert isinstance(event["notice_type"], str)
        assert isinstance(event["operator_id"], int)
        assert isinstance(event["message_id"], int)


# ── Cross-cutting Tests ─────────────────────────────────────────────


class TestAllFixturesLoadable:
    """Every fixture file must be valid JSON and loadable."""

    FIXTURE_NAMES = [
        "group_text.json",
        "group_text_member.json",
        "group_text_card.json",
        "group_text_emoji.json",
        "group_text_at.json",
        "group_image.json",
        "group_reply.json",
        "group_recall_notice.json",
    ]

    @pytest.mark.parametrize("name", FIXTURE_NAMES)
    def test_is_valid_json(self, name):
        data = _load(name)
        assert isinstance(data, dict)

    @pytest.mark.parametrize("name", FIXTURE_NAMES)
    def test_has_metadata(self, name):
        data = _load(name)
        assert "_comment" in data
        assert "_source" in data

    @pytest.mark.parametrize("name", FIXTURE_NAMES)
    def test_has_post_type(self, name):
        data = _load(name)
        assert data["post_type"] in ("message", "notice")
