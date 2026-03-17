"""
Tests for ingest parsers.

These tests are pure unit tests — no database required.
Run with: pytest tests/test_parsers.py -v
"""

import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


class TestJSONParser:
    def test_parse_sample_file(self):
        from src.ingest.parsers.json_parser import JSONParser

        parser = JSONParser()
        messages = parser.parse(FIXTURES / "sample_chat.json")

        assert len(messages) == 4
        first = messages[0]
        assert first.group_name == "NFA Test Group"
        assert first.platform == "wechat"
        assert first.external_group_id == "wx_grp_001"
        assert first.user_name == "Alice"
        assert first.external_member_id == "wx_u_001"
        assert first.content == "Hello everyone! Welcome to the group."
        assert first.normalized_content == "Hello everyone! Welcome to the group."
        assert first.external_message_id == "wx_msg_001"
        assert first.reply_to_external_message_id is None

    def test_reply_to_is_parsed(self):
        from src.ingest.parsers.json_parser import JSONParser

        parser = JSONParser()
        messages = parser.parse(FIXTURES / "sample_chat.json")
        assert messages[1].reply_to_external_message_id == "wx_msg_001"

    def test_missing_required_field_raises(self, tmp_path):
        from src.ingest.parsers.json_parser import JSONParser

        # Provide group_name and user_name but omit timestamp
        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps([{"group_name": "G", "user_name": "Alice", "content": "hi"}]))
        parser = JSONParser()
        with pytest.raises(ValueError, match="timestamp"):
            parser.parse(bad)

    def test_invalid_json_raises(self, tmp_path):
        from src.ingest.parsers.json_parser import JSONParser
        import json as _json

        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json")
        parser = JSONParser()
        with pytest.raises(_json.JSONDecodeError):
            parser.parse(bad)

    def test_non_array_root_raises(self, tmp_path):
        from src.ingest.parsers.json_parser import JSONParser

        bad = tmp_path / "bad.json"
        bad.write_text(json.dumps({"key": "value"}))
        parser = JSONParser()
        with pytest.raises(ValueError, match="array"):
            parser.parse(bad)

    def test_normalized_content_strips_whitespace(self, tmp_path):
        from src.ingest.parsers.json_parser import JSONParser

        data = [{
            "group_name": "G", "user_name": "A",
            "content": "  hello   world  ",
            "timestamp": "2024-01-01T00:00:00",
        }]
        f = tmp_path / "test.json"
        f.write_text(json.dumps(data))
        msgs = JSONParser().parse(f)
        assert msgs[0].normalized_content == "hello world"


class TestTXTParser:
    def test_parse_sample_file(self):
        from src.ingest.parsers.txt_parser import TXTParser

        parser = TXTParser(group_name="NFA Test Group", platform="wechat")
        messages = parser.parse(FIXTURES / "sample_chat.txt")

        assert len(messages) == 5
        assert messages[0].user_name == "Alice"
        assert messages[0].content == "Hello everyone! Welcome to the group."
        assert messages[0].external_message_id is None
        assert messages[0].group_name == "NFA Test Group"
        assert messages[0].platform == "wechat"

    def test_skips_malformed_lines(self, tmp_path):
        from src.ingest.parsers.txt_parser import TXTParser
        import warnings

        f = tmp_path / "chat.txt"
        f.write_text(
            "[2024-01-01 10:00:00] Alice: Good message\n"
            "this line has no timestamp\n"
            "[2024-01-01 10:01:00] Bob: Another good one\n"
        )
        parser = TXTParser(group_name="G")
        with warnings.catch_warnings(record=True):
            messages = parser.parse(f)
        assert len(messages) == 2

    def test_file_not_found_raises(self):
        from src.ingest.parsers.txt_parser import TXTParser

        parser = TXTParser(group_name="G")
        with pytest.raises(FileNotFoundError):
            parser.parse(Path("/nonexistent/file.txt"))


class TestCSVParser:
    def test_parse_sample_file(self):
        from src.ingest.parsers.csv_parser import CSVParser

        parser = CSVParser()
        messages = parser.parse(FIXTURES / "sample_chat.csv")

        assert len(messages) == 4
        assert messages[0].group_name == "NFA Test Group"
        assert messages[0].platform == "telegram"
        assert messages[0].external_message_id == "tg_msg_001"
        assert messages[1].reply_to_external_message_id == "tg_msg_001"

    def test_missing_required_column_raises(self, tmp_path):
        from src.ingest.parsers.csv_parser import CSVParser

        f = tmp_path / "bad.csv"
        f.write_text("user_name,content\nAlice,hello\n")
        with pytest.raises(ValueError, match="missing required columns"):
            CSVParser().parse(f)

    def test_empty_required_field_skipped(self, tmp_path):
        from src.ingest.parsers.csv_parser import CSVParser
        import warnings

        f = tmp_path / "chat.csv"
        f.write_text(
            "group_name,user_name,content,timestamp\n"
            "G,Alice,hello,2024-01-01 10:00:00\n"
            "G,,missing_user,2024-01-01 10:01:00\n"
        )
        with warnings.catch_warnings(record=True):
            msgs = CSVParser().parse(f)
        assert len(msgs) == 1
