"""
Topic classification module for NFA Forgotten Archive.

Pipeline:
    1. topic_rules.py   – defines the taxonomy and per-topic keyword rules
    2. topic_classifier.py – applies rules to a single message, returns TopicMatch list
    3. classification_service.py – reads messages from DB, writes results to message_topics

Classifier versions are explicit strings (e.g. "rule_v1") so results from
different versions coexist in message_topics and can be compared or re-run.
"""
