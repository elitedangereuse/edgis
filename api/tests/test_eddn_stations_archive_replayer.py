from __future__ import annotations

import bz2
import json
from pathlib import Path
from types import SimpleNamespace

from feeders.eddn_stations_archive_replayer import ordered_archives, replay_archives


class FakeConnection:
    def __init__(self):
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def write_archive(path: Path, *messages: dict) -> None:
    with bz2.open(path, "wt", encoding="utf-8") as stream:
        for message in messages:
            stream.write(json.dumps(message) + "\n")


def test_ordered_archives_sorts_by_archive_date(tmp_path):
    later = tmp_path / "Journal.Docked-2026-09-02.jsonl.bz2"
    earlier = tmp_path / "Journal.Docked-2026-09-01.jsonl.bz2"
    write_archive(later)
    write_archive(earlier)

    assert ordered_archives([later, earlier]) == [earlier, later]


def test_replay_streams_messages_without_live_metrics(tmp_path):
    archive = tmp_path / "Journal.Docked-2026-09-24.jsonl.bz2"
    write_archive(
        archive,
        {"message": {"event": "Docked", "Body": "Founders World"}},
        {"message": {"event": "Docked"}},
    )
    calls = []

    def process(message, **kwargs):
        calls.append((message, kwargs))
        return SimpleNamespace(status="success")

    connection = FakeConnection()
    counts = replay_archives(
        [archive], connection, process, commit_every=1,
    )

    assert counts == {
        "seen": 2,
        "with_host": 1,
        "processed": 2,
        "skipped": 0,
        "invalid": 0,
    }
    assert all(call[1]["verbose"] is False for call in calls)
    assert all(call[1]["commit"] is False for call in calls)
    assert all(call[1]["record_metrics"] is False for call in calls)
    assert connection.commits == 3
    assert connection.rollbacks == 0


def test_dry_run_rolls_back_the_replay_transaction(tmp_path):
    archive = tmp_path / "Journal.Docked-2026-09-24.jsonl.bz2"
    write_archive(archive, {"message": {"event": "Docked", "Body": "A 1"}})
    connection = FakeConnection()

    counts = replay_archives(
        [archive],
        connection,
        lambda *_args, **_kwargs: SimpleNamespace(status="success"),
        dry_run=True,
    )

    assert counts["processed"] == 1
    assert connection.commits == 0
    assert connection.rollbacks == 1
