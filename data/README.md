# Runtime data

The producer writes the replay file, its provenance manifest and SQLite checkpoints to the Compose source volume. Local copies under this directory are ignored by Git.

To collect a replay file outside Compose, install `requirements.txt` and run `python scripts/collect_data.py`. Pass `--end-timestamp 1795111806` to request the same source window as the recorded demo. A later query can return different results as the public index changes.
