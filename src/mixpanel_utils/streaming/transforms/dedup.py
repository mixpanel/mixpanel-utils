"""Content-based deduplication using MurmurHash."""

import json


def dedupe_records(job):
    """Return a transform that skips duplicate records based on content hash.

    Uses MurmurHash v3 on stable JSON stringification. Records with the same
    hash are filtered out (return {}).
    """
    hash_table = job.hash_table  # set()

    def transform(record):
        import mmh3
        h = mmh3.hash(json.dumps(record, sort_keys=True), signed=False)
        if h in hash_table:
            job.duplicates += 1
            return {}
        hash_table.add(h)
        return record

    return transform
