"""Copy the local observation lake into MongoDB so scheduled runs share its state.

The local lake is gitignored and therefore exists on exactly one machine. Any
runner without it re-accepts every prior observation as new. This migration is
append-only and idempotent: it never edits or deletes a landed observation, and
re-running it after a partial copy resumes rather than duplicating.

The local files are left in place. Verify the copy before removing anything.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from observation_store import MongoObservationStore, ObservationStore


BASE_DIR = Path(__file__).resolve().parent


def _read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _side_records(lake_dir: Path, kind: str) -> list[dict]:
    """Collect raw or quarantine rows, recovering run_id from the directory layout."""
    root = lake_dir / kind
    if not root.exists():
        return []
    rows = []
    for path in sorted(root.rglob("observations.jsonl")):
        run_id = path.parent.name
        for index, record in enumerate(_read_jsonl(path)):
            if kind == "raw":
                rows.append({"run_id": run_id, "index": index, "record": record})
            else:
                rows.append({"run_id": run_id, **record})
    return rows


def migrate_lake(lake_dir: Path, store: MongoObservationStore, *, apply: bool = False) -> dict:
    local = ObservationStore(lake_dir)
    observations = list(local.read_observations())
    runs = local.read_runs()
    raw = _side_records(lake_dir, "raw")
    quarantine = _side_records(lake_dir, "quarantine")

    existing_observations = store._known_ids()
    existing_runs = {run.get("run_id") for run in store.read_runs()}

    pending_observations = [
        record for record in observations
        if record.get("observation_id") and record["observation_id"] not in existing_observations
    ]
    pending_runs = [run for run in runs if run.get("run_id") not in existing_runs]
    pending_raw = [row for row in raw if row["run_id"] not in existing_runs]
    pending_quarantine = [row for row in quarantine if row["run_id"] not in existing_runs]

    summary = {
        "lakeDir": str(lake_dir),
        "applied": bool(apply),
        "local": {
            "observations": len(observations),
            "runs": len(runs),
            "raw": len(raw),
            "quarantine": len(quarantine),
        },
        "alreadyInMongo": {
            "observations": len(observations) - len(pending_observations),
            "runs": len(runs) - len(pending_runs),
        },
        "pending": {
            "observations": len(pending_observations),
            "runs": len(pending_runs),
            "raw": len(pending_raw),
            "quarantine": len(pending_quarantine),
        },
    }

    if not apply:
        summary["note"] = "Dry run. Re-run with --apply to copy the pending records."
        return summary

    store.ensure_indexes()
    if pending_observations:
        store._collection("normalized").insert_many(pending_observations, ordered=False)
    if pending_raw:
        store._collection("raw").insert_many(pending_raw, ordered=False)
    if pending_quarantine:
        store._collection("quarantine").insert_many(pending_quarantine, ordered=False)
    if pending_runs:
        store._collection("manifests").insert_many(
            [{**run, "storage": "migrated_from_filesystem"} for run in pending_runs], ordered=False
        )

    summary["copied"] = summary["pending"]
    summary["mongoTotals"] = {
        "observations": len(list(store.read_observations())),
        "runs": len(store.read_runs()),
    }
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy the local observation lake into MongoDB")
    parser.add_argument("--lake", type=Path, default=BASE_DIR / "lake")
    parser.add_argument("--uri", help="MongoDB URI; defaults to OBSERVATION_STORE_URI, then URI")
    parser.add_argument("--apply", action="store_true", help="Write the pending records; omit for a dry run")
    args = parser.parse_args()

    uri = args.uri or os.getenv("OBSERVATION_STORE_URI") or os.getenv("URI")
    if not uri:
        raise SystemExit("A MongoDB URI is required through --uri, OBSERVATION_STORE_URI, or URI")
    if not args.lake.exists():
        raise SystemExit(f"Local lake not found at {args.lake}")

    from pymongo import MongoClient

    database = MongoClient(uri, serverSelectionTimeoutMS=10000).get_default_database()
    if database is None:
        raise SystemExit("The MongoDB URI must include a database name")

    summary = migrate_lake(args.lake, MongoObservationStore(database), apply=args.apply)
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
