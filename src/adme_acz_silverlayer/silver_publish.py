"""Pinned Delta reads and immutable run publication for the reference pipeline."""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone

from .silver import assert_same_rows


def latest_delta_version(spark, path: str) -> int:
    if not isinstance(path, str) or not path:
        raise ValueError("Delta locator must be a nonempty path")
    escaped = path.replace("`", "``")
    row = spark.sql(f"DESCRIBE HISTORY delta.`{escaped}`").select("version").orderBy("version", ascending=False).first()
    if row is None or type(row["version"]) is not int or row["version"] < 0:
        raise ValueError("Delta history did not provide a nonnegative version")
    return row["version"]


def read_pinned_source(spark, path: str):
    """Return a lazy DataFrame bound to one source transaction-log version."""
    path = DeltaRunStore(spark).qualify(path)
    version = latest_delta_version(spark, path)
    frame = spark.read.format("delta").option("versionAsOf", version).load(path)
    return frame, {"source_path": path, "source_delta_version": version}


def _schema_signature(schema):
    """Ignore storage-introduced nullability only, not metadata or structure."""
    if isinstance(schema, dict):
        return {key: value if key == "metadata" else _schema_signature(value) for key, value in schema.items()
                if key not in {"nullable", "containsNull", "valueContainsNull"}}
    if isinstance(schema, list):
        return [_schema_signature(value) for value in schema]
    return schema


class DeltaRunStore:
    """Fabric-compatible Delta I/O and Hadoop filesystem manifest operations."""

    def __init__(self, spark):
        self.spark = spark

    def write(self, frame, path: str) -> int:
        frame.write.format("delta").mode("errorifexists").save(path)
        version = latest_delta_version(self.spark, path)
        if version != 0:
            raise ValueError("Run output is not a new immutable Delta table")
        return version

    def read(self, path: str, version: int):
        return self.spark.read.format("delta").option("versionAsOf", version).load(path)

    def version(self, path: str) -> int:
        return latest_delta_version(self.spark, path)

    def _filesystem(self, path: str):
        java_path = self.spark._jvm.org.apache.hadoop.fs.Path(path)
        return java_path, java_path.getFileSystem(self.spark._jsc.hadoopConfiguration())

    def qualify(self, path: str) -> str:
        """Resolve a locator once against the runtime's filesystem authority."""
        if not isinstance(path, str) or not path:
            raise ValueError("Storage locator must be a nonempty path")
        java_path, filesystem = self._filesystem(path)
        return filesystem.makeQualified(java_path).toString()

    def claim(self, path: str) -> None:
        """Exclusive run ownership; existing failed runs cannot be reused."""
        java_path, filesystem = self._filesystem(path.rstrip("/") + "/_claim")
        if not filesystem.mkdirs(java_path.getParent()) and not filesystem.exists(java_path.getParent()):
            raise OSError("Cannot create the run journal directory")
        stream = filesystem.create(java_path, False)
        stream.close()

    def journal(self, path: str, event: dict) -> None:
        """Expose complete JSON events by renaming a closed temporary file."""
        target, filesystem = self._filesystem(path)
        temporary, _ = self._filesystem(path + "." + uuid.uuid4().hex + ".tmp")
        stream = filesystem.create(temporary, False)
        try:
            stream.write(bytearray(json.dumps(event, sort_keys=True, allow_nan=False).encode("utf-8")))
        finally:
            stream.close()
        if filesystem.exists(target) or not filesystem.rename(temporary, target):
            raise OSError("Cannot publish the run journal event")


def publish_silver(result: dict, source_snapshot: dict, table_root: str, journal_root: str, store=None) -> dict:
    """Publish new run-specific tables; only succeeded.json authorizes consumption.

    No existing table is overwritten and no rollback is attempted. On failure the
    partial run is recorded, then the original error escapes to the caller.
    """
    manifest = result["manifest"]
    if manifest.get("status") != "validated":
        raise ValueError("Only fully validated candidates may be published")
    if not isinstance(source_snapshot.get("source_path"), str) or not source_snapshot["source_path"]:
        raise ValueError("Publication requires the pinned source locator")
    version = source_snapshot.get("source_delta_version")
    if type(version) is not int or version < 0:
        raise ValueError("Publication requires a nonnegative pinned source Delta version")
    if not isinstance(table_root, str) or not isinstance(journal_root, str) or not table_root or not journal_root:
        raise ValueError("Output and journal roots must be nonempty and separate from the source")
    run_id = manifest["run_id"]
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("Invalid publication run ID")
    expected = sorted(result["outputs"])
    if expected != manifest["expected_outputs"] or not expected:
        raise ValueError("Candidate output inventory changed after validation")
    if store is None:
        store = DeltaRunStore(next(iter(result["outputs"].values())).sparkSession)
    table_root, journal_root = store.qualify(table_root), store.qualify(journal_root)
    source_path = store.qualify(source_snapshot["source_path"]).rstrip("/")
    if any(
        root.rstrip("/") == source_path or root.rstrip("/").startswith(source_path + "/")
        for root in (table_root, journal_root)
    ):
        raise ValueError("Output and journal roots must be separate from the source")
    paths = {}
    for name in expected:
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
            raise ValueError("Unsafe logical output table name")
        paths[name] = table_root.rstrip("/") + "/" + name + "__run_" + run_id.replace("-", "_")
    journal_path = journal_root.rstrip("/") + "/" + run_id
    if any(path.rstrip("/") == source_path for path in paths.values()):
        raise ValueError("Run output would overwrite the source")
    event = {
        **manifest, **source_snapshot, "source_path": source_path, "status": "started", "output_paths": paths,
        "output_versions": {}, "attempted_outputs": [],
        "committed_outputs": [], "uncommitted_outputs": expected.copy(),
        "started_at": datetime.now(timezone.utc).isoformat(),
    }
    store.claim(journal_path)
    try:
        store.journal(journal_path + "/started.json", event)
        for index, name in enumerate(expected):
            frame = result["outputs"][name]
            if frame.count() != manifest["row_counts"][name]:
                raise ValueError(f"Candidate row count changed after validation: {name}")
            event["attempted_outputs"].append(name)
            output_version = store.write(frame, paths[name])
            if type(output_version) is not int or output_version != 0:
                raise ValueError("Run outputs must start at Delta version zero")
            event["committed_outputs"].append(name)
            event["uncommitted_outputs"].remove(name)
            event["output_versions"][name] = output_version
            store.journal(journal_path + f"/{index:04d}_committed.json", event)
            actual = store.read(paths[name], output_version)
            if _schema_signature(actual.schema.jsonValue()) != _schema_signature(frame.schema.jsonValue()):
                raise ValueError(f"Committed schema mismatch: {name}")
            assert_same_rows(frame, actual, name)
        if any(store.version(paths[name]) != event["output_versions"][name] for name in expected):
            raise ValueError("Concurrent mutation of a run output; refusing publication")
        event["status"] = "succeeded"
        event["completed_at"] = datetime.now(timezone.utc).isoformat()
        store.journal(journal_path + "/succeeded.json", event)
        return event
    except Exception as error:
        logging.getLogger("adme_silver_reference").exception("Silver publication failed for run %s", run_id)
        event["status"] = "failed"
        event["error_type"] = type(error).__name__
        event["completed_at"] = datetime.now(timezone.utc).isoformat()
        try:
            store.journal(journal_path + "/failed.json", event)
        except Exception:
            logging.getLogger("adme_silver_reference").exception("Cannot persist failure journal for run %s", run_id)
        raise
