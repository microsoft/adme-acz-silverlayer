"""Regenerate an ACZ `osducatalog` bronze Delta table from the public OSDU TNO open test data.

Source: https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data
(`rc--3.0.0/4-instances/TNO/master-data`), pinned to a commit so output is reproducible.
The manifests carry placeholder ACL and legal values; no tenant data is involved.

    python tests/tno_bronze.py --output ./tno-osducatalog --wells 25 --wellbores 25 --partition dp1
"""

import argparse
import json
import re
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

PROJECT = "osdu%2Fplatform%2Fdata-flow%2Fdata-loading%2Fopen-test-data"
COMMIT = "356d72859755b812ac68f767650b3f69ecd048a3"
TNO_MASTER_DATA = "rc--3.0.0/4-instances/TNO/master-data"
API = f"https://community.opengroup.org/api/v4/projects/{PROJECT}"
RAW = f"https://community.opengroup.org/osdu/platform/data-flow/data-loading/open-test-data/-/raw/{COMMIT}"

BRONZE_DDL = (
    "data string, meta string, id string, version string, kind string, acl string, legal string, tags string, "
    "createUser string, createTime timestamp, modifyUser string, modifyTime timestamp, ingestTime timestamp, "
    "fileDownloadTime timestamp, fileDownloadState string, fileDownloadFolder string, isActive boolean"
)
FIXTURE_USER = "tno-fixture"
FIXTURE_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)
RECORD_VERSION = "1700000000000000"
# Manifest ids and references use the placeholder authority "osdu:"; ADME stores the data partition id instead.
_PARTITION_REFERENCE = re.compile(r"osdu:(?=(?:master-data|reference-data|work-product|work-product-component|dataset)--)")


def _get(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def list_manifest_files(entity: str, limit: int) -> list[str]:
    """Return the first `limit` manifest file names for a TNO master-data folder, in name order."""
    names: list[str] = []
    page = 1
    while True:
        query = urllib.parse.urlencode({"path": f"{TNO_MASTER_DATA}/{entity}", "ref": COMMIT, "per_page": 100, "page": page})
        batch = json.loads(_get(f"{API}/repository/tree?{query}"))
        if not batch:
            break
        names.extend(item["name"] for item in batch if item["type"] == "blob" and item["name"].endswith(".json"))
        page += 1
    return sorted(names)[:limit]


def fetch_manifest(entity: str, name: str, cache: Path | None) -> dict:
    cached = cache / entity / name if cache else None
    if cached and cached.exists():
        return json.loads(cached.read_text(encoding="utf-8"))
    url = f"{RAW}/{TNO_MASTER_DATA}/{urllib.parse.quote(entity)}/{urllib.parse.quote(name)}"
    body = _get(url)
    if cached:
        cached.parent.mkdir(parents=True, exist_ok=True)
        cached.write_bytes(body)
    return json.loads(body)


def manifest_records(entity: str, limit: int, cache: Path | None = None) -> list[dict]:
    names = list_manifest_files(entity, limit)
    with ThreadPoolExecutor(max_workers=8) as pool:
        manifests = list(pool.map(lambda name: fetch_manifest(entity, name, cache), names))
    return [record for manifest in manifests for record in manifest["MasterData"]]


def bronze_row(record: dict, partition: str) -> tuple:
    """Build one bronze row whose `data` column holds the Storage record envelope, as ACZ delivers it."""
    def localize(value) -> str:
        return _PARTITION_REFERENCE.sub(f"{partition}:", json.dumps(value))

    modify_ms = int(FIXTURE_TIME.timestamp() * 1000)
    envelope = {"data": record["data"], "meta": record.get("meta"), "modifyUser": FIXTURE_USER, "modifyTime": modify_ms}
    return (
        localize(envelope),
        localize(record["meta"]) if record.get("meta") else None,
        _PARTITION_REFERENCE.sub(f"{partition}:", record["id"]),
        RECORD_VERSION,
        record["kind"],
        json.dumps(record.get("acl")),
        json.dumps(record.get("legal")),
        None,
        FIXTURE_USER,
        FIXTURE_TIME,
        FIXTURE_USER,
        FIXTURE_TIME,
        FIXTURE_TIME,
        None,
        None,
        None,
        True,
    )


def build_rows(wells: int, wellbores: int, partition: str, cache: Path | None = None) -> list[tuple]:
    records = manifest_records("Well", wells, cache) + manifest_records("Wellbore", wellbores, cache)
    return [bronze_row(record, partition) for record in records]


def write_bronze(spark, path: str, wells: int = 25, wellbores: int = 25, partition: str = "dp1", cache: Path | None = None) -> int:
    rows = build_rows(wells, wellbores, partition, cache)
    spark.createDataFrame(rows, BRONZE_DDL).write.format("delta").mode("overwrite").save(path)
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", required=True, help="Directory for the bronze Delta table.")
    parser.add_argument("--wells", type=int, default=25)
    parser.add_argument("--wellbores", type=int, default=25)
    parser.add_argument("--partition", default="dp1", help="Data partition id substituted for the manifests' 'osdu:' authority.")
    parser.add_argument("--cache", help="Optional directory caching downloaded manifests.")
    args = parser.parse_args()

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]").appName("tno-bronze")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    count = write_bronze(spark, str(Path(args.output).resolve()), args.wells, args.wellbores, args.partition,
                         Path(args.cache) if args.cache else None)
    print(f"Wrote {count} bronze rows to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
