"""Schema-driven, full-snapshot Silver transformation without storage access."""

from __future__ import annotations

import hashlib
import json
import re

from pyspark.sql import DataFrame, Window, functions as F, types as T

from .naming import child_table_name, kind_to_versioned_table_name
from .normalization import make_delta_column_alias
from .schema_contract import compile_schema, parse_json, project_record


REFERENCE_SCHEMA = T.ArrayType(T.StructType([
    T.StructField("source_path", T.StringType()),
    T.StructField("relationship_ordinal_path", T.ArrayType(T.IntegerType())),
    T.StructField("raw_reference", T.StringType()),
    T.StructField("wanted_id", T.StringType()),
    T.StructField("wanted_version", T.StringType()),
    T.StructField("parse_status", T.StringType()),
]))
PROJECTION_SCHEMA = T.StructType([
    T.StructField("json", T.StringType()),
    T.StructField("references", REFERENCE_SCHEMA),
])


def require_empty(frame: DataFrame, message: str) -> None:
    if frame.limit(1).count():
        raise ValueError(message)


def plan_type(plan: dict) -> T.DataType:
    """Translate the explicit projection contract; JSON alternatives stay text."""
    if plan["type"] == "object":
        return T.StructType([
            T.StructField(name, plan_type(child)) for name, child in plan["properties"].items()
        ])
    if plan["type"] == "array":
        return T.ArrayType(plan_type(plan["items"]))
    return {
        "string": T.StringType(), "json": T.StringType(), "null": T.StringType(),
        "integer": T.LongType(), "number": T.DoubleType(), "boolean": T.BooleanType(),
    }[plan["type"]]


def comparable_value(column, data_type):
    if isinstance(data_type, T.MapType):
        entries = F.transform(F.map_entries(column), lambda entry: F.struct(
            comparable_value(entry["key"], data_type.keyType).alias("key"),
            comparable_value(entry["value"], data_type.valueType).alias("value"),
        ))
        return F.array_sort(entries)
    if isinstance(data_type, T.ArrayType):
        return F.transform(column, lambda value: comparable_value(value, data_type.elementType))
    if isinstance(data_type, T.StructType):
        fields = [
            comparable_value(column.getField(field.name), field.dataType).alias(field.name)
            for field in data_type.fields
        ]
        return F.when(column.isNotNull(), F.struct(*fields))
    return column


def comparable_frame(frame: DataFrame) -> DataFrame:
    return frame.select(*[
        comparable_value(F.col("`" + field.name.replace("`", "``") + "`"), field.dataType).alias(field.name)
        for field in frame.schema.fields
    ])


def assert_same_rows(expected: DataFrame, actual: DataFrame, label: str) -> None:
    left, right = comparable_frame(expected), comparable_frame(actual)
    require_empty(left.exceptAll(right), f"Missing or changed rows: {label}")
    require_empty(right.exceptAll(left), f"Extra or changed rows: {label}")


def mark_latest(source: DataFrame) -> DataFrame:
    """Latest is per ID across kinds/schema versions, independent of isActive."""
    expected = {"id": T.StringType, "kind": T.StringType,
                "data": T.StringType, "isActive": T.BooleanType}
    for name, data_type in expected.items():
        if name not in source.columns or not isinstance(source.schema[name].dataType, data_type):
            raise ValueError(f"ACZ source must contain {name} with type {data_type.__name__}")
    if "version" not in source.columns or not (
        isinstance(source.schema["version"].dataType, (T.StringType, T.IntegerType, T.LongType))
        or isinstance(source.schema["version"].dataType, T.DecimalType) and source.schema["version"].dataType.scale == 0
    ):
        raise ValueError("ACZ record version must be a string or exact integer type")
    if len({name.casefold() for name in source.columns}) != len(source.columns):
        raise ValueError("Case-insensitive source column collisions")
    if any(name.casefold().startswith(("_silver_", "osdu__")) or name.casefold() == "osdu" for name in source.columns):
        raise ValueError("ACZ source collides with reserved projection columns")
    invalid = (
        F.col("id").isNull() | ~F.col("id").rlike(r"^[^:]+:[^:]+:.+$") | F.col("version").isNull()
        | ~F.col("version").cast("string").rlike(r"^[0-9]+$") | F.col("kind").isNull()
        | ~F.col("kind").rlike(r"^[^:]+:[^:]+:[^:]+:[0-9]+\.[0-9]+\.[0-9]+$")
        | (F.regexp_extract("id", r"^[^:]+:([^:]+):", 1)
           != F.regexp_extract("kind", r"^[^:]+:[^:]+:([^:]+):", 1))
        | F.col("data").isNull() | F.col("isActive").isNull()
    )
    require_empty(source.where(invalid), "Invalid ACZ identity, kind, payload or deletion state")
    stripped = F.regexp_replace(F.col("version").cast("string"), "^0+", "")
    marked = source.withColumn("_silver_version_key", F.when(stripped == "", "0").otherwise(stripped))
    require_empty(marked.groupBy("id", "_silver_version_key").count().where("count > 1"),
                  "Ambiguous ID/numeric record version across source kinds")
    window = Window.partitionBy("id").orderBy(
        F.length("_silver_version_key").desc(), F.col("_silver_version_key").desc()
    )
    return marked.withColumn("_silver_is_latest", F.row_number().over(window) == 1)


def resolve_relationships(requests: DataFrame, lookup: DataFrame) -> DataFrame:
    """Exact references never fall back to latest; deleted targets remain visible."""
    latest = lookup.where("_silver_is_latest").alias("latest")
    exact = lookup.alias("exact")
    request = requests.alias("request")
    joined = request.join(exact,
        (F.col("request.parse_status") == "pending")
        & (F.col("request.wanted_id") == F.col("exact.id"))
        & (F.col("request.wanted_version") == F.col("exact._silver_version_key")), "left")
    joined = joined.join(latest,
        (F.col("request.parse_status") == "pending")
        & (F.col("request.wanted_id") == F.col("latest.id")), "left")
    explicit = F.col("request.wanted_version").isNotNull()
    selected = {
        name: F.when(explicit, F.col("exact." + field)).otherwise(F.col("latest." + field))
        for name, field in (("target_id", "id"), ("target_version", "version"),
                            ("target_table", "_silver_table"), ("target_is_active", "isActive"))
    }
    status = F.when(F.col("request.parse_status") != "pending", F.col("request.parse_status"))
    status = status.when(selected["target_id"].isNotNull(),
                         F.when(selected["target_is_active"], "resolved").otherwise("target_deleted"))
    status = status.when(explicit & F.col("latest.id").isNotNull(), "version_not_found").otherwise("target_not_loaded")
    return joined.select("request.*", *[value.alias(name) for name, value in selected.items()], status.alias("status"))


def flatten_projection(frame: DataFrame, column_name: str) -> DataFrame:
    """Flatten only the typed projection; preserve raw columns and metadata."""
    base = [name for name in frame.columns if name != column_name]
    used = set(base)
    columns = [F.col("`" + name.replace("`", "``") + "`") for name in base]

    def walk(value, data_type, path):
        if isinstance(data_type, T.StructType):
            for field in data_type.fields:
                walk(value.getField(field.name), field.dataType, path + (field.name,))
        elif isinstance(data_type, T.ArrayType):
            return
        else:
            alias = make_delta_column_alias("__".join(path), used)
            if alias.casefold() in {name.casefold() for name in base}:
                raise ValueError(f"Typed projection collides with raw column: {alias}")
            columns.append(value.alias(alias))

    walk(F.col(column_name), frame.schema[column_name].dataType, (column_name,))
    result = frame.select(*columns)
    if len({name.casefold() for name in result.columns}) != len(result.columns):
        raise ValueError("Case-insensitive projected column collision")
    return result


def normalize_children(typed: DataFrame, plan: dict, parent_table: str) -> dict[str, DataFrame]:
    """One child per typed array; full ancestry disambiguates nested ordinals."""
    identity = ["id", "version", "kind", "isActive", "_silver_is_latest", "_silver_run_id"]
    outputs = {}
    paths = {}
    initial = typed.select(*identity, "_silver_record").withColumn(
        "_silver_ordinal_path", F.array().cast("array<int>")
    )

    def walk(frame, value, node, path):
        if node["type"] == "object":
            for name, child in node["properties"].items():
                walk(frame, value.getField(name), child, path + (name,))
        elif node["type"] == "array":
            expanded = frame.select(
                *identity, F.col("_silver_ordinal_path").alias("_parent_ordinals"),
                F.posexplode(value).alias("ordinal", "_silver_element"),
            ).withColumn("_silver_ordinal_path", F.concat("_parent_ordinals", F.array("ordinal")))
            table = child_table_name(parent_table, ".".join(path))
            if table in paths and paths[table] != path:
                raise ValueError(f"Normalized child name collision: {table}")
            paths[table] = path
            child = expanded.select(
                *identity, "ordinal", "_silver_ordinal_path",
                F.col("_silver_element").isNull().alias("_silver_element_is_null"),
                F.col("_silver_element").alias("value")
            )
            outputs[table] = flatten_projection(child, "value")
            walk(expanded, F.col("_silver_element"), node["items"], path + ("items",))

    walk(initial, F.col("_silver_record"), plan, ())
    return outputs


def build_silver(source: DataFrame, schemas: dict[str, dict], run_id: str, prefix: str = "gen_silver_") -> dict:
    """Build and validate all candidates before a caller can publish any table."""
    if not isinstance(run_id, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", run_id):
        raise ValueError("Run ID must be a bounded filename-safe identifier")
    if not isinstance(prefix, str) or not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*_", prefix):
        raise ValueError("Output prefix must be an SQL identifier ending in underscore")
    marked = mark_latest(source)
    kinds = sorted(row["kind"] for row in marked.select("kind").distinct().collect())
    if not kinds:
        raise ValueError("Source snapshot has no kinds; refusing an ambiguous empty rebuild")
    if not isinstance(schemas, dict) or not set(kinds).issubset(schemas):
        raise ValueError("Exact schemas are required for every source kind")
    plans = {kind: compile_schema(schemas[kind], kind) for kind in kinds}
    table_names = {kind: prefix + kind_to_versioned_table_name(kind) for kind in kinds}
    if len(set(table_names.values())) != len(kinds):
        raise ValueError("Schema kinds collide under the output naming contract")
    root_lookup = marked.select("id", "version", "_silver_version_key", "_silver_is_latest", "isActive", "kind")
    table_expression = F.create_map(*[item for kind, table in table_names.items() for item in (F.lit(kind), F.lit(table))])
    lookup = root_lookup.withColumn("_silver_table", table_expression[F.col("kind")])
    outputs, root_sources, references = {}, {}, []
    try:
        for kind in kinds:
            plan = plans[kind]
            projection = {**plan, "properties": {
                name: node for name, node in plan["properties"].items() if name not in {"id", "version", "kind"}
            }}
            raw = marked.where(F.col("kind") == kind).withColumn("_silver_run_id", F.lit(run_id))

            def project(payload, projection_plan=plan):
                return project_record(parse_json(payload), projection_plan)

            payload = F.to_json(
                F.struct(*[F.col("`" + name.replace("`", "``") + "`") for name in source.columns]),
                options={"ignoreNullFields": "false", "timeZone": "UTC",
                         "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"},
            )
            typed = raw.withColumn("_silver_projection",
                F.udf(project, PROJECTION_SCHEMA)(payload)
            ).withColumn("_silver_record", F.from_json(F.col("_silver_projection.json"), plan_type(projection)))
            parent = typed.select(
                *[F.col("`" + name.replace("`", "``") + "`") for name in source.columns],
                "_silver_is_latest", "_silver_run_id", F.col("_silver_record").alias("osdu"),
            )
            table = table_names[kind]
            outputs[table] = flatten_projection(parent, "osdu").persist()
            root_sources[table] = source.where(F.col("kind") == kind)
            children = normalize_children(typed, projection, table)
            for child_name, child in children.items():
                if child_name in outputs:
                    raise ValueError(f"Output naming collision: {child_name}")
                outputs[child_name] = child.persist()
            occurrences = typed.select(
                F.lit(run_id).alias("run_id"), F.lit(table).alias("source_table"),
                F.col("id").alias("source_id"), F.col("version").alias("source_version"),
                F.col("kind").alias("source_kind"), F.col("isActive").alias("source_is_active"),
                F.col("_silver_is_latest").alias("source_is_latest"),
                F.explode("_silver_projection.references").alias("reference"),
            ).select("run_id", "source_table", "source_id", "source_version", "source_kind",
                     "source_is_active", "source_is_latest", "reference.*")
            references.append(occurrences)
        requests = references[0]
        for frame in references[1:]:
            requests = requests.unionByName(frame)
        relationships = resolve_relationships(requests, lookup).withColumn("_silver_run_id", F.lit(run_id))
        bridge_name = prefix + "osdu_relationships"
        if bridge_name in outputs:
            raise ValueError("Relationship bridge collides with a source-derived output")
        outputs[bridge_name] = relationships.persist()
        counts = {name: frame.count() for name, frame in outputs.items()}
        for name, raw in root_sources.items():
            candidate = outputs[name].select(*[F.col("`" + column.replace("`", "``") + "`") for column in source.columns])
            if candidate.schema != raw.schema:
                raise ValueError(f"Source schema changed: {name}")
            assert_same_rows(raw, candidate, name)
        manifest = {
            "run_id": run_id, "status": "validated", "output_prefix": prefix,
            "expected_outputs": sorted(outputs), "row_counts": counts,
            "schema_sha256": {kind: hashlib.sha256(json.dumps(schemas[kind], sort_keys=True).encode()).hexdigest() for kind in kinds},
            "version_contract": "all versions present in source; latest per ID; deletion state preserved",
            "unversioned_reference_policy": "latest available target, including diagnostic deleted state",
            "local_pointer_policy": "preserved as source data; no inferred owner-scoped resolution",
        }
        return {"outputs": outputs, "manifest": manifest}
    except Exception:
        for frame in outputs.values():
            frame.unpersist()
        raise


def release_silver(result: dict) -> None:
    for frame in result["outputs"].values():
        frame.unpersist()
