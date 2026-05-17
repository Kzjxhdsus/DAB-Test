"""
framework_variant.py
--------------------
VARIANT-native Bronze → Bronze flattening framework for Databricks Job Compute.

Uses the SAME config format and runner as flatten_framework.py.
NOT compatible with Databricks Serverless — use flatten_framework.py there.

Key differences from flatten_framework.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* No variant_to_str UDF        — VARIANT columns are used directly throughout.
* No _json_split_array UDF     — variant_explode() handles array explosion natively.
* No from_json                 — try_variant_get() extracts typed fields directly from VARIANT.
* No Python sampling loop      — schema_of_variant_agg() infers types from the full
                                  VARIANT column in one Spark aggregation.

NOTE: VARIANT function APIs (variant_get, variant_explode, schema_of_variant_agg)
are Databricks SQL functions called via F.expr() / selectExpr() throughout.
Verify exact behaviour on your job compute cluster before production use.
"""

import re
import json
import traceback
from collections import defaultdict

# Increment this whenever the file is changed so the runner print confirms
# which version is actually loaded on the cluster.
_VERSION = "15"

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType


# ---------------------------------------------------------------------------
# Utilities  (self-contained — no flatten_framework import, no UDFs)
# ---------------------------------------------------------------------------

def to_snake_case(name: str) -> str:
    """Convert camelCase / PascalCase to snake_case."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", s)
    return s.lower()


def deduplicate(df: DataFrame, dedup_keys: list) -> DataFrame:
    """Keep only the most-recently-ingested row per unique dedup_keys combination."""
    window = Window.partitionBy(*dedup_keys).orderBy(F.col("ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


def write_table(df: DataFrame, table_name: str,
                write_mode: str = "overwrite", spark: SparkSession = None,
                source_table: str = ""):
    """Write df to a Delta table, stamping four lineage columns on every row."""
    def _parse(fq_name: str):
        parts = fq_name.split(".")
        return (parts[-2] if len(parts) >= 2 else ""), (parts[-1] if parts else fq_name)

    src_schema, src_table = _parse(source_table) if source_table else ("", "")
    tgt_schema, tgt_table = _parse(table_name)

    df = (
        df
        .withColumn("source_schema",     F.lit(src_schema))
        .withColumn("source_table_name", F.lit(src_table))
        .withColumn("target_schema",     F.lit(tgt_schema))
        .withColumn("target_table_name", F.lit(tgt_table))
    )
    (
        df.write
          .format("delta")
          .mode(write_mode)
          .option("overwriteSchema", "true")
          .saveAsTable(table_name)
    )
    print(
        f"[write_table] {table_name} → "
        f"{df.count():,} rows × {len(df.columns)} columns "
        f"(mode={write_mode})"
    )


def create_error_log_table(table_name: str, spark: SparkSession):
    """Create the pipeline error log Delta table if it does not already exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id          STRING     COMMENT 'UUID identifying one full pipeline execution',
            run_timestamp   TIMESTAMP  COMMENT 'UTC timestamp when the error was captured',
            pipeline_step   STRING     COMMENT 'Config name that failed',
            error_message   STRING     COMMENT 'Python exception message',
            error_traceback STRING     COMMENT 'Full stack trace'
        )
        USING DELTA
        COMMENT 'Persistent log of NRL Flattening Framework pipeline failures'
    """)
    print(f"[create_error_log_table] Ready: {table_name}")


def log_pipeline_error(table_name: str, run_id: str, pipeline_step: str,
                       exc: Exception, spark: SparkSession):
    """Append one error row to the pipeline error log table."""
    from datetime import datetime

    _schema = T.StructType([
        T.StructField("run_id",          T.StringType(),    False),
        T.StructField("run_timestamp",   T.TimestampType(), False),
        T.StructField("pipeline_step",   T.StringType(),    False),
        T.StructField("error_message",   T.StringType(),    True),
        T.StructField("error_traceback", T.StringType(),    True),
    ])
    error_row = spark.createDataFrame(
        [(run_id, datetime.utcnow(), pipeline_step, str(exc), traceback.format_exc())],
        schema=_schema,
    )
    error_row.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"[log_pipeline_error] Logged: step='{pipeline_step}' → {table_name}")


def create_audit_table(table_name: str, spark: SparkSession):
    """Create the pipeline audit Delta table if it does not already exist."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id       STRING     COMMENT 'UUID identifying one full pipeline execution',
            flattened_at TIMESTAMP  COMMENT 'UTC timestamp when the flattening pipeline ran',
            config_name  STRING     COMMENT 'Config file used',
            source_table STRING     COMMENT 'Bronze Delta table that was flattened',
            ingested_at  TIMESTAMP  COMMENT 'MAX(ingested_at) from the bronze table'
        )
        USING DELTA
        COMMENT 'Audit log of NRL Flattening Framework successful pipeline runs'
    """)
    print(f"[create_audit_table] Ready: {table_name}")


def log_pipeline_audit(table_name: str, run_id: str, config_name: str,
                       config: dict, spark: SparkSession):
    """Append one audit row to the pipeline audit table after a successful run."""
    from datetime import datetime

    bronze_table    = config.get("bronze_table", "unknown")
    bronze_filter   = config.get("bronze_filter")
    ingested_at_val = None
    if bronze_table and bronze_table != "unknown":
        try:
            _where = f"WHERE {bronze_filter}" if bronze_filter else ""
            ingested_at_val = (
                spark.sql(f"SELECT MAX(ingested_at) AS max_ia FROM {bronze_table} {_where}")
                     .collect()[0]["max_ia"]
            )
        except Exception as _e:
            print(f"[log_pipeline_audit] Could not read bronze metadata: {_e}")

    _schema = T.StructType([
        T.StructField("run_id",       T.StringType(),    False),
        T.StructField("flattened_at", T.TimestampType(), False),
        T.StructField("config_name",  T.StringType(),    False),
        T.StructField("source_table", T.StringType(),    True),
        T.StructField("ingested_at",  T.TimestampType(), True),
    ])
    audit_row = spark.createDataFrame(
        [(run_id, datetime.utcnow(), config_name, bronze_table, ingested_at_val)],
        schema=_schema,
    )
    audit_row.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"[log_pipeline_audit] Logged: config='{config_name}' → {table_name}")



# ---------------------------------------------------------------------------
# 1.  VARIANT DDL parser
#     schema_of_variant_agg returns:
#       OBJECT<fieldName TYPE, fieldName ARRAY<OBJECT<...>>, ...>
#     (space-separated name/type, OBJECT not STRUCT, no colons)
# ---------------------------------------------------------------------------

# Mapping from VARIANT DDL type keywords → PySpark DataType
_VARIANT_TYPE_MAP = {
    "BIGINT":        T.LongType(),
    "INT":           T.IntegerType(),
    "INTEGER":       T.IntegerType(),
    "SMALLINT":      T.ShortType(),
    "TINYINT":       T.ByteType(),
    "DOUBLE":        T.DoubleType(),
    "FLOAT":         T.FloatType(),
    "STRING":        StringType(),
    "BOOLEAN":       T.BooleanType(),
    "TIMESTAMP":     T.TimestampType(),
    "TIMESTAMP_NTZ": T.TimestampNTZType(),
    "DATE":          T.DateType(),
    "BINARY":        T.BinaryType(),
    "VOID":          StringType(),
    "VARIANT":       StringType(),   # nested VARIANT treated as string
}


def _parse_variant_ddl(ddl: str) -> StructType:
    """
    Parse a Databricks VARIANT schema DDL string into a PySpark StructType.

    Actual format returned by schema_of_variant_agg (confirmed from runtime):
        ARRAY<OBJECT<fieldName: TYPE, fieldName: ARRAY<OBJECT<...>>>>

    Two things differ from the originally assumed format:
    1. Fields use "name: TYPE" (colon) not "name TYPE" (space).
    2. Root is ARRAY<OBJECT<...>> not OBJECT<...> when payload is an array
       of entities — the ARRAY wrapper is unwrapped automatically so the
       returned StructType represents one element, not the whole array.

    Returns
    -------
    StructType
        One-element schema — fields of a single entity at this nesting level.
    """
    ddl = ddl.strip()

    # Unwrap ARRAY<OBJECT<...>> at the root — payload is an array of entities,
    # we want the schema of one entity, not the array itself.
    if ddl.upper().startswith("ARRAY<"):
        inner = ddl[6:-1].strip()
        if inner.upper().startswith("OBJECT<"):
            ddl = inner   # proceed with OBJECT<...>

    def _split_fields(inner: str) -> list:
        """
        Split a comma-separated field list respecting nested angle brackets.
        e.g. "a: INT, b: ARRAY<OBJECT<x: INT>>, c: STRING"
        """
        parts, depth, current = [], 0, ""
        for ch in inner:
            if ch == "<":
                depth += 1
                current += ch
            elif ch == ">":
                depth -= 1
                current += ch
            elif ch == "," and depth == 0:
                parts.append(current.strip())
                current = ""
            else:
                current += ch
        if current.strip():
            parts.append(current.strip())
        return parts

    def _parse_type(type_str: str):
        type_str = type_str.strip()
        if type_str.upper().startswith("OBJECT<"):
            return _parse_object(type_str[7:-1])
        if type_str.upper().startswith("ARRAY<"):
            elem_type = _parse_type(type_str[6:-1])
            return T.ArrayType(elem_type, True)
        # Scalar — strip precision qualifiers e.g. DECIMAL(10,2)
        base = type_str.split("(")[0].strip().upper()
        return _VARIANT_TYPE_MAP.get(base, StringType())

    def _parse_object(inner: str) -> StructType:
        fields = []
        for field_str in _split_fields(inner):
            # Actual format: "fieldName: TYPE_EXPRESSION" (colon separator)
            colon_idx = field_str.find(":")
            if colon_idx == -1:
                continue
            name     = field_str[:colon_idx].strip()
            type_str = field_str[colon_idx + 1:].strip()
            fields.append(StructField(name, _parse_type(type_str), True))
        return StructType(fields)

    if ddl.upper().startswith("OBJECT<"):
        return _parse_object(ddl[7:-1])

    # Fallback: treat whole thing as a single string field
    print(f"[_parse_variant_ddl] WARNING: unrecognised DDL root, got: {ddl[:100]}")
    return StructType([StructField("_value", StringType(), True)])



# ---------------------------------------------------------------------------
# 2.  Spark type → SQL string  (used in variant_get calls)
# ---------------------------------------------------------------------------

def _sql_type_str(dtype) -> str:
    """Convert a PySpark DataType to its SQL keyword for variant_get."""
    if isinstance(dtype, T.LongType):      return "BIGINT"
    if isinstance(dtype, T.IntegerType):   return "INT"
    if isinstance(dtype, T.ShortType):     return "SMALLINT"
    if isinstance(dtype, T.ByteType):      return "TINYINT"
    if isinstance(dtype, T.DoubleType):    return "DOUBLE"
    if isinstance(dtype, T.FloatType):     return "FLOAT"
    if isinstance(dtype, T.BooleanType):   return "BOOLEAN"
    if isinstance(dtype, T.TimestampType): return "TIMESTAMP"
    if isinstance(dtype, T.DateType):      return "DATE"
    return "STRING"



# ---------------------------------------------------------------------------
# 3.  Array discovery from StructType  (replaces sample-based _discover_arrays)
# ---------------------------------------------------------------------------

def _discover_arrays(schema: StructType, exclude_fields: set,
                     force_array_fields: set = None) -> list:
    """
    Walk a StructType and return every nested array field as a dict:
        {"field": "dot.path", "alias": "leafName", "schema": StructType, "force_single": bool}

    force_array_fields handles the XML single-element pattern — a field that
    schema_of_variant_agg inferred as OBJECT (because only one element was ever
    seen) but should be treated as an array.

    Unlike the sample-based version, this is exact — the full dataset was
    scanned by schema_of_variant_agg so sparse fields are never missed.
    """
    _forced = force_array_fields or set()
    found   = []

    def _walk(st: StructType, prefix: str):
        for field in st.fields:
            if field.name in exclude_fields:
                continue
            path = f"{prefix}.{field.name}" if prefix else field.name

            if isinstance(field.dataType, T.ArrayType):
                elem = field.dataType.elementType
                if isinstance(elem, StructType):
                    found.append({
                        "field":        path,
                        "alias":        field.name,
                        "schema":       elem,
                        "force_single": False,
                    })

            elif isinstance(field.dataType, StructType):
                if field.name in _forced:
                    # Treat single-occurrence OBJECT as a one-element array.
                    found.append({
                        "field":        path,
                        "alias":        field.name,
                        "schema":       field.dataType,
                        "force_single": True,
                    })
                else:
                    # Transparent struct wrapper — walk into it.
                    _walk(field.dataType, path)

            elif isinstance(field.dataType, StringType) and field.name in _forced:
                # VARIANT-typed field: schema_of_variant_agg returned VARIANT for
                # this field (e.g. mixed array/object across rows, or always-array
                # that the aggregation couldn't resolve to a concrete type).
                # VARIANT maps to StringType in _VARIANT_TYPE_MAP.
                # Defer schema inference to _process_node execution time when we
                # have actual data rows to run schema_of_variant_agg against.
                found.append({
                    "field":                  path,
                    "alias":                  field.name,
                    "schema":                 StructType([]),
                    "force_single":           True,   # tentative; updated after inference
                    "needs_schema_inference": True,
                })

    _walk(schema, "")
    return found



# ---------------------------------------------------------------------------
# 4.  Schema tree builder  (same structure as flatten_framework, built from
#     StructType instead of Python samples)
# ---------------------------------------------------------------------------

def _build_tree_node(alias: str, schema: StructType, exclude_fields: set,
                     target_prefix: str, dedup_keys_map: dict,
                     force_array_fields: set = None,
                     write_mode: str = "overwrite") -> dict:
    """
    Recursively build a schema_tree node from a StructType.

    Identical structure to flatten_framework._build_tree_node so _process_node
    can navigate it the same way; the only difference is that 'schema' is a
    proper StructType (typed) rather than an all-string schema.
    """
    children = []
    for info in _discover_arrays(schema, exclude_fields, force_array_fields):
        child_node = _build_tree_node(
            alias              = info["alias"],
            schema             = info["schema"],
            exclude_fields     = exclude_fields,
            target_prefix      = target_prefix,
            dedup_keys_map     = dedup_keys_map,
            force_array_fields = force_array_fields,
            write_mode         = write_mode,
        )
        child_node["field"]        = info["field"]
        child_node["force_single"] = info["force_single"]
        # Propagate needs_schema_inference so _process_node triggers lazy
        # schema inference for VARIANT-typed force_array_fields.
        if info.get("needs_schema_inference"):
            child_node["needs_schema_inference"] = True
        children.append(child_node)

    return {
        "alias":        alias,
        "target_table": f"{target_prefix}_{alias}",
        "dedup_keys":   dedup_keys_map.get(alias, []),
        "write_mode":   write_mode,
        "arrays":       children,
        "schema":       schema,   # carries typed StructType for field extraction
    }



# ---------------------------------------------------------------------------
# 5.  Recursive node processor  (variant_get + variant_explode)
# ---------------------------------------------------------------------------

def _process_node(node: dict, df: DataFrame, variant_col: str,
                  flatten_structs: dict, exclude_fields: set,
                  ancestor_col_names: list, spark: SparkSession,
                  source_table: str = "",
                  target_prefix: str = "",
                  dedup_keys_map: dict = None,
                  force_array_fields: set = None,
                  exclude_root_aliases: set = None,
                  passthrough_fields: set = None):
    """
    Recursively process one schema_tree node.

    At each level:
    1. Extract scalar fields from *variant_col* using try_variant_get().
    2. Select ancestor_cols + scalar_cols and write to node["target_table"].
    3. For every child array: navigate with variant_get → VARIANT, then
       cast to array<variant> + explode → one VARIANT row per element, recurse.

    try_variant_get(col, path, type) extracts a typed value from a VARIANT column.
    Both are Databricks SQL functions called via F.expr() / selectExpr().

    target_prefix / dedup_keys_map / force_array_fields are threaded through
    so that VARIANT-typed force_array_fields children can run lazy schema
    inference (schema_of_variant_agg) and rebuild their subtree at runtime.
    """
    schema      = node.get("schema", StructType([]))
    child_arrays = node.get("arrays", [])
    child_roots  = {child["field"].split(".")[0] for child in child_arrays}
    level_exclude = exclude_fields | child_roots

    # ---- Extract scalar fields via variant_get --------------------------------
    enriched_df  = df
    scalar_names = []

    for field in schema.fields:
        if field.name in level_exclude:
            continue

        if isinstance(field.dataType, T.ArrayType):
            continue  # child arrays handled by recursion

        if isinstance(field.dataType, StructType):
            if field.name in flatten_structs:
                sub_prefix = flatten_structs[field.name]
                for sf in field.dataType.fields:
                    if sf.name in exclude_fields:
                        continue
                    col_name = sub_prefix + to_snake_case(sf.name)
                    type_str = _sql_type_str(sf.dataType)
                    enriched_df = enriched_df.withColumn(
                        col_name,
                        F.expr(
                            f"try_variant_get(`{variant_col}`, "
                            f"'$.{field.name}.{sf.name}', '{type_str}')"
                        ),
                    )
                    scalar_names.append(col_name)
            # Non-flatten structs skipped — handled as child arrays
            continue

        col_name = to_snake_case(field.name)
        type_str = _sql_type_str(field.dataType)
        enriched_df = enriched_df.withColumn(
            col_name,
            F.expr(
                f"try_variant_get(`{variant_col}`, '$.{field.name}', '{type_str}')"
            ),
        )
        scalar_names.append(col_name)

    # ---- Write this level's table --------------------------------------------
    target_table  = node.get("target_table")
    _node_alias   = node.get("alias", "")
    _skip_root    = _node_alias in (exclude_root_aliases or set())
    if target_table and not _skip_root:
        all_col_names = ancestor_col_names + scalar_names
        table_df      = enriched_df.select(*[F.col(n) for n in all_col_names])
        dedup_keys    = node.get("dedup_keys", [])
        if dedup_keys:
            table_df = deduplicate(table_df, dedup_keys)
        write_table(table_df, target_table,
                    node.get("write_mode", "overwrite"), spark,
                    source_table=source_table)
    elif _skip_root:
        print(f"[_process_node] alias='{_node_alias}' in exclude_root_aliases — root table write suppressed")

    # ---- FK propagation ------------------------------------------------------
    fk_names      = [k for k in node.get("dedup_keys", []) if k in scalar_names]
    all_inherited = ancestor_col_names + fk_names

    # If passthrough_fields is set, restrict which columns flow to child tables.
    # Columns not in the set stay in this level's table but are not propagated.
    # If passthrough_fields is None (default), all columns flow through as before.
    if passthrough_fields is not None:
        all_inherited = [c for c in all_inherited if c in passthrough_fields]

    # ---- Recurse into child arrays -------------------------------------------
    for child in child_arrays:
        child_alias = child["alias"]

        # --- Lazy schema inference for VARIANT-typed force_array_fields -------
        # When schema_of_variant_agg couldn't resolve a consistent concrete type
        # for a field (e.g. it's sometimes array, sometimes object across rows)
        # it returns VARIANT, which _parse_variant_ddl maps to StringType.
        # _discover_arrays marks such fields with needs_schema_inference=True.
        # Here we re-run schema_of_variant_agg scoped to just that field's
        # VARIANT values so we get the real child schema, then rebuild the
        # subtree node with proper target_table / dedup_keys / child arrays.
        if child.get("needs_schema_inference"):
            _field_path = child["field"]   # may be dot-path, e.g. "competitions.competition"
            _raw_col    = f"_{child_alias}_raw_v"
            _elem_col   = f"_{child_alias}_elem_v"

            # The VARIANT field may be an array (multi-element) or an object
            # (XML single-element) depending on the row.  schema_of_variant_agg
            # on a mixed-type column returns VARIANT again — useless for schema
            # discovery.  Fix: normalise each value to a single element first.
            #   - If it's an array  → take element [0] as the schema sample.
            #   - If it's an object → use it directly.
            # schema_of_variant_agg then sees consistent OBJECT<...> values and
            # can infer a proper typed schema.
            #
            # IMPORTANT: use the raw `df` (input to _process_node, before scalar
            # extraction) rather than `enriched_df` (which carries many extra
            # withColumn projections).  Using enriched_df forces Spark to
            # materialise scalar columns (cast(variant as string), etc.) that are
            # not needed for schema inference and can trigger Photon's
            # INVALID_VARIANT_CAST on empty-variant rows.
            infer_df = (
                df
                .withColumn(
                    _raw_col,
                    F.expr(
                        f"try_variant_get(`{variant_col}`, '$.{_field_path}', 'VARIANT')"
                    ),
                )
                .filter(F.col(_raw_col).isNotNull())
                .withColumn(
                    _elem_col,
                    F.expr(
                        # Re-use the try_variant_get probe result directly rather
                        # than cast(raw as array<variant>)[0].
                        # Photon evaluates ALL CASE branches eagerly (branch
                        # speculation), so cast(object_variant as array<variant>)
                        # throws INVALID_VARIANT_CAST even when guarded by WHEN.
                        # try_variant_get(raw, '$[0]', 'VARIANT') is cast-free and
                        # returns NULL safely for both empty and non-array VARIANTs.
                        f"CASE"
                        f"  WHEN try_variant_get(`{_raw_col}`, '$[0]', 'VARIANT') IS NOT NULL"
                        f"  THEN try_variant_get(`{_raw_col}`, '$[0]', 'VARIANT')"
                        f"  ELSE `{_raw_col}`"
                        f" END"
                    ),
                )
                .filter(F.col(_elem_col).isNotNull())
            )
            if infer_df.isEmpty():
                print(
                    f"[_process_node] skipping '{child_alias}' — "
                    f"all rows null, cannot infer schema."
                )
                continue

            inferred_ddl = (
                infer_df
                .agg(F.expr(f"schema_of_variant_agg(`{_elem_col}`)").alias("_ddl"))
                .collect()[0]["_ddl"]
            )
            print(
                f"[_process_node] lazy DDL for '{child_alias}': "
                f"{inferred_ddl[:300]}"
            )
            inferred_schema = _parse_variant_ddl(inferred_ddl)

            rebuilt = _build_tree_node(
                alias              = child_alias,
                schema             = inferred_schema,
                exclude_fields     = exclude_fields,
                target_prefix      = target_prefix,
                dedup_keys_map     = dedup_keys_map or {},
                force_array_fields = force_array_fields or set(),
            )
            rebuilt["field"]                  = child["field"]
            # Always use coerce_to_array extraction (handles both array and
            # single-object shapes without needing to know which shape each row is).
            rebuilt["force_single"]           = False
            rebuilt["coerce_to_array"]        = True
            rebuilt["needs_schema_inference"] = False
            child = rebuilt   # use updated node for the rest of this iteration

        field_path   = child["field"]
        force_single = child.get("force_single", False)

        _child_variant_col = f"_{child_alias}_v"

        # --- Pre-check: verify the child array path has non-null data --------
        # Using child_df.isEmpty() (which derives from enriched_df) evaluates
        # ALL scalar try_variant_get(…, 'STRING') projections in enriched_df.
        # Photon throws INVALID_VARIANT_CAST on those even through TRY for
        # zero-byte / malformed VARIANT rows (same issue as the infer_df guard
        # above).  Using bare `df` here means only the single path-navigation
        # withColumn is evaluated — no STRING casts, no Photon bug.
        _prechk_v = f"_{child_alias}_prechk_v"
        if (
            df
            .withColumn(
                _prechk_v,
                F.expr(
                    f"try_variant_get(`{variant_col}`, '$.{field_path}', 'VARIANT')"
                ),
            )
            .filter(F.col(_prechk_v).isNotNull())
            .isEmpty()
        ):
            print(
                f"[_process_node] skipping '{child_alias}' subtree — "
                f"0 rows from '$.{field_path}'"
            )
            continue

        if force_single:
            # XML single-element pattern: field is a known OBJECT, not an ARRAY.
            # Extract it as a VARIANT directly — no explode needed.
            child_df = enriched_df.withColumn(
                _child_variant_col,
                F.expr(
                    f"try_variant_get(`{variant_col}`, '$.{field_path}', 'VARIANT')"
                ),
            ).filter(F.col(_child_variant_col).isNotNull())

        elif child.get("coerce_to_array"):
            # VARIANT-typed force_array_field: each row may contain either a
            # JSON array OR a single JSON object (XML single-element pattern).
            # Normalise: if array → cast and explode; if object → use as-is.
            #
            # A CASE WHEN expression cannot be used here because Photon evaluates
            # ALL branches eagerly (branch speculation).  For rows where the field
            # is a JSON object, cast(object_variant as array<variant>) throws
            # INVALID_VARIANT_CAST even when the WHEN condition is False.
            #
            # Fix: split into two filtered DataFrames — one for array-shaped rows,
            # one for object-shaped rows — then union.  Physical filter nodes are
            # always applied before downstream projections, so cast() is only ever
            # evaluated on rows that are confirmed arrays.
            _raw_v     = f"_{child_alias}_raw_v"
            _is_arr_ex = (
                f"try_variant_get(`{_raw_v}`, '$[0]', 'VARIANT') IS NOT NULL"
            )
            _base_df = (
                enriched_df
                .withColumn(
                    _raw_v,
                    F.expr(
                        f"try_variant_get(`{variant_col}`, '$.{field_path}', 'VARIANT')"
                    ),
                )
                .filter(F.col(_raw_v).isNotNull())
            )
            # Array-shaped rows: confirmed array → safe to cast, then explode.
            _array_df = (
                _base_df
                .filter(F.expr(_is_arr_ex))
                .withColumn(
                    _child_variant_col,
                    F.explode(F.expr(f"cast(`{_raw_v}` as array<variant>)")),
                )
                .drop(_raw_v)
            )
            # Object-shaped rows: single entity → use raw VARIANT directly.
            _object_df = (
                _base_df
                .filter(~F.expr(_is_arr_ex))
                .withColumn(_child_variant_col, F.col(_raw_v))
                .drop(_raw_v)
            )
            child_df = _array_df.unionByName(_object_df, allowMissingColumns=True)

        else:
            # Regular array field: variant_get extracts the child VARIANT array,
            # cast to array<variant> then standard explode gives one VARIANT
            # row per element.  Avoids variant_explode which requires DBR 15.x+.
            child_df = (
                enriched_df
                .withColumn(
                    _child_variant_col,
                    F.explode(
                        F.expr(
                            f"cast(try_variant_get(`{variant_col}`, "
                            f"'$.{field_path}', 'VARIANT') as array<variant>)"
                        )
                    ),
                )
            )

        # Guard: drop child rows where the child VARIANT is zero-byte / empty.
        # Applies regardless of which branch built child_df (force_single,
        # coerce_to_array, or regular explode).  Photon throws
        # INVALID_VARIANT_CAST when downstream scalar try_variant_get(…, 'STRING')
        # calls are evaluated against a zero-byte VARIANT — the VARIANT-typed
        # probe returns NULL safely without triggering the cast path.
        child_df = child_df.filter(
            F.expr(
                f"try_variant_get(`{_child_variant_col}`, '$', 'VARIANT') IS NOT NULL"
            )
        )

        print(f"[_process_node] child='{child_alias}'  path='$.{field_path}'")

        _process_node(
            node                 = child,
            df                   = child_df,
            variant_col          = _child_variant_col,
            flatten_structs      = flatten_structs,
            exclude_fields       = exclude_fields,
            exclude_root_aliases = exclude_root_aliases,
            ancestor_col_names   = all_inherited,
            spark                = spark,
            source_table         = source_table,
            target_prefix        = target_prefix,
            dedup_keys_map       = dedup_keys_map,
            force_array_fields   = force_array_fields,
            passthrough_fields   = passthrough_fields,
        )




# ---------------------------------------------------------------------------
# 6.  Main pipeline  (same signature as flatten_framework.run_multi_table_pipeline)
# ---------------------------------------------------------------------------

def run_multi_table_pipeline(config: dict, spark: SparkSession):
    """
    VARIANT-native Bronze → Bronze multi-table pipeline.

    Identical config format to flatten_framework.run_multi_table_pipeline.
    Drop-in replacement for job compute environments — same runner, same configs.

    What changes internally
    -----------------------
    * schema_of_variant_agg(payload) replaces Python sampling + infer_column_type.
    * variant_get / variant_explode replace get_json_object / _json_split_array.
    * No UDFs, no string conversion, no from_json.

    L0 mode (l0payload non-null)
    ----------------------------
    l0payload scalar fields are extracted directly via variant_get onto the df
    before tree traversal.  The l1 payload VARIANT is then used for child array
    navigation.  No JSON merge needed — each source column is accessed separately.
    """
    print(f"[framework_variant] version: {_VERSION}")
    bronze_table       = config["bronze_table"]
    target_prefix      = config["target_prefix"]
    elements           = config["elements"]
    dedup_keys_map     = config.get("dedup_keys_map", {})
    bronze_filter      = config.get("bronze_filter")
    write_mode         = config.get("write_mode", "overwrite")
    flatten_structs    = config.get("flatten_structs", {})
    exclude_fields      = set(config.get("exclude_fields", set()))
    exclude_root_aliases = set(config.get("exclude_root_aliases", set()))
    force_array_fields  = set(config.get("force_array_fields", set()))
    passthrough_fields  = config.get("passthrough_fields")   # None = pass everything (default)
    static_columns     = config.get("static_columns", {})
    type_overrides     = config.get("type_overrides", {})

    # ------------------------------------------------------------------
    # Step 1 — Read bronze. No variant_to_str — payload stays as VARIANT.
    # ------------------------------------------------------------------
    bronze_df = spark.read.format("delta").table(bronze_table)
    if bronze_filter:
        bronze_df = bronze_df.filter(bronze_filter)
        print(f"[run_multi_table_pipeline] bronze_filter applied: {bronze_filter}")

    # ------------------------------------------------------------------
    # Steps 2-7 — Process each l1element
    # ------------------------------------------------------------------
    for l1element, root_alias in elements.items():

        print(f"\n[run_multi_table_pipeline] processing element: {l1element}")

        # Step 2 — Filter to this l1element + detect L0 mode
        data_raw = bronze_df.filter(F.col("l1element") == l1element)

        if data_raw.isEmpty():
            print(
                f"[run_multi_table_pipeline] WARNING: no rows for "
                f"l1element='{l1element}' — skipping."
            )
            continue

        _l0_col = "l0payload"
        has_l0  = (
            _l0_col in bronze_df.columns
            and data_raw.filter(F.col(_l0_col).isNotNull()).limit(1).count() > 0
        )
        if has_l0:
            print(f"[run_multi_table_pipeline] L0 mode detected for '{l1element}'")

        # Step 3 — Schema inference via schema_of_variant_agg
        # Returns DDL like: OBJECT<playerId BIGINT, playerName STRING, ...>
        # NOTE: schema_of_variant_agg operates on the VARIANT payload column directly.
        l1_ddl = (
            data_raw
            .agg(F.expr("schema_of_variant_agg(payload)").alias("_ddl"))
            .collect()[0]["_ddl"]
        )
        print(f"[run_multi_table_pipeline] inferred DDL ({l1element}): {l1_ddl[:300]}")

        l1_schema = _parse_variant_ddl(l1_ddl)

        if has_l0:
            l0_ddl = (
                data_raw
                .agg(F.expr(f"schema_of_variant_agg({_l0_col})").alias("_ddl"))
                .collect()[0]["_ddl"]
            )
            l0_schema = _parse_variant_ddl(l0_ddl)
        else:
            l0_schema = StructType([])

        # Step 4 — Build the schema tree from l1_schema only.
        #
        # L0 mode: do NOT merge l0 + l1 schemas here.  l0 scalar fields are
        # extracted separately and passed as ancestor columns — if they were
        # also in root_schema, _process_node would try to extract them from
        # the l1 VARIANT (where they don't exist), creating duplicate column
        # names → COLUMN_ALREADY_EXISTS.
        root_schema = StructType(
            [f for f in l1_schema.fields if f.name not in exclude_fields]
        )

        schema_tree = _build_tree_node(
            alias              = root_alias,
            schema             = root_schema,
            exclude_fields     = exclude_fields,
            target_prefix      = target_prefix,
            dedup_keys_map     = dedup_keys_map,
            force_array_fields = force_array_fields,
            write_mode         = write_mode,
        )

        def _tree_summary(node, indent=0):
            prefix = "  " * indent
            arrays = node.get("arrays", [])
            print(
                f"{prefix}→ {node['alias']}  ({node['target_table']})  "
                f"child arrays: {[c['alias'] for c in arrays]}"
            )
            for child in arrays:
                _tree_summary(child, indent + 1)

        print(f"[run_multi_table_pipeline] schema tree ({l1element}):")
        _tree_summary(schema_tree)

        # Step 5 — Prepare the root VARIANT column for _process_node
        _root_variant_col = f"_{root_alias}_v"

        # Determine payload shape once — shared by both L0 and standard branches.
        # ARRAY<…>  → payload is an array of root entities (one or many per row).
        # OBJECT<…> → payload IS the root entity directly (no array wrapping).
        _l1_array_wrapped = l1_ddl.strip().upper().startswith("ARRAY<")

        if has_l0:
            # L0 mode: l0payload carries the root entity scalars; payload carries
            # child arrays.  payload may be [{"key": val}] (single-element array
            # wrapping) or a bare OBJECT — handled by the _l1_array_wrapped flag.
            if _l1_array_wrapped:
                root_df = data_raw.withColumn(
                    _root_variant_col,
                    F.expr("cast(payload as array<variant>)")[0],
                )
            else:
                root_df = data_raw.withColumn(_root_variant_col, F.col("payload"))

            # Stamp l0 scalar fields onto the df upfront so they flow as
            # ancestor columns into every child table.
            l0_scalar_names = []
            for field in l0_schema.fields:
                if field.name in exclude_fields:
                    continue
                if isinstance(field.dataType, (StructType, T.ArrayType)):
                    continue
                col_name = to_snake_case(field.name)
                type_str = _sql_type_str(field.dataType)
                root_df  = root_df.withColumn(
                    col_name,
                    F.expr(
                        f"try_variant_get(`{_l0_col}`, '$.{field.name}', '{type_str}')"
                    ),
                )
                l0_scalar_names.append(col_name)

        else:
            # Standard mode: no l0payload — payload IS the data.
            #
            # ARRAY<…>  → one bronze row holds multiple root entities; cast to
            #             array<variant> and explode so each element becomes its
            #             own row.  This is the typical case (ladder, squads, etc.)
            #
            # OBJECT<…> → one bronze row holds exactly one root entity (e.g.
            #             gameStats with no l0 wrapping).  Assign payload directly
            #             — exploding a JSON object raises INVALID_VARIANT_CAST.
            if _l1_array_wrapped:
                root_df = (
                    data_raw
                    .withColumn(
                        _root_variant_col,
                        F.explode(F.expr("cast(payload as array<variant>)")),
                    )
                )
            else:
                root_df = data_raw.withColumn(_root_variant_col, F.col("payload"))
            l0_scalar_names = []

        # Step 5.5 — Drop null / zero-byte VARIANT rows before any scalar
        # extraction is triggered.
        #
        # Photon throws INVALID_VARIANT_CAST on try_variant_get(…, 'STRING')
        # for zero-byte / malformed VARIANT values even through the TRY wrapper
        # (same issue documented at the infer_df guard ~100 lines above).
        # A 'VARIANT'-typed probe is cast-free (no type coercion) and safely
        # returns NULL for empty/malformed rows without triggering the bug.
        #
        # In L0 mode, both _root_variant_col (payload) AND l0payload carry
        # VARIANT columns whose scalar fields are extracted later — guard both.
        _guard_expr = (
            f"try_variant_get(`{_root_variant_col}`, '$', 'VARIANT') IS NOT NULL"
        )
        _guard_cols = [_root_variant_col]
        if has_l0:
            _guard_expr += (
                f" AND try_variant_get(`{_l0_col}`, '$', 'VARIANT') IS NOT NULL"
            )
            _guard_cols.append(_l0_col)
        root_df = root_df.filter(F.expr(_guard_expr))
        print(
            f"[run_multi_table_pipeline] root VARIANT guard applied "
            f"({' + '.join(_guard_cols)} must be non-null/non-empty)"
        )

        # Step 6 — Stamp static columns
        for col_name, col_val in static_columns.items():
            root_df = root_df.withColumn(col_name, F.lit(col_val))

        meta_col_names    = ["ingested_at", "ingested_from"]
        static_col_names  = list(static_columns.keys())
        initial_ancestors = meta_col_names + static_col_names + l0_scalar_names

        # If passthrough_fields is set, restrict initial ancestors so that
        # descriptive l0payload columns (e.g. competition_name, game_hash_tag)
        # never enter the framework-generated tables at all.
        if passthrough_fields is not None:
            initial_ancestors = [c for c in initial_ancestors if c in passthrough_fields]

        # Step 7 — Recursive tree traversal
        _process_node(
            node                 = schema_tree,
            df                   = root_df,
            variant_col          = _root_variant_col,
            flatten_structs      = flatten_structs,
            exclude_fields       = exclude_fields,
            exclude_root_aliases = exclude_root_aliases,
            ancestor_col_names   = initial_ancestors,
            spark                = spark,
            source_table         = bronze_table,
            target_prefix        = target_prefix,
            dedup_keys_map       = dedup_keys_map,
            force_array_fields   = force_array_fields,
            passthrough_fields   = passthrough_fields,
        )
