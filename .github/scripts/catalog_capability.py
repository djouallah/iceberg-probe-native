"""What the OneLake Iceberg REST catalog supports today, asked through pyiceberg.

THE READING THAT COMES BEFORE ANY WRITE BENCHMARK. The endpoint is a private preview under
active development, with no published documentation, so what it supports is established by
sending the request and reading the answer. This script sends them, one per probe, and prints
what came back. A `no` is as useful as a `yes` and is quoted in the server's own words.

EVERYTHING HERE IS A READING TAKEN ON A DATE, not a property of the product. The surface is
expected to move, so the workflow is the source of truth and the page it feeds (catalog.md)
carries the run id it came from.

IT IS NOT A BENCHMARK. Nothing is timed, nothing lands in results/, and a probe that comes back
`no` does not fail the job. The only non-zero exit is a credential failure, because then nothing
was read at all.

pyiceberg AND PLAIN HTTP, NO ENGINES. Every probe here is one or the other, so this runs in about
two minutes. An answer from pyiceberg alone is a fact about pyiceberg until DuckDB or Sail
confirms it.

WHAT IT READ, run 35582405124 (2026-09-21, pyiceberg 0.12.0). 19 supported, 10 no.

THE ENDPOINT DECLARES ITS OWN SURFACE, which is the most useful thing on this page. /v1/config
comes back with an `endpoints` list -- the REST spec's way for a server to say what it implements
-- and this one fills it in: thirteen entries covering namespaces (list, create, load, head,
drop), tables (list, load, head, update, drop, create), `POST /v1/{prefix}/tables/rename`, and
per-table `credentials`. Nothing the probes got a `no` from is on that list, and nothing on the
list came back `no`, so the list is accurate and it is where to look first: updateNamespace-
Properties (405), registerTable (pyiceberg reads the list and declines before sending),
multi-table transactions (405), and no metrics endpoint.

ONE SNAPSHOT PER COMMIT. An UpdateTableRequest carrying two `add-snapshot` updates comes back:

    400 BadRequest: Only one instance of each update type is allowed per request.
    Duplicate types: add-snapshot, set-snapshot-ref

That is the shape pyiceberg builds for `overwrite` (whole-table and by filter) and for `upsert`,
so those three are not available through pyiceberg here yet. `append` works, and so does
`delete`, including a delete matching only part of a data file, which rewrites the remainder --
so a row-level replace IS expressible through pyiceberg, as a delete then an append, in two
commits rather than one.

This is a statement about commit SHAPE, not about overwrite.

assert-ref-snapshot-id IS ENFORCED. With `commit.retry.num-retries` set to 0 so the client could
not refresh and re-send, a commit against a head another writer had already moved came back as
"CommitFailedException: One or more requirements failed. The client may retry." That is the
guarantee every lost-update defence in every client is built on, and it holds.

Also accepted: create with no location (the catalog assigns one), partitioned tables written
through, sort orders, schema evolution, table properties, tags, branches, rename, and DROP with
purge. Declined at create: a schema whose first field id is 0, which is Spark's SparkSchemaUtil
numbering and why the Spark probe renumbers with assignIncreasingFreshIds.

stage-create is a no-op: the request is accepted and the table is created immediately rather than
staged, so loadTable finds it straight after. That matches what bench/engines/duckdb_iceberg.py
assumes when it sets STAGE_CREATE_TABLES false. An earlier version of this probe read a 200
on the request as support for the flag, which is why create_staged now checks the effect.

It leaves the `_bench_capability` namespace behind, for the reason auth_smoke.py leaves
`_bench_probe`: deleting needs more permission than creating. Its tables are dropped at the end
unless CAPABILITY_KEEP=1.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

from bench import auth, scrub
from bench.config import ICEBERG_ENDPOINT, ONELAKE_BLOB, Config

NAMESPACE = "_bench_capability"

# Probe outcomes. Three of them are answers about the endpoint:
#
#   supported  the request was taken and had the effect it describes
#   no         the endpoint declined it, with a status code and a message
#   no-op      the request was accepted and the effect was not applied
#
# A no-op is tracked apart from a `no` because the response does not distinguish the two: both
# come back without an error in the client's hands, so only a probe that CHECKS THE EFFECT can
# tell them apart. That is why create_staged asks whether the table is there. `broken` is the
# only outcome that says nothing about the endpoint, because the probe could not ask its question.
#
# This is a private preview under active development, so every one of these is a reading taken
# on a date, not a property of the product.
SUPPORTED, REFUSED, NOOP, SKIPPED, BROKEN = (
    "supported",
    "no",
    "no-op",
    "skipped",
    "broken",
)

MARK = {SUPPORTED: "yes", REFUSED: "no", NOOP: "no-op", SKIPPED: "—", BROKEN: "?"}


class Skip(Exception):
    """A probe with nothing to ask this run, because a prerequisite did not land."""


class Refused(Exception):
    """The endpoint answered, and the answer was no. A finding, not a failure."""


class NoOp(Exception):
    """The endpoint took the request, returned success, and did not do it."""


# --------------------------------------------------------------------------------------------
# plain HTTP, for the questions pyiceberg has no API for
# --------------------------------------------------------------------------------------------


def request(
    method: str, url: str, token: str, body: dict | None = None, headers: dict | None = None
) -> dict:
    """One REST call. A 4xx or 5xx comes back as `Refused` carrying the server's own message."""
    data = json.dumps(body).encode() if body is not None else None
    headers = {"Authorization": f"Bearer {token}", **(headers or {})}
    if data is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            text = response.read().decode("utf-8", "replace")
            return json.loads(text) if text.strip() else {}
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", "replace")[:600]
        raise Refused(f"HTTP {exc.code}: {' '.join(detail.split())}") from None


class Endpoint:
    """The catalog URL, and the prefix /v1/config says every later path is built from."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.token = auth.onelake_token()
        self.config = request(
            "GET", f"{ICEBERG_ENDPOINT}/v1/config?warehouse={cfg.warehouse}", self.token
        )
        self.prefix = self.config.get("overrides", {}).get("prefix", cfg.warehouse)

    def call(
        self, method: str, path: str, body: dict | None = None, headers: dict | None = None
    ) -> dict:
        return request(
            method, f"{ICEBERG_ENDPOINT}/v1/{self.prefix}/{path}", self.token, body, headers
        )


# --------------------------------------------------------------------------------------------
# the probe harness
# --------------------------------------------------------------------------------------------


def _is_server_refusal(message: str) -> bool:
    """Did the endpoint answer no, as opposed to the client or the network falling over?"""
    lowered = message.lower()
    marks = (
        "400",
        "403",
        "404",
        "405",
        "409",
        "415",
        "422",
        "501",
        "badrequest",
        "bad request",
        "not implemented",
        "unsupported",
        "malformed",
        "already exists",
        "notfound",
        "not found",
        "forbidden",
        "commitfailed",
        "does not support endpoint",
    )
    return any(mark in lowered for mark in marks)


class Report:
    def __init__(self, reason=None) -> None:
        self.rows: list[dict] = []
        # How an unexpected exception is turned into one line. The default reads the exception
        # itself, which is right for pyiceberg, where the endpoint's message is in the text. The
        # Spark probe passes its own, because py4j keeps the JVM message on the exception OBJECT
        # and `str()` gives only "An error occurred while calling o93.sql."
        self._reason = reason or (lambda exc: scrub.scrub_exc(exc, 600))

    def record(self, group: str, question: str, outcome: str, detail: object) -> None:
        self.rows.append(
            {
                "group": group,
                "question": question,
                "outcome": outcome,
                "detail": scrub.scrub(detail),
            }
        )

    def run(self, group: str, question: str, fn) -> bool:
        print(f"\n[{len(self.rows) + 1:>2}] {group}: {question}", flush=True)
        try:
            detail = fn() or ""
        except Skip as skip:
            self.record(group, question, SKIPPED, skip)
            print(f"     skip       {scrub.scrub(skip)}", flush=True)
            return False
        except Refused as refused:
            self.record(group, question, REFUSED, refused)
            print(f"     no         {scrub.scrub(refused)}", flush=True)
            return False
        except NoOp as noop:
            self.record(group, question, NOOP, noop)
            print(f"     no-op      {scrub.scrub(noop)}", flush=True)
            return False
        except Exception as exc:  # noqa: BLE001 - reporting the failure is the job
            message = self._reason(exc)
            # A client-side exception is usually still the endpoint talking back through the
            # client, so the status code in the message decides which it was.
            outcome = REFUSED if _is_server_refusal(message) else BROKEN
            self.record(group, question, outcome, message)
            print(f"     {outcome:<10} {message}", flush=True)
            return False
        self.record(group, question, SUPPORTED, detail)
        print(f"     supported  {scrub.scrub(detail)}", flush=True)
        return True

    def markdown(self) -> list[str]:
        lines = ["| Area | Question | Supported | What came back |", "|---|---|---|---|"]
        for row in self.rows:
            detail = row["detail"].replace("|", "\\|").replace("\n", " ")[:240]
            lines.append(
                f"| {row['group']} | {row['question']} | {MARK[row['outcome']]} | "
                f"{'`' + detail + '`' if detail else ''} |"
            )
        return lines

    def tally(self) -> dict[str, int]:
        counts = dict.fromkeys(MARK, 0)
        for row in self.rows:
            counts[row["outcome"]] += 1
        return counts


# --------------------------------------------------------------------------------------------
# what the probes write
# --------------------------------------------------------------------------------------------


def iceberg_schema():
    """Two optional columns with field ids from 1 -- pyiceberg's numbering, which this endpoint
    accepts. Declared explicitly rather than inferred from Arrow, so the partition and sort
    probes have real source ids to point at."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import LongType, NestedField

    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "v", LongType(), required=False),
    )


def rows(pairs):
    import pyarrow as pa

    return pa.table(
        {
            "id": pa.array([i for i, _ in pairs], pa.int64()),
            "v": pa.array([v for _, v in pairs], pa.int64()),
        }
    )


def raw_schema(first_field_id: int) -> dict:
    return {
        "type": "struct",
        "schema-id": 0,
        "fields": [
            {"id": first_field_id, "name": "id", "required": False, "type": "long"},
            {"id": first_field_id + 1, "name": "v", "required": False, "type": "long"},
        ],
    }


# --------------------------------------------------------------------------------------------
# partition transforms, column types, type promotion: the same cases for every engine
# --------------------------------------------------------------------------------------------

TEMPORAL = ("year", "month", "day", "hour")
# Rows every engine writes for each transform: (id, value). The two timestamps differ in year,
# month, day and hour, so every temporal transform splits them; truncate(2) splits ap / ba.
TRANSFORM_ROWS = {
    "bucket": [(i, i * 10) for i in range(1, 9)],
    "truncate": [(1, "apple"), (2, "apricot"), (3, "banana")],
    "temporal": [(1, "2025-01-01 10:00:00"), (2, "2026-02-02 11:00:00")],
}

SCALAR_TYPES = ("decimal", "date", "timestamp", "timestamptz", "uuid", "binary")
NESTED_TYPES = ("struct", "list", "map")
# What column x reads back as, normalised by `normalize`, after each engine writes its literal.
TYPE_EXPECTED = {
    "decimal": "12.34",
    "date": "2026-01-02",
    "timestamp": "2026-01-02T03:04:05.123456",
    "timestamptz": "2026-01-02T03:04:05.123456+00:00",
    "uuid": "6f1c2d3e-4b5a-4c6d-8e7f-0123456789ab",
    "binary": "0102",
    "struct": '{"a": 1, "b": "x"}',
    "list": "[1, 2, 3]",
    "map": '{"k": 1}',
}


def _family(kind: str) -> str:
    return "temporal" if kind in TEMPORAL else kind


def transform_case(kind: str):
    """(schema, spec, expected partition count) for one transform: bucket, truncate, or one of
    TEMPORAL. Column 2 holds the value; bucket partitions column 1, the others column 2."""
    from pyiceberg.partitioning import PartitionField, PartitionSpec
    from pyiceberg.schema import Schema
    from pyiceberg.transforms import (
        BucketTransform,
        DayTransform,
        HourTransform,
        MonthTransform,
        TruncateTransform,
        YearTransform,
    )
    from pyiceberg.types import LongType, NestedField, StringType, TimestampType

    value_type = {"bucket": LongType(), "truncate": StringType(), "temporal": TimestampType()}
    schema = Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "x", value_type[_family(kind)], required=False),
    )
    transform = {
        "bucket": BucketTransform(4),
        "truncate": TruncateTransform(2),
        "year": YearTransform(),
        "month": MonthTransform(),
        "day": DayTransform(),
        "hour": HourTransform(),
    }[kind]
    source = 1 if kind == "bucket" else 2
    spec = PartitionSpec(PartitionField(source, 1000, transform, f"{kind}_p"))
    rows_ = TRANSFORM_ROWS[_family(kind)]
    if kind == "bucket":
        bucket = BucketTransform(4).transform(LongType())
        parts = len({bucket(i) for i, _ in rows_})
    else:
        parts = 2
    return schema, spec, parts


def transform_arrow(kind: str, schema):
    """TRANSFORM_ROWS for `kind` as an Arrow table matching `schema`, for pyiceberg's append."""
    import datetime as dt

    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    rows_ = TRANSFORM_ROWS[_family(kind)]
    if kind in TEMPORAL:
        rows_ = [(i, dt.datetime.fromisoformat(v)) for i, v in rows_]
    return pa.Table.from_pylist(
        [{"id": i, "x": v} for i, v in rows_], schema=schema_to_pyarrow(schema)
    )


def type_schema(name: str):
    """(id long, x <the type>) for one TYPE_EXPECTED case."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import (
        BinaryType,
        DateType,
        DecimalType,
        ListType,
        LongType,
        MapType,
        NestedField,
        StringType,
        StructType,
        TimestampType,
        TimestamptzType,
        UUIDType,
    )

    x = {
        "decimal": DecimalType(10, 2),
        "date": DateType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
        "uuid": UUIDType(),
        "binary": BinaryType(),
        "struct": StructType(
            NestedField(3, "a", LongType(), required=False),
            NestedField(4, "b", StringType(), required=False),
        ),
        "list": ListType(element_id=3, element_type=LongType(), element_required=False),
        "map": MapType(
            key_id=3, key_type=StringType(), value_id=4, value_type=LongType(), value_required=False
        ),
    }[name]
    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "x", x, required=False),
    )


def type_arrow(name: str, schema):
    """The one row (1, <TYPE_EXPECTED value>) as an Arrow table matching `schema`."""
    import datetime as dt
    import decimal
    import uuid

    import pyarrow as pa
    from pyiceberg.io.pyarrow import schema_to_pyarrow

    ts = dt.datetime(2026, 1, 2, 3, 4, 5, 123456)
    value = {
        "decimal": decimal.Decimal("12.34"),
        "date": dt.date(2026, 1, 2),
        "timestamp": ts,
        "timestamptz": ts.replace(tzinfo=dt.UTC),
        "uuid": uuid.UUID(TYPE_EXPECTED["uuid"]).bytes,
        "binary": b"\x01\x02",
        "struct": {"a": 1, "b": "x"},
        "list": [1, 2, 3],
        "map": [("k", 1)],
    }[name]
    return pa.Table.from_pylist([{"id": 1, "x": value}], schema=schema_to_pyarrow(schema))


def normalize(name: str, value) -> str:
    """One read-back value as the string TYPE_EXPECTED holds for it."""
    import datetime as dt
    import uuid

    if value is None:
        return "null"
    if name == "uuid":
        if isinstance(value, bytes | bytearray):
            value = uuid.UUID(bytes=bytes(value))
        return str(value)
    if name == "binary":
        return bytes(value).hex()
    if name == "timestamptz" and isinstance(value, dt.datetime):
        return value.astimezone(dt.UTC).isoformat()
    if isinstance(value, dt.date | dt.datetime):
        return value.isoformat()
    if name == "map" and isinstance(value, list):
        value = dict(value)
    if name in NESTED_TYPES:
        return json.dumps(value, sort_keys=True, default=str)
    return str(value)


def type_result(tbl, name: str) -> tuple[str, str, str]:
    """Column x of the table's one row, read through pyiceberg, against TYPE_EXPECTED."""
    try:
        found = tbl.scan(selected_fields=("x",)).to_arrow().to_pylist()
    except Exception as exc:  # noqa: BLE001 - the read-back failing is the answer
        return name, NOOP, f"written; pyiceberg cannot read it: {scrub.scrub_exc(exc, 200)}"
    got = normalize(name, found[0]["x"]) if len(found) == 1 else f"{len(found)} rows"
    if got != TYPE_EXPECTED[name]:
        return name, NOOP, f"expected {TYPE_EXPECTED[name]}, reads {got}"
    return name, SUPPORTED, got


def partition_result(tbl, kind: str, parts: int) -> tuple[str, str, str]:
    """The spec carries the transform and the data files split the rows into `parts`."""
    spec = [str(f.transform) for f in tbl.spec().fields]
    if not any(s.startswith(kind) for s in spec):
        return kind, NOOP, f"the spec is {spec}"
    files = list(tbl.scan().plan_files())
    found = {str(task.file.partition) for task in files}
    count = sum(task.file.record_count for task in files)
    expected = len(TRANSFORM_ROWS[_family(kind)])
    if len(found) != parts or count != expected:
        return (
            kind,
            NOOP,
            f"{len(found)} partition(s) holding {count} row(s), expected {parts} and {expected}",
        )
    return kind, SUPPORTED, f"{spec[0]} over {parts} partitions"


def verdict(results: list[tuple[str, str, str]]) -> str:
    """One probe asking several things: yes only if every one of them is."""
    ok = [label for label, outcome, _ in results if outcome == SUPPORTED]
    refused = [f"{label}: {d}" for label, outcome, d in results if outcome == REFUSED]
    ignored = [f"{label}: {d}" for label, outcome, d in results if outcome == NOOP]
    if refused:
        raise Refused(f"works: {ok or 'none'}; refused: " + "; ".join(refused + ignored))
    if ignored:
        raise NoOp(f"works: {ok or 'none'}; not applied: " + "; ".join(ignored))
    return "all of " + ", ".join(ok)


def promotion_schema():
    """(id long, c int), for the int -> long promotion."""
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, LongType, NestedField

    return Schema(
        NestedField(1, "id", LongType(), required=False),
        NestedField(2, "c", IntegerType(), required=False),
    )


def promotion_check(tbl) -> str:
    """After the promotion: c is a long, and the row written as an int still reads 7."""
    kind = str(tbl.schema().find_field("c").field_type)
    if kind != "long":
        raise NoOp(f"returned success and c is still {kind}")
    found = tbl.scan(selected_fields=("c",)).to_arrow().to_pylist()
    if [r["c"] for r in found] != [7]:
        raise NoOp(f"c is long, and the old row reads {found}")
    return "c is long, and the row written as int reads 7"


# --------------------------------------------------------------------------------------------
# the probes
# --------------------------------------------------------------------------------------------


class Capability:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.endpoint: Endpoint | None = None
        self.catalog = None
        self.report = Report()
        # Unique per run AND attempt, so a re-run never collides with a table the first attempt
        # created and could not drop.
        self.run = "{}_{}".format(
            os.environ.get("GITHUB_RUN_ID", "local"),
            os.environ.get("GITHUB_RUN_ATTEMPT", "1"),
        )
        self.created: list[str] = []
        self.keep = os.environ.get("CAPABILITY_KEEP") == "1"
        self.baseline: str | None = None

    # -- helpers -----------------------------------------------------------------------------

    def name(self, what: str) -> str:
        return f"cap_{self.run}_{what}"

    def identifier(self, table: str) -> str:
        return f"{NAMESPACE}.{table}"

    def location(self, table: str) -> str:
        return f"{self.cfg.base_path}/Tables/{NAMESPACE}/{table}"

    def create(self, what: str, **kwargs):
        """pyiceberg createTable, remembered so the drop at the end can find it."""
        table = self.name(what)
        made = self.catalog.create_table(
            self.identifier(table),
            schema=kwargs.pop("schema", iceberg_schema()),
            location=self.location(table),
            **kwargs,
        )
        self.created.append(table)
        return made

    def create_raw(self, what: str, body: dict) -> dict:
        """createTable as a hand-built REST request, for the shapes pyiceberg will not send."""
        table = self.name(what)
        payload = {
            "name": table,
            "location": self.location(table),
            "schema": raw_schema(1),
            "partition-spec": {"spec-id": 0, "fields": []},
            "write-order": {"order-id": 0, "fields": []},
            "stage-create": False,
            "properties": {},
            **body,
        }
        answer = self.endpoint.call("POST", f"namespaces/{NAMESPACE}/tables", payload)
        self.created.append(table)
        return answer

    def load(self, table: str):
        return self.catalog.load_table(self.identifier(table))

    def need_baseline(self):
        if self.baseline is None:
            raise Skip("no table was created, so there is nothing to write to")
        return self.load(self.baseline)

    def fresh(self, what: str, seed=((1, 10), (2, 20), (3, 30)), **kwargs):
        """A table with rows already in it, for a probe that needs something to modify."""
        if self.catalog is None:
            raise Skip("the catalog never loaded")
        table = self.create(what, **kwargs)
        table.append(rows(seed))
        return table

    # -- the endpoint itself -------------------------------------------------------------------

    def config(self) -> str:
        self.endpoint = Endpoint(self.cfg)
        config = self.endpoint.config
        # `endpoints` is the spec's own way for a server to declare what it implements. If this
        # one fills it in, everything below is confirmation rather than discovery.
        declared = config.get("endpoints")
        detail = f"prefix={self.endpoint.prefix!r}"
        if config.get("overrides"):
            detail += f", overrides={sorted(config['overrides'])}"
        if config.get("defaults"):
            detail += f", defaults={sorted(config['defaults'])}"
        detail += (
            f", declares {len(declared)} endpoints: {declared}"
            if declared
            else ", declares NO endpoints list, so support has to be probed rather than read"
        )
        return detail

    def catalog_loads(self) -> str:
        self.catalog = auth.catalog(self.cfg)
        return f"pyiceberg RestCatalog lists {len(self.catalog.list_namespaces())} namespaces"

    # -- namespaces ----------------------------------------------------------------------------

    def create_namespace(self) -> str:
        self.catalog.create_namespace_if_not_exists(NAMESPACE)
        return f"{NAMESPACE} exists"

    def load_namespace(self) -> str:
        payload = self.endpoint.call("GET", f"namespaces/{NAMESPACE}")
        return f"properties={payload.get('properties', {})}"

    def namespace_properties(self) -> str:
        payload = self.endpoint.call(
            "POST",
            f"namespaces/{NAMESPACE}/properties",
            {"removals": [], "updates": {"probed-at": self.run}},
        )
        return f"updated={payload.get('updated')}, missing={payload.get('missing')}"

    # -- creating tables -----------------------------------------------------------------------

    def create_table(self) -> str:
        table = self.create("base")
        self.baseline = self.created[-1]
        version = table.metadata.format_version
        return f"field ids from 1, explicit location, format-version {version}"

    def create_field_id_zero(self) -> str:
        """Spark numbers top-level fields from 0; pyiceberg from 1. This is the known refusal."""
        self.create_raw("id0", {"schema": raw_schema(0)})
        return "a schema whose first field id is 0 is accepted"

    def create_without_location(self) -> str:
        table = self.name("noloc")
        self.endpoint.call(
            "POST",
            f"namespaces/{NAMESPACE}/tables",
            {
                "name": table,
                "schema": raw_schema(1),
                "partition-spec": {"spec-id": 0, "fields": []},
                "write-order": {"order-id": 0, "fields": []},
                "stage-create": False,
                "properties": {},
            },
        )
        self.created.append(table)
        return "the catalog assigns a location when the request omits one"

    def create_staged(self) -> str:
        """IS stage-create HONOURED, or just accepted? Those are different answers.

        The spec says a staged create returns the table metadata but does NOT commit the table
        to the catalog: it appears only when the first write commits. So a 200 proves nothing on
        its own, and the real question is whether the table is there afterwards. If loadTable
        finds it, the flag was parsed and ignored -- the table was created for real, which is
        the opposite of what a client asking to stage one expects.
        """
        table = self.name("staged")
        self.endpoint.call(
            "POST",
            f"namespaces/{NAMESPACE}/tables",
            {
                "name": table,
                "location": self.location(table),
                "schema": raw_schema(1),
                "partition-spec": {"spec-id": 0, "fields": []},
                "write-order": {"order-id": 0, "fields": []},
                "stage-create": True,
                "properties": {},
            },
        )
        if self.catalog.table_exists(self.identifier(table)):
            self.created.append(table)  # it is real, so the teardown has to drop it
            raise NoOp(
                "accepted; the table is created immediately rather than staged, so loadTable "
                "finds it straight after the create"
            )
        return "honoured: the table is not in the catalog until the first commit"

    def create_partitioned(self) -> str:
        from pyiceberg.partitioning import PartitionField, PartitionSpec
        from pyiceberg.transforms import IdentityTransform

        spec = PartitionSpec(
            PartitionField(source_id=1, field_id=1000, transform=IdentityTransform(), name="id")
        )
        table = self.create("part", partition_spec=spec)
        table.append(rows([(1, 10), (2, 20)]))
        return "an identity-partitioned table is created and written"

    def _transform(self, kind: str) -> tuple[str, str, str]:
        schema, spec, parts = transform_case(kind)
        try:
            table = self.create(f"tr_{kind}", schema=schema, partition_spec=spec)
            table.append(transform_arrow(kind, schema))
        except Exception as exc:  # noqa: BLE001 - each transform answers for itself
            return kind, REFUSED, scrub.scrub_exc(exc, 300)
        return partition_result(table.refresh(), kind, parts)

    def partition_bucket(self) -> str:
        return verdict([self._transform("bucket")])

    def partition_truncate(self) -> str:
        return verdict([self._transform("truncate")])

    def partition_temporal(self) -> str:
        return verdict([self._transform(kind) for kind in TEMPORAL])

    def _typed(self, name: str) -> tuple[str, str, str]:
        schema = type_schema(name)
        try:
            table = self.create(f"type_{name}", schema=schema)
            table.append(type_arrow(name, schema))
        except Exception as exc:  # noqa: BLE001 - each type answers for itself
            return name, REFUSED, scrub.scrub_exc(exc, 300)
        return type_result(table.refresh(), name)

    def types_scalar(self) -> str:
        return verdict([self._typed(name) for name in SCALAR_TYPES])

    def types_nested(self) -> str:
        return verdict([self._typed(name) for name in NESTED_TYPES])

    def type_promotion(self) -> str:
        import pyarrow as pa
        from pyiceberg.io.pyarrow import schema_to_pyarrow
        from pyiceberg.types import LongType

        schema = promotion_schema()
        table = self.create("promote", schema=schema)
        table.append(pa.Table.from_pylist([{"id": 1, "c": 7}], schema=schema_to_pyarrow(schema)))
        with table.update_schema() as update:
            update.update_column("c", LongType())
        return promotion_check(table.refresh())

    def create_sorted(self) -> str:
        from pyiceberg.table.sorting import SortField, SortOrder
        from pyiceberg.transforms import IdentityTransform

        order = SortOrder(SortField(source_id=1, transform=IdentityTransform()))
        self.create("sorted", sort_order=order)
        # Read the metadata as the catalog stores it: Spark fails to load a table
        # created with a sort order ("sortOrder is null"), which is what a default-sort-order-id
        # naming no stored sort order looks like.
        meta = self.endpoint.call("GET", f"namespaces/{NAMESPACE}/tables/{self.created[-1]}")
        meta = meta.get("metadata", {})
        default = meta.get("default-sort-order-id")
        stored = {o.get("order-id"): len(o.get("fields", [])) for o in meta.get("sort-orders", [])}
        if default not in stored:
            raise NoOp(
                f"accepted, and the table names default-sort-order-id {default} while the "
                f"sort orders it stores are {stored} (order-id: field count)"
            )
        if not stored[default]:
            raise NoOp(f"accepted, and the table's default sort order is unsorted: {stored}")
        return f"default-sort-order-id {default}; stored sort orders {stored}"

    # -- writing -------------------------------------------------------------------------------

    def append(self) -> str:
        table = self.need_baseline()
        table.append(rows([(1, 10), (2, 20), (3, 30)]))
        table.refresh()
        summary = table.current_snapshot().summary
        return f"one snapshot, operation={summary.operation}"

    def two_snapshots_one_commit(self) -> str:
        """The shape pyiceberg's overwrite, upsert and any rewriting delete all send: one
        UpdateTableRequest carrying two add-snapshot updates."""
        table = self.fresh("multi")
        with table.transaction() as tx:
            tx.append(rows([(4, 40)]))
            tx.append(rows([(5, 50)]))
        return "a commit carrying two snapshots is accepted"

    def overwrite_whole(self) -> str:
        table = self.fresh("ovw")
        table.overwrite(rows([(9, 90)]))
        return "pyiceberg overwrite, which is a delete and an append in one commit"

    def overwrite_filtered(self) -> str:
        table = self.fresh("ovwf")
        table.overwrite(rows([(1, 111)]), overwrite_filter="id == 1")
        return "a filtered overwrite is accepted"

    def delete_whole_file(self) -> str:
        """Every row of the only data file matches, so nothing is rewritten: one snapshot."""
        table = self.fresh("delfile", seed=((1, 10),))
        table.delete("id == 1")
        return "a metadata-only delete, taking whole data files, is accepted"

    def delete_partial_file(self) -> str:
        """One row of three matches, so pyiceberg rewrites the remainder and the commit carries
        an append beside the delete."""
        table = self.fresh("delpart")
        table.delete("id == 1")
        return "a delete that rewrites the rest of its file is accepted"

    def upsert(self) -> str:
        table = self.fresh("upsert")
        result = table.upsert(rows([(1, 111), (4, 40)]), join_cols=["id"])
        return f"updated={result.rows_updated}, inserted={result.rows_inserted}"

    # -- the commit protocol ---------------------------------------------------------------------

    def stale_assertion(self) -> str:
        """DOES THE CATALOG ENFORCE assert-ref-snapshot-id? The whole concurrency question, asked
        with no concurrency at all.

        Two handles on one table. B commits, so the branch head moves. A then commits against the
        head IT loaded, which is now stale. Retries are turned off on this table so the client
        cannot refresh and re-send: what comes back is the endpoint's own answer.

        A DECLINE IS THE RESULT WE WANT and is recorded as supported -- the catalog is enforcing
        the assertion, which is what every lost-update defence in every client is built on.
        Acceptance would mean a commit built on a base the table has moved past is taken, which
        the client has no way to detect.
        """
        self.fresh("stale", properties={"commit.retry.num-retries": "0"})
        name = self.created[-1]
        a, b = self.load(name), self.load(name)
        b.append(rows([(8, 80)]))
        try:
            a.append(rows([(9, 90)]))
        except Exception as exc:  # noqa: BLE001 - a decline here is the answer we want
            return f"enforced, declined with: {scrub.scrub_exc(exc, 220)}"
        a.refresh()
        ids = sorted(a.scan().to_arrow().column("id").to_pylist())
        raise Refused(
            "not enforced: a commit against a stale head was accepted with retries off, and the "
            f"table now holds ids {ids}"
        )

    def add_column(self) -> str:
        from pyiceberg.types import StringType

        table = self.fresh("schema")
        with table.update_schema() as update:
            update.add_column("note", StringType())
        return f"column added, schema-id is now {table.schema().schema_id}"

    def drop_column(self) -> str:
        table = self.fresh("dropcol")
        with table.update_schema() as update:
            update.delete_column("v")
        names = [field.name for field in self.load(self.created[-1]).schema().fields]
        if "v" in names:
            raise NoOp(f"returned success and the schema is still {names}")
        return f"schema now {names}"

    def rename_column(self) -> str:
        table = self.fresh("renamecol")
        with table.update_schema() as update:
            update.rename_column("v", "v2")
        table = self.load(self.created[-1])
        names = [field.name for field in table.schema().fields]
        if "v2" not in names:
            raise NoOp(f"returned success and the schema is still {names}")
        data = sorted(table.scan().to_arrow().column("v2").to_pylist())
        return f"schema now {names}; data kept: {data}"

    def partition_evolution(self) -> str:
        from pyiceberg.transforms import BucketTransform

        table = self.fresh("specevo")
        with table.update_spec() as update:
            update.add_field("id", BucketTransform(4), "id_bucket")
        spec = self.load(self.created[-1]).spec()
        if not spec.fields:
            raise NoOp("returned success and the table is still unpartitioned")
        return f"spec now {spec}"

    def sort_order_evolution(self) -> str:
        from pyiceberg.transforms import IdentityTransform

        table = self.fresh("sortevo")
        with table.update_sort_order() as update:
            update.asc("id", IdentityTransform())
        order = self.load(self.created[-1]).sort_order()
        if not order.fields:
            raise NoOp("update_sort_order returned success and the table has no sort order")
        return f"sort order {order}"

    def set_property(self) -> str:
        table = self.need_baseline()
        with table.transaction() as tx:
            tx.set_properties(**{"probed-at": self.run})
        return "table properties can be set"

    def vended_only(self) -> str:
        """pyiceberg on vended credentials alone: a second RestCatalog that asks for them and has
        no adls.token. Whether pyiceberg resolved a SAS decides the answer -- read off the table's
        FileIO properties -- so an ambient Azure login cannot pass the probe instead."""
        from pyiceberg.catalog import load_catalog

        self.fresh("vended")
        name = self.created[-1]
        catalog = load_catalog(
            "onelake_vended",
            **{
                "uri": ICEBERG_ENDPOINT,
                "token": self.endpoint.token,
                "warehouse": self.cfg.warehouse,
                "adls.account-name": "onelake",
                "adls.account-host": ONELAKE_BLOB,
                "header.X-Iceberg-Access-Delegation": "vended-credentials",
            },
        )
        table = catalog.load_table(self.identifier(name))
        keys = sorted(k for k in table.io.properties if k.startswith("adls.sas-token"))
        if not keys:
            raise Refused(
                "pyiceberg resolved no vended credential (no adls.sas-token key in the table's "
                f"FileIO); the table's metadata location is {table.metadata_location!r}"
            )
        count = table.scan().to_arrow().num_rows
        table.append(rows([(4, 40)]))
        after = self.load(name).scan().to_arrow().num_rows
        return f"FileIO holds {keys}; read {count} rows, appended one, now {after}"

    def credential_vending(self) -> str:
        """Storage credentials scoped to one table, asked both ways a client can: GET
        .../credentials, and loadTable with X-Iceberg-Access-Delegation, which is where the Java
        client reads them. Full prefixes and config KEYS are reported -- never a value. The Java
        client matches a prefix against the table's abfss:// paths, so its exact form matters."""
        self.need_baseline()
        path = f"namespaces/{NAMESPACE}/tables/{self.baseline}"

        def describe(creds) -> list[str]:
            return [f"{c.get('prefix')!r} -> {sorted(c.get('config', {}))}" for c in creds or []]

        endpoint = self.endpoint.call("GET", f"{path}/credentials")
        loaded = self.endpoint.call(
            "GET", path, headers={"X-Iceberg-Access-Delegation": "vended-credentials"}
        )
        detail = (
            f"GET /credentials: {describe(endpoint.get('storage-credentials'))}; "
            f"loadTable with the delegation header: storage-credentials "
            f"{describe(loaded.get('storage-credentials'))}, config keys "
            f"{sorted(loaded.get('config') or {})}, metadata location "
            f"{loaded.get('metadata', {}).get('location')!r}"
        )
        if not endpoint.get("storage-credentials"):
            raise NoOp(f"no storage-credentials from GET /credentials; {detail}")
        return detail

    # -- refs ------------------------------------------------------------------------------------

    def create_tag(self) -> str:
        table = self.fresh("tag")
        snapshot = table.current_snapshot()
        with table.manage_snapshots() as manage:
            manage.create_tag(snapshot_id=snapshot.snapshot_id, tag_name="probe-tag")
        return f"tag on snapshot {snapshot.snapshot_id}"

    def create_branch(self) -> str:
        table = self.fresh("branch")
        snapshot = table.current_snapshot()
        with table.manage_snapshots() as manage:
            manage.create_branch(snapshot_id=snapshot.snapshot_id, branch_name="probe-branch")
        return f"branch on snapshot {snapshot.snapshot_id}"

    def pointers(self, table: str) -> str:
        """current-snapshot-id and refs.main as the catalog stores them, read as raw REST JSON."""
        meta = self.endpoint.call("GET", f"namespaces/{NAMESPACE}/tables/{table}")
        meta = meta.get("metadata", {})
        current = meta.get("current-snapshot-id")
        main = meta.get("refs", {}).get("main", {}).get("snapshot-id")
        flag = "" if current == main else ", THEY DISAGREE"
        return f"the catalog holds current-snapshot-id {current}, refs.main {main}{flag}"

    def write_to_branch(self) -> str:
        """Write-audit-publish: append to a branch, main must not move."""
        table = self.fresh("wap")
        name = self.created[-1]
        with table.manage_snapshots() as manage:
            manage.create_branch(
                snapshot_id=table.current_snapshot().snapshot_id, branch_name="audit"
            )
        table.append(rows([(7, 70)]), branch="audit")
        table = self.load(name)
        main = table.scan().to_arrow().num_rows
        branch = table.scan(snapshot_id=table.metadata.refs["audit"].snapshot_id).to_arrow()
        pointers = self.pointers(name)
        if main != 3 or branch.num_rows != 4:
            raise NoOp(f"main has {main} rows, the branch {branch.num_rows}; {pointers}")
        return f"main has {main} rows, the branch has {branch.num_rows}; {pointers}"

    # -- reading ---------------------------------------------------------------------------------

    def time_travel(self) -> str:
        table = self.fresh("travel")
        first = table.current_snapshot().snapshot_id
        table.append(rows([(4, 40)]))
        count = table.scan(snapshot_id=first).to_arrow().num_rows
        if count != 3:
            raise NoOp(f"scan(snapshot_id=<first>) reads {count} rows, not 3")
        return f"scan(snapshot_id=<first snapshot>) reads {count} rows (the table has 4)"

    def metadata_tables(self) -> str:
        table = self.fresh("inspect")
        snapshots = table.inspect.snapshots().num_rows
        files = table.inspect.files().num_rows
        return f"inspect.snapshots() reads {snapshots} snapshot(s), inspect.files() {files} file(s)"

    # -- maintenance -----------------------------------------------------------------------------

    def rollback(self) -> str:
        """manage_snapshots().rollback_to_snapshot: moves refs.main, nothing else."""
        table = self.fresh("rollback")
        name = self.created[-1]
        first = table.current_snapshot().snapshot_id
        table.append(rows([(4, 40)]))
        with table.manage_snapshots() as manage:
            manage.rollback_to_snapshot(first)
        count = self.load(name).scan().to_arrow().num_rows
        pointers = self.pointers(name)
        if count != 3:
            raise NoOp(
                f"returned success and the table reads {count} rows, not 3; asked for {first}, "
                f"{pointers}"
            )
        return f"rolled back to 3 rows; {pointers}"

    def expire_snapshots(self) -> str:
        """pyiceberg sends ONE remove-snapshots carrying every id, where the Java client sends one
        per snapshot and hits the one-update-type-per-commit rule."""
        table = self.fresh("expire")
        name = self.created[-1]
        table.append(rows([(4, 40)]))
        table.append(rows([(5, 50)]))
        current = table.current_snapshot().snapshot_id
        old = [s.snapshot_id for s in table.snapshots() if s.snapshot_id != current]
        before = len(table.snapshots())
        table.maintenance.expire_snapshots().by_ids(old).commit()
        table = self.load(name)
        after = len(table.snapshots())
        if after >= before:
            raise NoOp(f"returned success and history went {before} -> {after}")
        return (
            f"history {before} -> {after} snapshot(s); {table.scan().to_arrow().num_rows} rows kept"
        )

    # -- catalog operations -----------------------------------------------------------------------

    def rename(self) -> str:
        """Renamed, AND still readable. OneLake moves the table's folder on a rename, so the
        name alone proves nothing: the data has to be read back from under the new name."""
        self.fresh("rename_from")
        source = self.created[-1]
        target = self.name("rename_to")
        self.catalog.rename_table(self.identifier(source), self.identifier(target))
        self.created.remove(source)
        self.created.append(target)
        table = self.load(target)
        try:
            count = table.scan().to_arrow().num_rows
        except Exception as exc:  # noqa: BLE001 - an unreadable table IS the answer
            raise NoOp(
                f"renamed, and the data is unreadable under the new name; location "
                f"{table.location().rsplit('/', 2)[-2:]}: {scrub.scrub_exc(exc, 300)}"
            ) from None
        if count != 3:
            raise NoOp(f"renamed, and the table reads {count} row(s), not 3")
        return f"{source} renamed to {target}, and its 3 rows read back"

    def register(self) -> str:
        """registerTable: adopt an existing metadata.json under a new name."""
        table = self.need_baseline()
        target = self.name("registered")
        self.catalog.register_table(self.identifier(target), table.metadata_location)
        self.created.append(target)
        return "an existing metadata.json can be registered under a new name"

    def multi_table_transaction(self) -> str:
        """POST /v1/{prefix}/transactions/commit -- the spec's multi-table commit."""
        self.endpoint.call("POST", "transactions/commit", {"table-changes": []})
        return "the multi-table transaction endpoint answers"

    def drop_purge(self) -> str:
        self.create("droppable")
        table = self.created.pop()
        self.catalog.purge_table(self.identifier(table))
        return "DROP TABLE with purge is honoured"

    # -- teardown ---------------------------------------------------------------------------------

    def drop_everything(self) -> None:
        if self.catalog is None or not self.created:
            return
        if self.keep:
            print(f"\nCAPABILITY_KEEP=1; left {len(self.created)} table(s) in {NAMESPACE}")
            return
        dropped, left = 0, []
        for table in self.created:
            try:
                self.catalog.drop_table(self.identifier(table))
                dropped += 1
            except Exception as exc:  # noqa: BLE001 - teardown is best effort, by design
                left.append(f"{table} ({scrub.scrub_exc(exc, 80)})")
        print(f"\ndropped {dropped} probe table(s)" + (f"; left behind {left}" if left else ""))


PROBES = [
    ("endpoint", "GET /v1/config, and does it declare its endpoints", "config"),
    ("endpoint", "pyiceberg RestCatalog attaches", "catalog_loads"),
    ("namespace", "createNamespace", "create_namespace"),
    ("namespace", "loadNamespaceMetadata", "load_namespace"),
    ("namespace", "updateNamespaceProperties", "namespace_properties"),
    ("create", "createTable, field ids from 1, with location", "create_table"),
    ("create", "createTable with a field id of 0 (Spark numbering)", "create_field_id_zero"),
    ("create", "createTable with no location", "create_without_location"),
    ("create", "createTable with stage-create, is it honoured", "create_staged"),
    ("create", "createTable partitioned, then write it", "create_partitioned"),
    ("create", "createTable with a sort order", "create_sorted"),
    ("create", "partitioned by bucket(4, id), then append", "partition_bucket"),
    ("create", "partitioned by truncate(2, x), then append", "partition_truncate"),
    ("create", "partitioned by year / month / day / hour, then append", "partition_temporal"),
    ("create", "types: decimal, date, timestamp, timestamptz, uuid, binary", "types_scalar"),
    ("create", "nested types: struct, list, map", "types_nested"),
    ("write", "append, one snapshot per commit", "append"),
    ("write", "two snapshots in ONE commit", "two_snapshots_one_commit"),
    ("write", "overwrite the whole table", "overwrite_whole"),
    ("write", "overwrite by filter", "overwrite_filtered"),
    ("write", "delete taking whole data files", "delete_whole_file"),
    ("write", "delete that rewrites part of a file", "delete_partial_file"),
    ("write", "upsert", "upsert"),
    ("commit", "is assert-ref-snapshot-id enforced", "stale_assertion"),
    ("commit", "add a column", "add_column"),
    ("commit", "drop a column", "drop_column"),
    ("commit", "rename a column", "rename_column"),
    ("commit", "type promotion, int -> long", "type_promotion"),
    ("commit", "partition evolution (add a bucket field)", "partition_evolution"),
    ("commit", "sort order evolution, update_sort_order()", "sort_order_evolution"),
    ("commit", "set a table property", "set_property"),
    ("endpoint", "credential vending, GET .../credentials", "credential_vending"),
    ("endpoint", "read and append on vended credentials alone", "vended_only"),
    ("read", "time travel, scan(snapshot_id=)", "time_travel"),
    ("read", "metadata tables, inspect.snapshots() and inspect.files()", "metadata_tables"),
    ("refs", "create a tag", "create_tag"),
    ("refs", "create a branch", "create_branch"),
    ("refs", "append to a branch (write-audit-publish)", "write_to_branch"),
    ("maintenance", "manage_snapshots().rollback_to_snapshot", "rollback"),
    ("maintenance", "maintenance.expire_snapshots()", "expire_snapshots"),
    ("catalog", "renameTable", "rename"),
    ("catalog", "registerTable", "register"),
    ("catalog", "multi-table transactions", "multi_table_transaction"),
    ("catalog", "dropTable with purge", "drop_purge"),
]


def write_step_summary(probe: Capability) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if not target:
        return
    counts = probe.report.tally()
    lines = [
        "## OneLake Iceberg REST catalog: what it supports",
        "",
        f"{counts[SUPPORTED]} supported · {counts[REFUSED]} refused · "
        f"{counts[NOOP]} accepted then ignored · "
        f"{counts[SKIPPED]} skipped · {counts[BROKEN]} could not be asked",
        "",
        *probe.report.markdown(),
        "",
        f"pyiceberg {_pyiceberg_version()} · run `{probe.run}`",
    ]
    with open(target, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def _pyiceberg_version() -> str:
    try:
        from importlib.metadata import version

        return version("pyiceberg")
    except Exception:  # noqa: BLE001 - a version string must never fail the run
        return "unknown"


def main() -> int:
    cfg = Config.from_env()
    probe = Capability(cfg)
    print(
        f"workspace {cfg.workspace_id}  lakehouse {cfg.lakehouse_id}  namespace {NAMESPACE}\n"
        f"pyiceberg {_pyiceberg_version()}, run {probe.run}"
    )

    try:
        for group, question, method in PROBES:
            ok = probe.report.run(group, question, getattr(probe, method))
            # The first two are the credential chain. Without them nothing else means anything.
            if not ok and method in ("config", "catalog_loads"):
                print(f"\nstopped at {method}: the catalog was never reached")
                write_step_summary(probe)
                return 1
    finally:
        try:
            probe.drop_everything()
        except Exception as exc:  # noqa: BLE001 - teardown must not mask the findings
            print(f"  warning: teardown failed: {scrub.scrub_exc(exc, 200)}")

    counts = probe.report.tally()
    print("\n" + "\n".join(probe.report.markdown()))
    print(
        f"\n{counts[SUPPORTED]} supported, {counts[REFUSED]} refused, "
        f"{counts[NOOP]} accepted then ignored, "
        f"{counts[SKIPPED]} skipped, {counts[BROKEN]} could not be asked"
    )
    write_step_summary(probe)
    # A refusal is a finding, not a failure: this script answers questions, it does not gate.
    return 0


if __name__ == "__main__":
    sys.exit(main())
