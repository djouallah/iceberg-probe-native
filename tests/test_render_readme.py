import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "render_readme.py"
_spec = importlib.util.spec_from_file_location("render_readme", _SCRIPT)
render_readme = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(render_readme)

README = """# old title

stale table

<!-- blocked:start -->
## Blocked by the OneLake catalog

| Operation | Catalog |
|---|---|
| Rename table | no: `501` |
<!-- blocked:end -->

## Old section
"""


def _row(key, outcome="supported", detail=""):
    return {"key": key, "group": "g", "question": key, "outcome": outcome, "detail": detail}


DATA = {
    "duckdb": {
        "engine": "duckdb",
        "version": "v2.0.0-alpha1",
        "run": "1",
        "date": "2026-10-09",
        "rows": [
            _row("create_table"),
            _row("merge_into", "no", "400 Duplicate types: add-snapshot"),
            _row("merge_update_only"),
            _row("merge_delete_only"),
            _row("merge_insert_only", "no-op", "nothing written"),
            _row("merge_by_source_only"),
        ],
    },
    "polars": {
        "engine": "polars",
        "version": "2.0.0",
        "run": "1",
        "date": "2026-10-09",
        "rows": [_row("sink_append")],
    },
}


def test_cell_takes_the_worst_outcome_and_keeps_the_reasons():
    rows = {r["key"]: r for r in DATA["duckdb"]["rows"]}
    outcome, why = render_readme.cell(rows, ["merge_update_only", "merge_insert_only"])
    assert outcome == "no-op"
    assert [r["key"] for r in why] == ["merge_insert_only"]


def test_unprobed_is_a_dash_and_absent_engine_is_na():
    text = render_readme.render(DATA, README)
    line = next(ln for ln in text.splitlines() if ln.startswith("| UPDATE |"))
    assert line == "| UPDATE | na | — |"


def test_refusals_get_a_note_quoting_the_detail():
    text = render_readme.render(DATA, README)
    merge = next(ln for ln in text.splitlines() if ln.startswith("| MERGE INTO / upsert |"))
    assert merge == "| MERGE INTO / upsert | na | no ¹ |"
    assert "1. DuckDB, MERGE INTO / upsert: merge_into: `400 Duplicate types: add-snapshot`" in text


def test_the_blocked_section_is_kept_verbatim_and_the_rest_rewritten():
    text = render_readme.render(DATA, README)
    assert "| Rename table | no: `501` |" in text
    assert "stale table" not in text and "## Old section" not in text
    assert text.count("<!-- blocked:start -->") == 1


def test_duckdb_isolation_rows_fill_the_duckdb_column():
    data = {
        **DATA,
        "duckdb_isolation": {
            "engine": "duckdb_isolation",
            "version": "v2.0.0-alpha1",
            "run": "1",
            "date": "2026-10-09",
            "configs": {},
            "levels": {},
            "transactions": [],
            "combos": [],
            "rows": [_row("race_append"), _row("race_read_write", "no", "skew: final [...]")],
        },
    }
    text = render_readme.render(data, README)
    lines = text.splitlines()
    assert "| Concurrent append: both kept | — | yes |" in lines
    assert any(
        ln.startswith("| INSERT ... SELECT racing a writer") and "| — | no " in ln for ln in lines
    )
