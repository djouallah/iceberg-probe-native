"""The catalog resolves ids, not names."""

from __future__ import annotations

from bench.config import Config


def test_warehouse_is_guid_over_guid():
    cfg = Config(workspace_id="ws-guid", lakehouse_id="lh-guid")
    assert cfg.warehouse == "ws-guid/lh-guid"
    assert cfg.base_path == "abfss://ws-guid@onelake.dfs.fabric.microsoft.com/lh-guid"
