"""Tests for tabular_file_utils helpers (JSON/CSV/Parquet shared code)."""

from __future__ import annotations

import builtins
import math
import sys
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest

from limbo_core.adapters.connections.errors import MissingPackageError
from limbo_core.domain.validation import ValidationError
from limbo_core.domain.value_objects import TabularBatch
from limbo_core.plugins.builtin.persistence.tabular_file_utils import (
    cell_from_json_value,
    cell_to_json_value,
    dump_json_bytes,
    ensure_parent_dir,
    load_json_document_from_bytes,
    normalize_arrow_scalar,
    safe_filename_stem,
    tabular_batch_from_json_document,
    tabular_batch_to_json_document,
    try_import_pyarrow,
)

if TYPE_CHECKING:
    from pathlib import Path


class TestSafeFilenameStem:
    def test_strips_path_to_basename(self) -> None:
        assert safe_filename_stem("dir/out.csv") == "out.csv"

    def test_strips_nul_bytes(self) -> None:
        assert safe_filename_stem("a\x00b") == "ab"

    @pytest.mark.parametrize("name", ["", ".", "..", "/.", "/.."])
    def test_invalid_raises(self, name: str) -> None:
        with pytest.raises(ValidationError, match="Invalid artifact name"):
            safe_filename_stem(name)


class TestEnsureParentDir:
    def test_creates_missing_parents(self, tmp_path: Path) -> None:
        target = tmp_path / "a" / "b" / "file.txt"
        ensure_parent_dir(target)
        assert target.parent.is_dir()


class TestTryImportPyarrow:
    def test_raises_missing_package_when_import_fails(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(sys.modules, "pyarrow", raising=False)
        real_import = builtins.__import__

        def fake_import(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "pyarrow":
                raise ImportError("simulated missing pyarrow")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        with pytest.raises(MissingPackageError, match="pyarrow"):
            try_import_pyarrow()

    def test_returns_module_when_available(self) -> None:
        pytest.importorskip("pyarrow")
        pa = try_import_pyarrow()
        assert pa.__name__ == "pyarrow"


class TestNormalizeArrowScalar:
    def test_none(self) -> None:
        assert normalize_arrow_scalar(None) is None

    def test_bool_preserved(self) -> None:
        assert normalize_arrow_scalar(True) is True
        assert normalize_arrow_scalar(False) is False

    def test_primitives_passthrough(self) -> None:
        d = date(2024, 1, 2)
        dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert normalize_arrow_scalar("x") == "x"
        assert normalize_arrow_scalar(1) == 1
        assert math.isclose(normalize_arrow_scalar(1.5), 1.5)
        assert normalize_arrow_scalar(d) == d
        assert normalize_arrow_scalar(dt) == dt

    def test_bytes_decoded_utf8(self) -> None:
        assert normalize_arrow_scalar(b"hello") == "hello"

    def test_invalid_utf8_bytes_use_replace(self) -> None:
        raw = b"\xff\xfe"
        out = normalize_arrow_scalar(raw)
        assert isinstance(out, str)
        assert "\ufffd" in out

    def test_unknown_type_stringified(self) -> None:
        class Odd:
            def __str__(self) -> str:
                return "odd"

        assert normalize_arrow_scalar(Odd()) == "odd"


class TestCellToJsonValue:
    def test_datetime_tagged(self) -> None:
        dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert cell_to_json_value(dt) == {
            "__type__": "datetime",
            "v": dt.isoformat(),
        }

    def test_date_tagged(self) -> None:
        d = date(2024, 6, 7)
        assert cell_to_json_value(d) == {"__type__": "date", "v": d.isoformat()}

    def test_other_passthrough(self) -> None:
        assert cell_to_json_value("plain") == "plain"
        assert cell_to_json_value(None) is None


class TestCellFromJsonValue:
    def test_datetime_tag(self) -> None:
        dt = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert (
            cell_from_json_value({"__type__": "datetime", "v": dt.isoformat()})
            == dt
        )

    def test_date_tag(self) -> None:
        assert cell_from_json_value({
            "__type__": "date",
            "v": "2024-01-02",
        }) == date(2024, 1, 2)

    def test_tagged_dict_with_bad_payload_raises(self) -> None:
        with pytest.raises(ValidationError, match="Unsupported JSON cell"):
            cell_from_json_value({"__type__": "datetime", "v": 123})

    def test_none_and_bool(self) -> None:
        assert cell_from_json_value(None) is None
        assert cell_from_json_value(True) is True
        assert cell_from_json_value(False) is False

    def test_str_int_float(self) -> None:
        assert cell_from_json_value("a") == "a"
        assert cell_from_json_value(3) == 3
        assert math.isclose(cell_from_json_value(2.5), 2.5)

    def test_unsupported_raises(self) -> None:
        with pytest.raises(ValidationError, match="Unsupported JSON cell"):
            cell_from_json_value([1, 2])


class TestTabularBatchJsonRoundTrip:
    def test_round_trip(self) -> None:
        batch = TabularBatch(
            column_names=("id", "d"), rows=({"id": 1, "d": date(2024, 1, 2)},)
        )
        doc = tabular_batch_to_json_document(batch)
        back = tabular_batch_from_json_document(doc)
        assert back.column_names == batch.column_names
        assert back.rows == batch.rows


class TestTabularBatchFromJsonDocumentErrors:
    def test_root_not_object(self) -> None:
        with pytest.raises(ValidationError, match="must be an object"):
            tabular_batch_from_json_document([])

    def test_column_names_invalid(self) -> None:
        with pytest.raises(ValidationError, match="column_names"):
            tabular_batch_from_json_document({
                "column_names": [1, 2],
                "rows": [],
            })

    def test_rows_not_list(self) -> None:
        with pytest.raises(ValidationError, match="rows must be a list"):
            tabular_batch_from_json_document({
                "column_names": ["a"],
                "rows": {},
            })

    def test_row_not_object(self) -> None:
        with pytest.raises(ValidationError, match=r"rows\[0\]"):
            tabular_batch_from_json_document({
                "column_names": ["a"],
                "rows": [[]],
            })


def _import_except(real_import: Any, blocked: str) -> Any:
    def fake(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == blocked:
            raise ImportError(f"blocked {blocked}")
        return real_import(name, *args, **kwargs)

    return fake


class TestDumpJsonBytes:
    def test_stdlib_fallback_when_orjson_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(sys.modules, "orjson", raising=False)
        monkeypatch.setattr(
            builtins,
            "__import__",
            _import_except(builtins.__import__, "orjson"),
        )
        doc = {"a": 1}
        raw = dump_json_bytes(doc)
        assert raw == b'{"a":1}'

    def test_orjson_when_available(self) -> None:
        orjson = pytest.importorskip("orjson")
        doc = {"b": [1, 2]}
        raw = dump_json_bytes(doc)
        assert raw == orjson.dumps(doc)


class TestLoadJsonDocumentFromBytes:
    def test_stdlib_fallback_when_orjson_missing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delitem(sys.modules, "orjson", raising=False)
        monkeypatch.setattr(
            builtins,
            "__import__",
            _import_except(builtins.__import__, "orjson"),
        )
        assert load_json_document_from_bytes(b'{"x":1}') == {"x": 1}

    def test_orjson_when_available(self) -> None:
        pytest.importorskip("orjson")
        assert load_json_document_from_bytes(b'{"y":2}') == {"y": 2}

    def test_non_object_root_raises(self) -> None:
        with pytest.raises(ValidationError, match="must be an object"):
            load_json_document_from_bytes(b"[]")
        with pytest.raises(ValidationError, match="must be an object"):
            load_json_document_from_bytes(b'"hi"')
