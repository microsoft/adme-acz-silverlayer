import copy
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"

sys.path.insert(0, str(SRC))

from adme_acz_silverlayer import notebook_sync  # noqa: E402


class NotebookSyncTests(unittest.TestCase):
    def test_removed_reference_generator_is_not_advertised(self) -> None:
        import adme_acz_silverlayer

        self.assertNotIn("reference_notebook", adme_acz_silverlayer.__all__)
        self.assertNotIn("--reference", notebook_sync.build_parser().format_help())

    def test_committed_notebook_is_clean_and_self_contained(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)

        self.assertTrue(notebook_sync.notebook_is_clean(nb))
        self.assertEqual(notebook_sync.validation_issues(nb), [])

    def test_clean_notebook_removes_execution_artifacts(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)
        dirty = copy.deepcopy(nb)
        first_code_cell = next(cell for cell in dirty["cells"] if cell["cell_type"] == "code")
        first_code_cell["execution_count"] = 12
        first_code_cell["outputs"] = [{"output_type": "stream", "name": "stdout", "text": ["hello\n"]}]

        cleaned = notebook_sync.clean_notebook(dirty)

        self.assertNotEqual(cleaned, dirty)
        self.assertTrue(notebook_sync.notebook_is_clean(cleaned))
        self.assertEqual(notebook_sync.validation_issues(cleaned), [])

    def test_summary_reflects_current_notebook_shape(self) -> None:
        summary = notebook_sync.summarize_notebook(NOTEBOOK)

        self.assertEqual(summary.path, NOTEBOOK)
        self.assertGreaterEqual(summary.cells, 20)
        self.assertGreaterEqual(summary.code_cells, 1)
        self.assertGreaterEqual(summary.markdown_cells, 1)
        self.assertIn("## Run pipeline", summary.headings)

    def test_parser_does_not_advertise_unavailable_reference_generator(self) -> None:
        with self.assertRaises(SystemExit):
            notebook_sync.build_parser().parse_args(["--reference"])

    def test_shared_helper_sync_is_idempotent(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)
        synchronized = notebook_sync.synchronize_shared_helpers(nb)
        self.assertEqual(synchronized, nb)
        self.assertEqual(notebook_sync.synchronize_shared_helpers(synchronized), synchronized)

    def test_shared_helper_drift_is_detected_and_repaired(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)
        dirty = copy.deepcopy(nb)
        cell = next(
            cell for cell in dirty["cells"]
            if "def _flatten_typed_structs(" in "".join(cell.get("source", []))
        )
        cell["source"] = "".join(cell["source"]).replace(
            "return parent.select(select_exprs)", "return parent.select(select_exprs).limit(1)"
        ).splitlines(keepends=True)
        self.assertNotEqual(dirty, nb)
        self.assertIn(
            "Shared Spark helpers differ from the package source; run scripts/sync_notebook.py.",
            notebook_sync.validation_issues(dirty),
        )
        self.assertEqual(notebook_sync.synchronize_shared_helpers(dirty), nb)

    def test_check_reports_helper_drift_without_writing(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)
        cell = next(
            cell for cell in nb["cells"]
            if "def _flatten_typed_structs(" in "".join(cell.get("source", []))
        )
        cell["source"] = "".join(cell["source"]).replace(
            "return parent.select(select_exprs)", "return parent.select(select_exprs).limit(1)"
        ).splitlines(keepends=True)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "notebook.ipynb"
            notebook_sync.write_notebook(path, nb)
            before = path.read_bytes()
            with mock.patch("sys.stderr", new=io.StringIO()) as errors:
                self.assertEqual(notebook_sync.main([str(path), "--check"]), 1)
            self.assertIn("not synchronized", errors.getvalue())
            self.assertEqual(path.read_bytes(), before)
            self.assertTrue(notebook_sync.sync_notebook(path))
            self.assertFalse(notebook_sync.sync_notebook(path, check=True))

    def test_shared_type_constants_are_synchronized(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)
        dirty = copy.deepcopy(nb)
        cell = next(
            cell for cell in dirty["cells"]
            if "_JSON_TYPE_MAP:" in "".join(cell.get("source", []))
        )
        cell["source"] = "".join(cell["source"]).replace(
            '"integer": T.LongType()', '"integer": T.StringType()'
        ).splitlines(keepends=True)
        self.assertNotEqual(dirty, nb)
        self.assertEqual(notebook_sync.synchronize_shared_helpers(dirty), nb)

    def test_missing_or_duplicate_shared_helpers_fail_explicitly(self) -> None:
        nb = notebook_sync.load_notebook(NOTEBOOK)
        cell = next(
            cell for cell in nb["cells"]
            if "def _flatten_typed_structs(" in "".join(cell.get("source", []))
        )
        duplicate = copy.deepcopy(nb)
        duplicate["cells"].append(copy.deepcopy(cell))
        with self.assertRaisesRegex(ValueError, "occurs more than once"):
            notebook_sync.synchronize_shared_helpers(duplicate)
        cell["source"] = "".join(cell["source"]).replace(
            "def _flatten_typed_structs(", "def unsupported_flatten("
        ).splitlines(keepends=True)
        with self.assertRaisesRegex(ValueError, "missing shared helper"):
            notebook_sync.synchronize_shared_helpers(nb)


if __name__ == "__main__":
    unittest.main()
