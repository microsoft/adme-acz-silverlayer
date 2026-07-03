import copy
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
NOTEBOOK = ROOT / "ADME ACZ Silver Layer.ipynb"

sys.path.insert(0, str(SRC))

from adme_acz_silverlayer import notebook_sync  # noqa: E402


class NotebookSyncTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
