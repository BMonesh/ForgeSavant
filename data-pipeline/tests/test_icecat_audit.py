import sys
from pathlib import Path
import unittest
from unittest.mock import patch


PIPELINE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PIPELINE_DIR))

import audit_icecat


class IcecatAuditReportTests(unittest.TestCase):
    def _run_with_empty_catalog(self, *arguments: str):
        with (
            patch.object(sys, "argv", ["audit_icecat.py", *arguments]),
            patch.object(audit_icecat, "load_entries", return_value=[]),
            patch.dict("os.environ", {"ICECAT_USERNAME": "user", "ICECAT_PASSWORD": "password"}),
            patch.object(audit_icecat.Path, "write_text", autospec=True) as write_text,
        ):
            self.assertEqual(audit_icecat.main(), 0)
            return write_text.call_args.args[0]

    def test_bounded_audit_does_not_overwrite_canonical_report(self):
        destination = self._run_with_empty_catalog("--component", "processors", "--limit", "5")
        self.assertEqual(destination, audit_icecat.SMOKE_REPORT_PATH)

    def test_full_audit_writes_canonical_report(self):
        destination = self._run_with_empty_catalog("--component", "processors")
        self.assertEqual(destination, audit_icecat.REPORT_PATH)


if __name__ == "__main__":
    unittest.main()
