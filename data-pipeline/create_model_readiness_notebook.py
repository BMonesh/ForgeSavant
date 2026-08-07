"""Create and execute the reader-facing model-readiness notebook."""

from __future__ import annotations

from pathlib import Path

import nbformat
from nbclient import NotebookClient


PROJECT_DIR = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = Path(__file__).resolve().parent / "notebooks" / "model_readiness.ipynb"


def build_notebook():
    notebook = nbformat.v4.new_notebook()
    notebook["metadata"]["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    notebook["metadata"]["language_info"] = {"name": "python", "version": "3"}
    notebook["cells"] = [
        nbformat.v4.new_markdown_cell("# ForgeSavant model-readiness assessment\n\n## tl;dr"),
        nbformat.v4.new_code_cell("""from pathlib import Path
import json
import duckdb
import pandas as pd
from IPython.display import Markdown, display

project_dir = Path.cwd()
analytics_dir = project_dir / "data-pipeline" / "analytics"
readiness = json.loads((analytics_dir / "model_readiness_summary.json").read_text(encoding="utf-8"))
dataset = readiness["dataset"]
display(Markdown(
    f"**Descriptive quality monitoring is ready, but predictive ML is not.** "
    f"The warehouse contains **{dataset['distinctProducts']} distinct products** covering "
    f"**{dataset['catalogCoverageRate']:.1%}** of the {dataset['verifiedProducts']}-product catalog. "
    "It has no retailer-price series, performance targets, or user-outcome labels."
))"""),
        nbformat.v4.new_markdown_cell("""## Context & Methods

This diagnostic asks which analytical and machine-learning uses are supported by the current immutable product-content warehouse. It checks grain, key uniqueness, temporal validity, category coverage, label availability, and collection history.

### Key Assumptions

- Open Icecat observations are product-content evidence, not retailer offers or benchmark results.
- A known duplicate is a valid idempotent pipeline outcome and is not a second training row.
- Predictive readiness requires an independently observed target and an evaluation split appropriate to the intended use."""),
        nbformat.v4.new_code_cell("""database_path = analytics_dir / "forgesavant.duckdb"
summary_path = analytics_dir / "data_quality_summary.json"
assert database_path.exists(), "Run npm run analytics:build first"
assert summary_path.exists(), "Run npm run analytics:build first"
connection = duckdb.connect(str(database_path), read_only=True)"""),
        nbformat.v4.new_markdown_cell("## Data\n\nThe analytical grain is one immutable normalized source observation per source product content version."),
        nbformat.v4.new_code_cell("""profile = pd.DataFrame([{
    "Observations": dataset["observations"],
    "Unique observation IDs": dataset["distinctObservationIds"],
    "Distinct products": dataset["distinctProducts"],
    "Verified catalog": dataset["verifiedProducts"],
    "Collection dates": dataset["distinctIngestionDates"],
    "Future observations": dataset["futureObservations"],
    "Median specification fields": dataset["medianSpecificationFields"],
}])
display(profile)

assert dataset["observations"] == dataset["distinctObservationIds"]
assert dataset["futureObservations"] == 0"""),
        nbformat.v4.new_markdown_cell("## Results\n\n### Coverage is concentrated in four categories"),
        nbformat.v4.new_code_cell("""category_coverage = pd.DataFrame(readiness["categories"])
category_coverage["coverage"] = category_coverage["coverageRate"].map(lambda value: f"{value:.1%}" if value is not None else "n/a")
display(category_coverage[["category", "observedProducts", "verifiedProducts", "coverage"]])"""),
        nbformat.v4.new_markdown_cell("### Current evidence supports monitoring, not prediction"),
        nbformat.v4.new_code_cell("""use_readiness = pd.DataFrame(readiness["uses"])
display(use_readiness)
check_results = pd.DataFrame([{"check": key, "passed": value} for key, value in readiness["checks"].items()])
display(check_results)"""),
        nbformat.v4.new_markdown_cell("""## Takeaways

- Use the current warehouse for provenance, completeness, source-coverage, and ingestion-health analysis.
- Pilot reviewed product-content enrichment only on the 14 covered products; do not infer missing categories from them.
- Keep the existing rule-based compatibility engine as the production decision layer until labeled outcomes exist.
- Do not train or advertise a price forecast, performance predictor, or personalized recommender from this dataset."""),
        nbformat.v4.new_code_cell("""display(Markdown("### Evidence required next\\n" + "\\n".join(f"- {item}" for item in readiness["requiredNextEvidence"])))
connection.close()"""),
    ]
    for index, cell in enumerate(notebook["cells"]):
        cell["id"] = f"forgesavant-{index:02d}"
    return notebook


def normalize_execution_metadata(notebook):
    for cell in notebook["cells"]:
        cell.get("metadata", {}).pop("execution", None)
    notebook["metadata"]["kernelspec"] = {"display_name": "Python 3", "language": "python", "name": "python3"}
    notebook["metadata"]["language_info"] = {"name": "python", "version": "3"}


def main() -> int:
    NOTEBOOK_PATH.parent.mkdir(parents=True, exist_ok=True)
    notebook = build_notebook()
    NotebookClient(notebook, timeout=120, kernel_name="python3", resources={"metadata": {"path": str(PROJECT_DIR)}}).execute()
    normalize_execution_metadata(notebook)
    NOTEBOOK_PATH.write_text(nbformat.writes(notebook), encoding="utf-8", newline="\n")
    print(NOTEBOOK_PATH)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
