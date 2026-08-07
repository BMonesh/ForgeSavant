"""Official Blender Open Data aggregate benchmark connector."""

from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import urlencode

import requests


BASE_URL = "https://opendata.blender.org/benchmarks/query/"
PUBLIC_DOMAIN_NOTICE = "https://www.blender.org/news/introducing-blender-benchmark/"
GPU_COMPUTE_TYPES = ("OPTIX", "CUDA", "HIP", "METAL", "ONEAPI")


def normalized_device(value: object) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", str(value or "").casefold()))


@dataclass(frozen=True)
class AggregateResult:
    device_name: str
    median_score: float
    benchmark_count: int
    query_url: str


class BlenderOpenDataConnector:
    def __init__(self, *, timeout: int = 30, session=None):
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "ForgeSavant/1.0 benchmark-connector"})

    def _fetch(self, params: list[tuple[str, str]]) -> tuple[list[AggregateResult], str]:
        response = self.session.get(BASE_URL, params=params, timeout=self.timeout)
        response.raise_for_status()
        if "application/json" not in response.headers.get("content-type", ""):
            raise ValueError("Blender Open Data returned a non-JSON response")
        payload = response.json()
        expected = ["Device Name", "Median Score", "Number of Benchmarks"]
        if [column.get("display_name") for column in payload.get("columns", [])] != expected:
            raise ValueError("Blender Open Data aggregate schema changed")
        query_url = f"{BASE_URL}?{urlencode(params)}"
        rows = []
        for row in payload.get("rows", []):
            if not isinstance(row, list) or len(row) != 3:
                continue
            rows.append(AggregateResult(str(row[0]), float(row[1]), int(row[2]), query_url))
        return rows, query_url

    def cpu_results(self, version: str) -> list[AggregateResult]:
        rows, _ = self._fetch([
            ("compute_type", "CPU"),
            ("blender_version", version),
            ("group_by", "device_name"),
            ("response_type", "datatables"),
        ])
        return rows

    def gpu_results(self, version: str) -> list[AggregateResult]:
        params = [("compute_type", value) for value in GPU_COMPUTE_TYPES]
        params.extend([
            ("blender_version", version),
            ("group_by", "device_name"),
            ("response_type", "datatables"),
        ])
        rows, _ = self._fetch(params)
        return rows


def match_verified_device(identity: dict, results: list[AggregateResult], category: str) -> AggregateResult | None:
    catalog_name = identity.get("currentName") or identity.get("name")
    needle = normalized_device(catalog_name)
    matches = []
    for result in results:
        candidate = normalized_device(result.device_name)
        matched = candidate == needle if category == "gpus" else needle in candidate
        if matched:
            matches.append(result)
    if not matches:
        return None
    # The aggregate endpoint can contain whitespace variants as separate rows.
    # Prefer the exact normalized device with the largest evidence base.
    return sorted(matches, key=lambda row: (-row.benchmark_count, row.device_name))[0]
