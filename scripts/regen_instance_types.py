"""Regenerate the instance type lists in
src/aibs_informatics_cdk_lib/constructs_/batch/instance_types.py.

Source of truth: this script. The notebook at notebooks/ec2-instance-type-selection.ipynb
is for exploration only.

Usage:
    python scripts/regen_instance_types.py \\
        --region us-west-2 \\
        --profile sandbox \\
        --output src/aibs_informatics_cdk_lib/constructs_/batch/instance_types.py

Filters are composable. Each *_FILTERS list below describes one preset. To disable a
filter, comment it out or remove it from the list.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Tuple

# --------------------------------------------------------------------------------------
# Compute / pricing model
# --------------------------------------------------------------------------------------

# Coefficients used to make apples-to-apples price comparisons across instance shapes.
# Effective price for a hypothetical 16-core / 64 GiB machine:
#   (price_per_vcpu * 16 + price_per_gib * 64) / 2
COMPUTE_COEFF = 16.0
MEM_COEFF = 64.0

SPOT_ADVISOR_URL = "https://spot-bid-advisor.s3.amazonaws.com/spot-advisor-data.json"


# --------------------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------------------


def get_instance_type_spot_interruptions(
    os_: Literal["Linux", "Windows"] = "Linux", region: Optional[str] = None
) -> Dict[str, Tuple[float, float]]:
    """Fetch spot interruption rate ranges per instance type from the AWS Spot Advisor.

    Returns a dict of {instance_type: (lower_bound, upper_bound)}.
    """
    with urllib.request.urlopen(SPOT_ADVISOR_URL, timeout=30) as response:
        spot_advisor = json.loads(response.read()).get("spot_advisor", {})
    region = region or os.environ.get("AWS_REGION") or "us-west-2"

    try:
        region_data = spot_advisor[region][os_]
    except KeyError as exc:
        available_regions = ", ".join(sorted(spot_advisor.keys()))
        raise KeyError(
            f"Spot Advisor data missing region/OS: region={region!r}, os={os_!r}. "
            f"Available regions: {available_regions}"
        ) from exc

    rates: Dict[str, Tuple[float, float]] = {}
    for it, info in region_data.items():
        rate = info.get("r")
        if rate == 0:
            rates[it] = (0.0, 0.05)
        elif rate == 1:
            rates[it] = (0.05, 0.10)
        elif rate == 2:
            rates[it] = (0.10, 0.15)
        elif rate == 3:
            rates[it] = (0.15, 0.20)
        else:
            # Upper limit is approximate; advisor doesn't bound it.
            rates[it] = (0.20, 0.65)
    return rates


_SIZE_RANK = {"nano": 0, "micro": 1, "small": 2, "medium": 3, "large": 4, "metal": 5}
_SORT_PATTERN = re.compile(
    r"([\w-]+)\.((\d*)x)?(nano|micro|small|medium|large|metal)(?:-(\d+)xl)?"
)


def instance_type_sort_key(instance_type: str) -> Tuple[str, int, int, int]:
    """Sort key (family, size_rank, factor, metal_factor).

    Examples:
        c5.2xlarge       -> ('c5', 4, 2, 0)
        m7i-flex.metal   -> ('m7i-flex', 5, 0, 0)
        m7i.metal-24xl   -> ('m7i', 5, 0, 24)
        m7i.metal-48xl   -> ('m7i', 5, 0, 48)
    """
    match = _SORT_PATTERN.match(instance_type)
    if match is None:
        raise ValueError(f"Invalid instance type: {instance_type}")
    family, factorstr, factornum, size, metal_factor = match.groups()
    size_rank = _SIZE_RANK[size]
    factor = int(factornum) if factornum else (1 if factorstr and "x" in factorstr else 0)
    metal_n = int(metal_factor) if metal_factor else 0
    return (family, size_rank, factor, metal_n)


def network_performance_to_gbps(network_performance: str) -> float:
    """Approximate Gbps from AWS's NetworkPerformance string.

    Note: 'Up to N Gigabit' returns N — which is a burst ceiling, not sustained
    bandwidth. Callers that need sustained throughput should also exclude entries
    starting with 'Up to ' (see is_sustained_network()).
    """
    pattern = re.compile(r"(\d+(?:\.\d*)?)\s*Gigabit")
    rough = {"Low": 0.05, "Moderate": 0.3, "High": 1.0}
    if network_performance in rough:
        return rough[network_performance]
    if match := pattern.search(network_performance):
        return float(match.group(1))
    raise ValueError(f"Invalid network performance: {network_performance}")


def is_sustained_network(network_performance: str) -> bool:
    """Whether the instance sustains its advertised network rate (vs burst-only)."""
    return not network_performance.startswith("Up to ")


# --------------------------------------------------------------------------------------
# Filter definitions
# --------------------------------------------------------------------------------------


@dataclass
class InstanceFilter:
    """A single composable filter on enriched instance-type info dicts."""

    name: str
    predicate: Callable[[Dict[str, Any]], bool]
    enabled: bool = True

    def apply(self, info: Dict[str, Any]) -> bool:
        return self.predicate(info) if self.enabled else True


# --- Reusable predicates ---


def f_max_memory_gib(limit: float) -> InstanceFilter:
    return InstanceFilter(
        f"memory_gib<={limit}",
        lambda i: i["MemoryInfo.SizeInGiB"] <= limit,
    )


def f_min_memory_gib(limit: float) -> InstanceFilter:
    return InstanceFilter(
        f"memory_gib>{limit}",
        lambda i: i["MemoryInfo.SizeInGiB"] > limit,
    )


def f_max_vcpus(limit: int) -> InstanceFilter:
    return InstanceFilter(
        f"vcpus<={limit}",
        lambda i: i["VCpuInfo.DefaultVCpus"] <= limit,
    )


def f_max_price_per_compute(limit: float) -> InstanceFilter:
    return InstanceFilter(
        f"$/16c64g<{limit}",
        lambda i: i["PricingOnDemandPerCompute"] < limit,
    )


def f_min_network_gbps(limit: float, sustained_only: bool = False) -> InstanceFilter:
    name = f"net>={limit}gbps" + ("(sustained)" if sustained_only else "")

    def predicate(i: Dict[str, Any]) -> bool:
        if i["NetworkInfo.NetworkPerformanceGbps"] < limit:
            return False
        if sustained_only and not is_sustained_network(i["NetworkInfo.NetworkPerformance"]):
            return False
        return True

    return InstanceFilter(name, predicate)


def f_max_spot_interruption(rate: float) -> InstanceFilter:
    return InstanceFilter(
        f"spot_interrupt<={rate}",
        lambda i: i["SpotInterruptionRateUpper"] <= rate,
    )


def f_exclude_families(prefixes: List[str]) -> InstanceFilter:
    return InstanceFilter(
        f"exclude:{','.join(prefixes)}",
        lambda i: not any(i["InstanceFamily"].startswith(p) for p in prefixes),
    )


def f_supports(usage_class: Literal["spot", "on-demand"]) -> InstanceFilter:
    return InstanceFilter(
        f"supports:{usage_class}",
        lambda i: usage_class in (i.get("SupportedUsageClasses") or []),
    )


# --- Preset definitions ---


@dataclass
class Preset:
    """A named instance-type preset built by chaining filters."""

    name: str
    filters: List[InstanceFilter] = field(default_factory=list)

    def select(self, candidates: List[Dict[str, Any]]) -> List[str]:
        out = []
        for it in candidates:
            if all(f.apply(it) for f in self.filters):
                out.append(it["InstanceType"])
        return out


# Common base — applied to every preset. Edit here to widen/narrow universe globally.
BASE_FILTERS = [
    f_max_memory_gib(1024),
    f_max_vcpus(256),
    f_exclude_families(["t"]),  # burstable, not suitable for batch jobs
    f_min_network_gbps(1.0),
]

# Accelerator/GPU exclusion — opt-in per preset. AWS's gpu_limits=(0, 0) only checks
# GpuInfo, so Inferentia/Trainium and fractional-GPU families (g6f, gr6f) slip
# through. Add CPU_ONLY_FILTERS to a preset's filter list to keep it CPU-only;
# omit them from a preset that should accept GPU/accelerator instances.
CPU_ONLY_FILTERS = [
    f_exclude_families(
        [
            "g",
            "gr",
            "p",  # NVIDIA GPU
            "inf",
            "trn",  # AWS Inferentia / Trainium
            "dl",  # Habana Gaudi
            "vt",
            "f",  # video transcoding / FPGA
        ]
    ),
]


def build_presets(arch: Literal["x86_64", "arm64"]) -> List[Preset]:
    """Return the preset definitions for a given architecture.

    Each Preset reuses BASE_FILTERS plus per-preset constraints. Comment out
    filters or whole presets to disable them.
    """
    return [
        Preset(
            f"ON_DEMAND_INSTANCE_TYPES{'_ARM' if arch == 'arm64' else ''}",
            filters=[
                *BASE_FILTERS,
                *CPU_ONLY_FILTERS,
                f_supports("on-demand"),
                f_supports("spot"),
                f_max_price_per_compute(1.0),
            ],
        ),
        Preset(
            f"SPOT_INSTANCE_TYPES{'_ARM' if arch == 'arm64' else ''}",
            filters=[
                *BASE_FILTERS,
                *CPU_ONLY_FILTERS,
                f_supports("on-demand"),
                f_supports("spot"),
                f_max_price_per_compute(1.0),
                # Spot interruption filter — disable by setting enabled=False to rely on
                # the SPOT_PRICE_CAPACITY_OPTIMIZED allocation strategy alone for pool
                # selection. Kept enabled by default for backwards compatibility.
                f_max_spot_interruption(0.15),
            ],
        ),
        Preset(
            f"TRANSFER_INSTANCE_TYPES{'_ARM' if arch == 'arm64' else ''}",
            filters=[
                *BASE_FILTERS,
                *CPU_ONLY_FILTERS,
                f_max_memory_gib(8),
                f_max_vcpus(4),
                f_max_price_per_compute(1.50),
                f_min_network_gbps(10.0),
            ],
        ),
        Preset(
            f"MICRO_INSTANCE_TYPES{'_ARM' if arch == 'arm64' else ''}",
            filters=[
                *BASE_FILTERS,
                *CPU_ONLY_FILTERS,
                f_max_memory_gib(4),
                f_max_price_per_compute(1.50),
            ],
        ),
        Preset(
            f"SMALL_INSTANCE_TYPES{'_ARM' if arch == 'arm64' else ''}",
            filters=[
                *BASE_FILTERS,
                *CPU_ONLY_FILTERS,
                f_min_memory_gib(4),
                f_max_memory_gib(8),
                f_max_price_per_compute(1.50),
            ],
        ),
        Preset(
            f"MEDIUM_INSTANCE_TYPES{'_ARM' if arch == 'arm64' else ''}",
            filters=[
                *BASE_FILTERS,
                *CPU_ONLY_FILTERS,
                f_min_memory_gib(8),
                f_max_memory_gib(16),
                f_max_price_per_compute(1.50),
            ],
        ),
    ]


# --------------------------------------------------------------------------------------
# Data fetching / enrichment
# --------------------------------------------------------------------------------------


def fetch_enriched_instance_types(
    region: str, arch: Literal["x86_64", "arm64"]
) -> List[Dict[str, Any]]:
    """Pull instance type descriptions, prices, and spot interruption rates from AWS,
    flatten into plain dicts indexed by the same keys the filters reference.
    """
    # Imports are local so that --help works without AWS deps loaded.
    from aibs_informatics_aws_utils.ec2 import (  # noqa: F401
        describe_instance_types_by_props,
        get_instance_type_on_demand_price,
        get_instance_type_spot_price,
    )

    print(f"[{arch}] Describing instance types in {region}...", file=sys.stderr)
    # No gpu_limits filter here: GPU/accelerator exclusion is handled per-preset
    # via CPU_ONLY_FILTERS so a future GPU preset can opt back in.
    raw = describe_instance_types_by_props(
        architectures=[arch],
        vcpu_limits=(1, 256),
        memory_limits=(1, 1024 * 1024),
        on_demand_support=True,
        spot_support=True,
        regions=[region],
    )
    print(f"[{arch}]   {len(raw)} instance types", file=sys.stderr)

    print(f"[{arch}] Fetching pricing...", file=sys.stderr)
    for it in raw:
        if not (
            "spot" in it["SupportedUsageClasses"] and "on-demand" in it["SupportedUsageClasses"]
        ):
            continue
        it["Pricing"] = {
            "OnDemand": get_instance_type_on_demand_price(region, it["InstanceType"]),
            "Spot": get_instance_type_spot_price(region, it["InstanceType"]),
        }

    print(f"[{arch}] Fetching spot interruption rates...", file=sys.stderr)
    interruptions = get_instance_type_spot_interruptions(region=region)

    enriched = []
    for it in raw:
        if "Pricing" not in it or it["Pricing"]["OnDemand"] is None:
            continue
        vcpus = it["VCpuInfo"]["DefaultVCpus"]
        mem_gib = it["MemoryInfo"]["SizeInMiB"] / 1024.0
        per_vcpu = it["Pricing"]["OnDemand"] / vcpus
        per_gib = it["Pricing"]["OnDemand"] / mem_gib
        per_compute = (per_vcpu * COMPUTE_COEFF + per_gib * MEM_COEFF) / 2
        lo, hi = interruptions.get(it["InstanceType"], (0.0, 0.0))
        enriched.append(
            {
                "InstanceType": it["InstanceType"],
                "InstanceFamily": it["InstanceType"].split(".")[0],
                "VCpuInfo.DefaultVCpus": vcpus,
                "MemoryInfo.SizeInGiB": mem_gib,
                "NetworkInfo.NetworkPerformance": it["NetworkInfo"]["NetworkPerformance"],
                "NetworkInfo.NetworkPerformanceGbps": network_performance_to_gbps(
                    it["NetworkInfo"]["NetworkPerformance"]
                ),
                "SupportedUsageClasses": it["SupportedUsageClasses"],
                "PricingOnDemandPerCompute": per_compute,
                "SpotInterruptionRateUpper": hi,
            }
        )
    return enriched


# --------------------------------------------------------------------------------------
# Code generation
# --------------------------------------------------------------------------------------


HEADER = """# DO NOT EDIT — generated by scripts/regen_instance_types.py.
# Re-run: `python scripts/regen_instance_types.py --region <region> --profile <profile>`
"""

LEGACY_ALIASES = """
# --- Legacy aliases (kept for backwards compatibility; prefer the names above) ---
LAMBDA_SMALL_INSTANCE_TYPES: list[str] = MICRO_INSTANCE_TYPES
LAMBDA_MEDIUM_INSTANCE_TYPES: list[str] = SMALL_INSTANCE_TYPES
LAMBDA_LARGE_INSTANCE_TYPES: list[str] = MEDIUM_INSTANCE_TYPES
"""


def render_list(name: str, items: List[str]) -> str:
    sorted_items = sorted(items, key=instance_type_sort_key)
    if not sorted_items:
        return f"{name}: list[str] = []\n"
    body = ",\n".join(f'    "{it}"' for it in sorted_items)
    return f"{name}: list[str] = [\n{body},\n]\n"


def render_file(presets_by_arch: Dict[str, List[Tuple[str, List[str]]]]) -> str:
    chunks = [HEADER]
    for arch, pairs in presets_by_arch.items():
        chunks.append(f"\n# --- {arch} ---\n")
        for name, items in pairs:
            chunks.append("\n" + render_list(name, items))
    chunks.append(LEGACY_ALIASES)
    return "".join(chunks)


# --------------------------------------------------------------------------------------
# Entrypoint
# --------------------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="us-west-2")
    parser.add_argument("--profile", default=None, help="AWS profile to use")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).parent.parent
        / "src/aibs_informatics_cdk_lib/constructs_/batch/instance_types.py",
    )
    parser.add_argument(
        "--arch",
        choices=["x86_64", "arm64", "both"],
        default="both",
        help="Which architecture(s) to regenerate. 'both' produces x86_64 lists and "
        "*_ARM variants for arm64.",
    )
    args = parser.parse_args()

    if args.profile:
        os.environ["AWS_PROFILE"] = args.profile
    os.environ["AWS_REGION"] = args.region
    os.environ["AWS_DEFAULT_REGION"] = args.region

    archs: List[Literal["x86_64", "arm64"]] = (
        ["x86_64", "arm64"] if args.arch == "both" else [args.arch]
    )

    presets_by_arch: Dict[str, List[Tuple[str, List[str]]]] = {}
    for arch in archs:
        candidates = fetch_enriched_instance_types(args.region, arch)
        pairs = []
        for preset in build_presets(arch):
            selected = preset.select(candidates)
            print(f"[{arch}] {preset.name}: {len(selected)}", file=sys.stderr)
            pairs.append((preset.name, selected))
        presets_by_arch[arch] = pairs

    rendered = render_file(presets_by_arch)
    args.output.write_text(rendered)
    print(f"Wrote {args.output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
