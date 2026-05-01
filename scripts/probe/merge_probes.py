"""
merge_probes.py

Merges the NAIP NDVI + OSIP texture probe CSVs on api_no, prints a
side-by-side table per stratum, and computes summary statistics.

Usage
-----
  python merge_probes.py --county hocking
  python merge_probes.py --county carroll
"""

import csv
import argparse
from collections import defaultdict


def load(path: str) -> dict[str, dict]:
    with open(path, encoding="utf-8") as f:
        return {row["api_no"]: row for row in csv.DictReader(f)}


def num(s):
    if s is None or s == "" or s == "None":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--county", default="hocking")
    parser.add_argument("--apis-file", default=None)
    args = parser.parse_args()

    county = args.county.lower()
    naip = load(f"probe_naip_ndvi_{county}.csv")
    text = load(f"probe_osip_texture_{county}.csv")

    apis_file = args.apis_file or (
        "hocking_sample_apis.txt" if county == "hocking"
        else f"sample_{county}_apis.txt"
    )

    apis: list[tuple[str, str]] = []
    with open(apis_file) as f:
        for line in f:
            line = line.strip()
            if not line: continue
            parts = line.split(",", 1)
            apis.append((parts[0], parts[1] if len(parts) > 1 else ""))

    by_stratum: dict[str, list[dict]] = defaultdict(list)
    rows = []
    for api_no, stratum in apis:
        n = naip.get(api_no, {})
        t = text.get(api_no, {})
        pad     = num(n.get("naip_ndvi_pad"))
        delta   = num(n.get("naip_ndvi_delta"))
        edge    = num(t.get("edge_ratio"))

        # Three-signal combined scoring: each pathway fires independently,
        # so wells in *any* of (forest pad, clearing pad, sharp-edge pad)
        # get surfaced. Tiered absolute threshold preserves info about
        # how non-vegetative the surface is.
        abs_signal = (30 if pad is not None and pad < 0.10 else
                      15 if pad is not None and pad < 0.20 else
                       5 if pad is not None and pad < 0.30 else 0)
        delta_signal = 30 if delta is not None and delta < -0.10 else (
                       15 if delta is not None and delta < -0.05 else 0)
        edge_signal  = (20 if edge is not None and edge > 1.50 else
                        10 if edge is not None and edge > 1.20 else 0)
        combined = min(80, abs_signal + delta_signal + edge_signal)

        merged = {
            "api_no":            api_no,
            "stratum":           stratum,
            "s2_ndvi_relative":  num(n.get("s2_ndvi_relative")),
            "s2_recent_ndvi":    num(n.get("s2_recent_ndvi")),
            "naip_ndvi_pad":     pad,
            "naip_ndvi_bg":      num(n.get("naip_ndvi_bg")),
            "naip_delta":        delta,
            "naip_score":        int(num(n.get("naip_score")) or 0),
            "vari_delta":        num(t.get("vari_delta")),
            "edge_ratio":        edge,
            "texture_score":     int(num(t.get("texture_score")) or 0),
            "abs_signal":        abs_signal,
            "delta_signal":      delta_signal,
            "edge_signal":       edge_signal,
            "combined_score":    combined,
        }
        rows.append(merged)
        by_stratum[stratum].append(merged)

    # Per-stratum table — show which of the three signals fired and the combined
    for stratum, items in by_stratum.items():
        print(f"\n=== Stratum: {stratum} ({len(items)} wells) ===")
        print(f"  {'api_no':<14}  {'pad NDVI':>8}  {'delta':>6}  {'edge':>5}  {'abs':>3}  {'dlt':>3}  {'edg':>3}  {'COMB':>4}")
        for r in items:
            pn = "       -" if r["naip_ndvi_pad"] is None else f"{r['naip_ndvi_pad']:>+8.3f}"
            nd = "     -" if r["naip_delta"]     is None else f"{r['naip_delta']:>+6.2f}"
            er = "    -" if r["edge_ratio"]      is None else f"{r['edge_ratio']:>5.2f}"
            print(f"  {r['api_no']}  {pn}  {nd}  {er}  {r['abs_signal']:>3}  {r['delta_signal']:>3}  {r['edge_signal']:>3}  {r['combined_score']:>4}")

    # Summary
    print("\n=== Summary ===")
    print(f"  {'metric':<35}  {'A':>3}  {'B':>3}  {'C':>3}  {'all':>4}")
    def count(items, pred):
        return sum(1 for r in items if pred(r))
    for label, pred in [
        ("Absolute pad NDVI < 0.10",      lambda r: r["naip_ndvi_pad"] is not None and r["naip_ndvi_pad"] < 0.10),
        ("Absolute pad NDVI < 0.20",      lambda r: r["naip_ndvi_pad"] is not None and r["naip_ndvi_pad"] < 0.20),
        ("Absolute pad NDVI < 0.30",      lambda r: r["naip_ndvi_pad"] is not None and r["naip_ndvi_pad"] < 0.30),
        ("NAIP delta < -0.10",            lambda r: r["naip_delta"]    is not None and r["naip_delta"] < -0.10),
        ("Edge ratio > 1.20",             lambda r: r["edge_ratio"]    is not None and r["edge_ratio"] > 1.20),
        ("Edge ratio > 1.50",             lambda r: r["edge_ratio"]    is not None and r["edge_ratio"] > 1.50),
        ("---", lambda r: False),
        ("Old delta-only flagged (>=15)", lambda r: r["naip_score"] >= 15 or r["texture_score"] >= 25),
        ("COMBINED >= 15 (mild)",         lambda r: r["combined_score"] >= 15),
        ("COMBINED >= 30 (likely pad)",   lambda r: r["combined_score"] >= 30),
        ("COMBINED >= 50 (strong pad)",   lambda r: r["combined_score"] >= 50),
    ]:
        a = count(by_stratum.get("A_disturbed", []), pred)
        b = count(by_stratum.get("B_low_recent", []), pred)
        c = count(by_stratum.get("C_random", []), pred)
        total = count(rows, pred)
        print(f"  {label:<35}  {a:>3}  {b:>3}  {c:>3}  {total:>4}")

    # Signal-pathway breakdown: of wells flagged at COMBINED >= 30, which
    # signals carried the load? This tells us if any pathway is dispensable.
    flagged = [r for r in rows if r["combined_score"] >= 30]
    if flagged:
        print(f"\n  Of {len(flagged)} wells with COMBINED >= 30, signals firing:")
        only_abs   = sum(1 for r in flagged if r["abs_signal"]   > 0 and r["delta_signal"] == 0 and r["edge_signal"]  == 0)
        only_delta = sum(1 for r in flagged if r["delta_signal"] > 0 and r["abs_signal"]  == 0 and r["edge_signal"]  == 0)
        only_edge  = sum(1 for r in flagged if r["edge_signal"]  > 0 and r["abs_signal"]  == 0 and r["delta_signal"] == 0)
        multi      = sum(1 for r in flagged if (r["abs_signal"] > 0) + (r["delta_signal"] > 0) + (r["edge_signal"] > 0) >= 2)
        print(f"    only absolute NDVI : {only_abs}")
        print(f"    only delta         : {only_delta}")
        print(f"    only edge          : {only_edge}")
        print(f"    multi-signal       : {multi}")

    # Agreement: do the two methods rank the same wells highly?
    print("\n  Agreement:")
    naip_top = sorted(rows, key=lambda r: -r["naip_score"])[:5]
    text_top = sorted(rows, key=lambda r: -r["texture_score"])[:5]
    naip_apis = {r["api_no"] for r in naip_top}
    text_apis = {r["api_no"] for r in text_top}
    overlap = naip_apis & text_apis
    print(f"    Top-5 by NAIP    : {sorted(naip_apis)}")
    print(f"    Top-5 by texture : {sorted(text_apis)}")
    print(f"    Overlap          : {sorted(overlap) or '(none)'}")

    # Write merged CSV
    fieldnames = list(rows[0].keys())
    out_path = f"probe_comparison_{county}.csv"
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"\n  Merged CSV -> {out_path}")


if __name__ == "__main__":
    main()
