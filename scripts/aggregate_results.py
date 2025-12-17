#!/usr/bin/env python3
"""Aggregate simulation CSV outputs into a summary CSV.

Outputs expected: folder structure under OUTPUT_DIR like:
  OUTPUT_DIR/scenario_{scen}/policy_{pol}.csv

Each policy CSV must have header: sim,PVFP,PVFPrem

Produces a summary CSV with columns:
  Scenario,Policy,ProbRuin,AvgPVFP,AvgPVFPrem,PM_Avg,MedianPVFP,MedianPVFPrem,PM_Median
"""
import argparse
import csv
import os
from statistics import mean, median


def summarize_policy(path):
    pvfps = []
    pvfp_rems = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                pvfp = float(row.get("PVFP") or row.get("out1") or 0.0)
            except Exception:
                pvfp = float(row.get("PVFP") or row.get("out1") or 0.0)
            try:
                pvfprem = float(row.get("PVFPrem") or row.get("out2") or 0.0)
            except Exception:
                pvfprem = float(row.get("PVFPrem") or row.get("out2") or 0.0)
            pvfps.append(pvfp)
            pvfp_rems.append(pvfprem)

    if not pvfps:
        return None

    n = len(pvfps)
    prob_ruin = sum(1 for v in pvfps if v < 0) / n
    avg_pvfp = mean(pvfps)
    avg_pvfprem = mean(pvfp_rems) if pvfp_rems else 0.0
    pm_avg = (avg_pvfp / avg_pvfprem) if avg_pvfprem != 0 else None
    med_pvfp = median(pvfps)
    med_pvfprem = median(pvfp_rems) if pvfp_rems else 0.0
    pm_med = (med_pvfp / med_pvfprem) if med_pvfprem != 0 else None

    return {
        "ProbRuin": prob_ruin,
        "AvgPVFP": avg_pvfp,
        "AvgPVFPrem": avg_pvfprem,
        "PM_Avg": pm_avg,
        "MedianPVFP": med_pvfp,
        "MedianPVFPrem": med_pvfprem,
        "PM_Median": pm_med,
        "N": n
    }


def main():
    p = argparse.ArgumentParser(description="Aggregate policy CSV results")
    p.add_argument("--output-dir", required=True, help="Output directory where scenario folders live")
    p.add_argument("--out-file", default=None, help="Summary CSV filename (relative to output-dir). Defaults to <output-subdir>_summary.csv")
    args = p.parse_args()

    out_dir = args.output_dir
    if args.out_file:
        summary_name = args.out_file
    else:
        base = os.path.basename(os.path.normpath(out_dir))
        summary_name = f"{base}_summary.csv"
    summary_path = os.path.join(out_dir, summary_name)

    rows = []
    for scen_name in sorted(os.listdir(out_dir)):
        scen_path = os.path.join(out_dir, scen_name)
        if not os.path.isdir(scen_path):
            continue
        # Expect scen_name like scenario_1
        scen_id = scen_name.replace("scenario_", "")
        for fname in sorted(os.listdir(scen_path)):
            if not fname.startswith("policy_") or not fname.endswith(".csv"):
                continue
            pol_id = fname.replace("policy_", "").replace(".csv", "")
            fpath = os.path.join(scen_path, fname)
            stats = summarize_policy(fpath)
            if stats is None:
                continue
            rows.append({
                "Scenario": scen_id,
                "Policy": pol_id,
                **stats
            })

    # Write summary CSV
    if rows:
        fieldnames = ["Scenario", "Policy", "N", "ProbRuin", "AvgPVFP", "AvgPVFPrem", "PM_Avg", "MedianPVFP", "MedianPVFPrem", "PM_Median"]
        with open(summary_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        print(f"Wrote summary to {summary_path}")
    else:
        print("No results found to summarize")


if __name__ == "__main__":
    main()
