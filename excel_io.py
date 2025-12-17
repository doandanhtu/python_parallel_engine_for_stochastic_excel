# excel_io.py
import os
import csv
import logging

logger = logging.getLogger("stochastic_engine")

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_policy_csv(path, outputs):
    """
    Write policy outputs as CSV with columns: sim, PVFP, PVFPrem

    Each item in `outputs` is expected to be either:
      - a tuple/list with two values (PVFP, PVFPrem), or
      - a single scalar (treated as PVFP, PVFPrem=None)
    """
    ensure_dir(os.path.dirname(path))

    if not outputs:
        raise ValueError("No outputs to write")

    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["sim", "PVFP", "PVFPrem"])

        for i, row in enumerate(outputs, start=1):
            # Normalize row
            if isinstance(row, (tuple, list)):
                # Support nested single-row tuples where Excel returns ((x, y),)
                if len(row) == 1 and isinstance(row[0], (tuple, list)):
                    vals = list(row[0])
                else:
                    vals = list(row)
            else:
                vals = [row]

            # Extract PVFP and PVFPrem
            pvfp = vals[0] if len(vals) >= 1 else None
            pvfprem = vals[1] if len(vals) >= 2 else None

            writer.writerow([i, pvfp, pvfprem])
