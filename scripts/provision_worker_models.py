#!/usr/bin/env python3
"""Provision per-worker copies of an Excel model.

Usage:
  python scripts/provision_worker_models.py --model models/my.xlsb --out worker_models --n-workers 6 [--force]

This script is idempotent: it will skip copying if destination exists unless --force is set.
"""
import argparse
import os
import shutil
import sys
import hashlib


def sha256(path, chunk_size=8192):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def provision(model, out_dir, n_workers, force=False, clean=False):
    # If clean is requested, remove out_dir entirely first (clean slate)
    if clean and os.path.exists(out_dir):
        try:
            shutil.rmtree(out_dir)
        except Exception as e:
            print(f"Failed to remove directory {out_dir}: {e}")
            return 1
    os.makedirs(out_dir, exist_ok=True)
    ext = os.path.splitext(model)[1]
    results = []
    for wid in range(1, n_workers + 1):
        dest = os.path.join(out_dir, f"model_worker_{wid}{ext}")
        if os.path.exists(dest) and not force:
            results.append((wid, dest, False))
            continue
        try:
            shutil.copy2(model, dest)
            results.append((wid, dest, True))
        except Exception as e:
            print(f"Failed to copy for worker {wid}: {e}")
            return 1

    # Print summary with checksums
    print("Provisioning complete")
    for wid, dest, copied in results:
        status = "created" if copied else "exists"
        try:
            h = sha256(dest)
        except Exception:
            h = "?"
        print(f"Worker {wid}: {dest} ({status}) sha256={h}")

    return 0


def main():
    p = argparse.ArgumentParser(description="Provision per-worker Excel model copies")
    p.add_argument("--model", required=True, help="Path to source model file")
    p.add_argument("--out", default="worker_models", help="Output directory for copies")
    p.add_argument("--n-workers", type=int, required=True, help="Number of worker copies to create")
    p.add_argument("--force", action="store_true", help="Recreate copies even if present")
    p.add_argument("--clean", action="store_true", help="Remove out directory first (clean slate)")
    args = p.parse_args()

    rc = provision(args.model, args.out, args.n_workers, force=args.force, clean=args.clean)
    sys.exit(rc)


if __name__ == "__main__":
    main()
