Stochastic Excel-Python Parallel Engine v1.0
======================================

Quick Start
-----------
1. Edit `config.yaml` to point to your model and CSVs, adjust the worksheet and range names if your model layout differs (`worksheet_name`, `rng_assump`, `rng_policy`, `rng_out`), choose scenarios/policies, and configure provisioning settings.

2. Run the engine:

```bash
python run.py
```

Provisioning is controlled entirely via `config.yaml`. Set `provision.enabled: true` to auto-provision worker models before the run starts.

Overview
--------
This project runs parallel simulations by controlling Excel workbooks via COM (pywin32). It provisions per-worker Excel model copies for isolation, runs scenarios and policies, collects outputs to CSV, and can aggregate results.

Repository layout
-----------------
- `run.py` - Main entry. Starts the scheduler and workers.
- `scheduler.py` - Job scheduler that manages worker processes.
- `worker.py` - Worker process that opens an Excel workbook and runs simulations.
- `excel_io.py` - Functions to write policy CSV outputs.
- `utils.py` - Helpers for reading CSV and expanding config selections.
- `scripts/provision_worker_models.py` - Idempotent script to create per-worker model copies.
- `scripts/aggregate_results.py` - Aggregates per-policy CSV outputs into a summary CSV.
- `config.yaml` - Main configuration file.
- `models/` - Folder for Excel workbooks.
- `outputs/` - Runtime output folder (created by runs).
- `worker_models/` - Per-worker model copies (created by provisioning, persistent across runs).
 
Architecture
------------
This repository follows a simple separation of concerns:

- `models/` — store master Excel workbooks (read-only during runs). The master workbook is used only as the source when creating per-worker copies.
- `worker_models/` — persistent per-worker copies created by provisioning. Workers open those copies during runs so the master workbook remains untouched.
- `parameters/` — scenario assumptions CSV files. Each file should have the scenario identifier in the first column and the remaining columns must match the order of the Excel `rng_assump` range defined in `config.yaml`. Typical format:

  ScenarioId,Assump1,Assump2,Assump3
  1,0.02,0.01,1000
  2,0.03,0.015,950

  The code reads `assumptions_csv` as a mapping from ScenarioId → list of values and writes them to the configured worksheet range. Keep the CSV column order in sync with the model.

- `policies/` — policy definition CSVs. Each policy file uses the first column as the policy identifier and the remaining columns correspond (in order) to the Excel `rng_policy` range. Example:

  PolicyId,ParamA,ParamB,ParamC
  101,1,0.05,200
  102,2,0.10,150

  The worker code writes each policy row into the policy range before executing simulations.

- `outputs/` — per-run result folders. Each run should use a new `output_dir` value in `config.yaml` to keep results isolated.

Configuration (`config.yaml`)
-----------------------------
Key fields:
- `model_path`: path to Excel workbook
- `assumptions_csv`: CSV with scenario assumptions (first column = scenario id)
- `policies_csv`: CSV with policy definitions (first column = policy id)
- `scenarios`: can be `"all"`, a list of IDs, or range strings like `"5:10"`
- `policies`: same flexible format as `scenarios`
- `n_workers`: number of worker processes
- `n_sims`: number of simulations per policy
- `output_dir`: directory where results are written (new folder per run)
- `worker_models_dir`: directory where per-worker model copies are stored (persistent, at project root level)
- Logging/timeouts/retry settings: `log_level`, `queue_timeout`, `worker_timeout`, `max_retries`, `retry_delay`, `retry_backoff`

Provisioning options (add to `config.yaml` under `provision`):

- `provision.enabled`: `true|false` — run provisioning automatically before engine starts
- `provision.force`: `true|false` — overwrite existing files in `worker_models_dir` during provisioning
- `provision.clean`: `true|false` — remove all worker models before copying (clean slate)
- `provision.n_workers`: optional override for number of copies to create; defaults to top-level `n_workers`

Example:
```yaml
worker_models_dir: worker_models

provision:
  enabled: true
  force: false
  clean: false
  # n_workers: 20
```

CSV output format
-----------------
Each policy run writes CSV to:
`{output_dir}/scenario_{scen}/policy_{pol}.csv`
with header: `sim,PVFP,PVFPrem`.

Aggregation
-----------
To summarize results after a run:

```bash
python scripts/aggregate_results.py --output-dir outputs/test_3
```

By default, it writes `summary.csv` to that directory. To customize the output filename:

```bash
python scripts/aggregate_results.py --output-dir outputs/test_3 --out-file my_summary.csv
```

The aggregator produces CSV with columns:
`Scenario,Policy,N,ProbRuin,AvgPVFP,AvgPVFPrem,PM_Avg,MedianPVFP,MedianPVFPrem,PM_Median`.

Notes & Recommendations
-----------------------
- **Provisioning strategy**: Set `provision.enabled: true` for automatic pre-provisioning. Worker models are stored in `worker_models_dir` (at project root), persistent across runs.
- **Multiple runs without re-provisioning**: Create new `output_dir` values in config for each run. Set `provision.enabled: false` if models already exist and are current.
- **Fresh models**: Set `provision.enabled: true` and `provision.clean: true` to remove and recreate all worker models.
- **Overwrite existing models**: Set `provision.force: true` to overwrite without removing directory.
- **Isolation**: Per-worker copies isolate Excel instances and reduce cross-process interference, improving reliability.
- **Tune worker_timeout** based on `n_sims` (rough estimate: 1-2 seconds per simulation).
- **If Excel COM errors persist**, try increasing `max_retries` and `retry_delay`.