# run.py
import yaml
import logging
import os
import time
import argparse
import subprocess
import sys
from scheduler import run_engine
from utils import load_csv_dict, expand_config_list


def setup_logging(cfg):
    """Configure logging with file and console output."""
    log_level = getattr(logging, cfg.get("log_level", "INFO"))
    log_file = cfg.get("log_file", "engine.log")
    
    # Create logger
    logger = logging.getLogger("stochastic_engine")
    logger.setLevel(log_level)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # File handler
    try:
        fh = logging.FileHandler(log_file)
        fh.setLevel(log_level)
        fh.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        logger.addHandler(fh)
    except Exception as e:
        print(f"Warning: couldn't setup file logging: {e}")
    
    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(logging.Formatter(
        '%(levelname)s: %(message)s'
    ))
    logger.addHandler(ch)
    
    return logger


def main():
    start_time = time.time()
    
    with open("config.yaml") as f:
        cfg = yaml.safe_load(f)
    
    logger = setup_logging(cfg)
    logger.info("="*60)
    logger.info("Starting Stochastic Engine")
    logger.info("="*60)
    logger.info(f"Config: {cfg}")
    
    try:
        # Convert paths to absolute to work with spawned worker processes
        model_path = os.path.abspath(cfg["model_path"])
        assumptions_csv = os.path.abspath(cfg["assumptions_csv"])
        policies_csv = os.path.abspath(cfg["policies_csv"])
        output_dir = os.path.abspath(cfg["output_dir"])
        worker_models_dir = os.path.abspath(cfg.get("worker_models_dir", "worker_models"))
        
        logger.info(f"Model path: {model_path}")
        logger.info(f"Worker models dir: {worker_models_dir}")
        
        # Verify model file exists
        if not os.path.exists(model_path):
            raise FileNotFoundError(f"Model file not found: {model_path}")

        # Read provisioning options from config
        provision_cfg = cfg.get("provision", {}) or {}
        provision_enabled = bool(provision_cfg.get("enabled", False))
        provision_force = bool(provision_cfg.get("force", False))
        provision_clean = bool(provision_cfg.get("clean", False))
        provision_n_workers = int(provision_cfg.get("n_workers", cfg.get("n_workers")))

        # Optionally provision per-worker model copies before starting
        if provision_enabled:
            provision_script = os.path.join(os.path.dirname(__file__), "scripts", "provision_worker_models.py")
            cmd = [sys.executable, provision_script, "--model", model_path, "--out", worker_models_dir, "--n-workers", str(provision_n_workers)]
            if provision_force:
                cmd.append("--force")
            if provision_clean:
                cmd.append("--clean")
            logger.info(f"Provisioning worker models with: {' '.join(cmd)}")
            try:
                proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
                logger.info(proc.stdout)
                if proc.stderr:
                    logger.warning(proc.stderr)
            except Exception as e:
                logger.error(f"Provisioning failed: {e}")
                raise
        
        assumptions = load_csv_dict(assumptions_csv)
        logger.info(f"Loaded {len(assumptions)} assumption sets")
        
        policies = load_csv_dict(policies_csv)
        logger.info(f"Loaded {len(policies)} policies")
        
        # Expand scenarios and policies from config
        scenarios = expand_config_list(cfg["scenarios"], set(assumptions.keys()))
        logger.info(f"Scenarios to run: {scenarios}")
        
        policies_to_run = expand_config_list(cfg["policies"], set(policies.keys()))
        logger.info(f"Policies to run: {policies_to_run}")
        
        logger.info(f"Running {len(scenarios)} scenarios with {len(policies_to_run)} policies each")
        # Worksheet/range addresses from config
        worksheet_name = cfg.get("worksheet_name", "Inputs")
        rng_assump_addr = cfg.get("rng_assump", "I7:Z7")
        rng_policy_addr = cfg.get("rng_policy", "I3:T3")
        rng_out_addr = cfg.get("rng_out", "U11:V11")

        run_engine(
            model_path = model_path,
            assumptions_dict = assumptions,
            policies_dict = policies,
            scenarios = scenarios,
            policies = policies_to_run,
            n_workers = cfg["n_workers"],
            n_sims = cfg["n_sims"],
            output_dir = output_dir,
            worker_models_dir = worker_models_dir,
            worksheet_name = worksheet_name,
            rng_assump_addr = rng_assump_addr,
            rng_policy_addr = rng_policy_addr,
            rng_out_addr = rng_out_addr,
            queue_timeout = cfg.get("queue_timeout", 10.0),
            worker_timeout = cfg.get("worker_timeout", 300.0),
            max_retries = cfg.get("max_retries", 3),
            retry_delay = cfg.get("retry_delay", 1.0),
            retry_backoff = cfg.get("retry_backoff", 2.0),
            logger = logger
        )
        
        logger.info("Engine completed successfully")
        
    except KeyboardInterrupt:
        logger.warning("Received interrupt signal, shutting down gracefully...")
        raise
    except Exception as e:
        logger.error(f"Engine failed: {e}", exc_info=True)
        raise
    
    finally:
        elapsed = time.time() - start_time
        logger.info("="*60)
        logger.info(f"Total execution time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        logger.info("="*60)


if __name__ == "__main__":
    main()
