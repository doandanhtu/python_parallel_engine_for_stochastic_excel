# scheduler.py
import multiprocessing as mp
import logging
import time
import signal
import sys
from collections import deque
from worker import worker_loop
import os
import shutil

MSG_SET_SCENARIO = "SET_SCENARIO"
MSG_RUN_POLICY   = "RUN_POLICY"
MSG_SHUTDOWN     = "SHUTDOWN"

logger = logging.getLogger("stochastic_engine")

shutdown_signal = False


def run_engine(
    model_path,
    assumptions_dict,
    policies_dict,
    scenarios,
    policies,
    n_workers,
    n_sims,
    output_dir,
    worker_models_dir,
    worksheet_name="Inputs",
    rng_assump_addr="I7:Z7",
    rng_policy_addr="I3:T3",
    rng_out_addr="U11:V11",
    queue_timeout=10.0,
    worker_timeout=300.0,
    max_retries=3,
    retry_delay=1.0,
    retry_backoff=2.0,
    logger=None
):

    global shutdown_signal
    shutdown_signal = False
    
    def signal_handler(signum, frame):
        global shutdown_signal
        shutdown_signal = True
        logger.warning("Shutdown signal received, cleaning up workers...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    mp.set_start_method("spawn", force=True)

    task_queue   = mp.Queue()
    result_queue = mp.Queue()
    
    # Start overall timer
    engine_start = time.time()

    # ---------------- start workers ----------------
    logger.info(f"Starting {n_workers} worker processes")
    workers = {}

    # Look for pre-provisioned worker models in worker_models_dir
    os.makedirs(worker_models_dir, exist_ok=True)
    model_ext = os.path.splitext(model_path)[1]
    worker_model_paths = {}
    all_present = True
    for wid in range(1, n_workers + 1):
        expected = os.path.join(worker_models_dir, f"model_worker_{wid}{model_ext}")
        if not os.path.exists(expected):
            all_present = False
            break

    if not all_present:
        logger.info(f"Pre-provisioned worker models not complete; creating missing copies in {worker_models_dir}")
        for wid in range(1, n_workers + 1):
            dest = os.path.join(worker_models_dir, f"model_worker_{wid}{model_ext}")
            if not os.path.exists(dest):
                try:
                    shutil.copy2(model_path, dest)
                    logger.debug(f"Copied model to {dest} for worker {wid}")
                except Exception as e:
                    logger.error(f"Failed to copy model for worker {wid}: {e}. Aborting to avoid using shared master model.")
                    raise RuntimeError(f"Failed to create worker model copy for worker {wid}: {e}")
            worker_model_paths[wid] = dest
    else:
        logger.info(f"Using existing pre-provisioned worker models in {worker_models_dir}")
        for wid in range(1, n_workers + 1):
            worker_model_paths[wid] = os.path.join(worker_models_dir, f"model_worker_{wid}{model_ext}")

    for wid in range(1, n_workers + 1):
        p = mp.Process(
            target=worker_loop,
            args=(
                wid,
                task_queue,
                result_queue,
                worker_model_paths.get(wid, model_path),
                output_dir,
                n_sims,
                worksheet_name,
                rng_assump_addr,
                rng_policy_addr,
                rng_out_addr,
                max_retries,
                retry_delay,
                retry_backoff
            )
        )
        p.start()
        workers[wid] = p
        logger.debug(f"Worker {wid} started (model: {worker_model_paths.get(wid, model_path)})")

    # ---------------- build job queue ----------------
    jobs = deque()
    for scen in scenarios:
        for pol in policies:
            jobs.append((scen, pol))
    
    total_jobs = len(jobs)
    logger.info(f"Created {total_jobs} jobs ({len(scenarios)} scenarios × {len(policies)} policies)")

    # ---------------- worker state ----------------
    worker_busy = {wid: False for wid in workers}
    worker_scenario = {wid: None for wid in workers}
    worker_last_activity = {wid: time.time() for wid in workers}

    active_jobs = 0
    completed_jobs = 0
    job_start_times = {}  # Track when each job started

    # ---------------- main scheduler loop ----------------
    while (jobs or active_jobs > 0) and not shutdown_signal:

        # ---------- dispatch ----------
        for wid in workers:
            if not shutdown_signal and not worker_busy[wid] and jobs:
                scen, pol = jobs.popleft()

                # change scenario only if needed
                if worker_scenario[wid] != scen:
                    task_queue.put({
                        "type": MSG_SET_SCENARIO,
                        "scenario_id": scen,
                        "assumptions": assumptions_dict[scen]
                    })
                    worker_scenario[wid] = scen
                    logger.debug(f"Worker {wid} set to scenario {scen}")

                task_queue.put({
                    "type": MSG_RUN_POLICY,
                    "scenario_id": scen,
                    "policy_id": pol,
                    "policy_data": policies_dict[pol]
                })

                worker_busy[wid] = True
                worker_last_activity[wid] = time.time()
                active_jobs += 1
                job_key = f"scen_{scen}_pol_{pol}"
                job_start_times[job_key] = time.time()
                logger.debug(f"Worker {wid} assigned scenario {scen}, policy {pol}")

        # ---------- collect with timeout ----------
        try:
            msg = result_queue.get(timeout=queue_timeout)
            
            if msg["event"] == "POLICY_DONE":
                wid = msg["worker"]
                worker_busy[wid] = False
                worker_last_activity[wid] = time.time()
                active_jobs -= 1
                completed_jobs += 1
                job_key = f"scen_{msg['scenario']}_pol_{msg['policy']}"
                job_elapsed = time.time() - job_start_times.get(job_key, time.time())
                logger.info(f"Completed {completed_jobs}/{total_jobs}: "
                           f"Scenario {msg['scenario']}, Policy {msg['policy']} ({job_elapsed:.2f}s)")

            elif msg["event"] == "ERROR":
                wid = msg["worker"]
                logger.error(f"Worker {wid} failed: {msg['error']}")
                raise RuntimeError(f"Worker {wid} failed: {msg['error']}")
            
            elif msg["event"] == "SCENARIO_SET":
                logger.debug(f"Worker {msg['worker']} set scenario {msg['scenario']}")
        
        except mp.queues.Empty:
            # Check for stuck workers
            current_time = time.time()
            for wid in workers:
                if worker_busy[wid]:
                    elapsed = current_time - worker_last_activity[wid]
                    # Warn if approaching timeout (at 70% of timeout)
                    if elapsed > worker_timeout * 0.7:
                        logger.warning(f"Worker {wid} approaching timeout: {elapsed:.0f}s / {worker_timeout:.0f}s")
                    if elapsed > worker_timeout:
                        logger.error(f"Worker {wid} timeout after {elapsed:.0f}s, terminating")
                        workers[wid].terminate()
                        raise RuntimeError(f"Worker {wid} stuck (no response for {elapsed:.0f}s)")
            
            # No results available but workers may still be working
            if active_jobs > 0:
                logger.debug(f"Waiting for results... ({active_jobs} jobs active, {len(jobs)} pending)")
                continue
        
        except KeyboardInterrupt:
            shutdown_signal = True
            logger.warning("Scheduler interrupted")

    # ---------- graceful shutdown ----------
    if shutdown_signal:
        logger.warning("Initiating graceful shutdown")
    
    logger.info(f"Sending shutdown signals to {len(workers)} workers")
    for _ in workers:
        task_queue.put({"type": MSG_SHUTDOWN})

    # Wait for workers with timeout
    for wid, p in workers.items():
        p.join(timeout=10.0)
        if p.is_alive():
            logger.warning(f"Worker {wid} did not exit, terminating")
            p.terminate()
            p.join(timeout=2.0)
    
    # Report final timing
    engine_elapsed = time.time() - engine_start
    avg_job_time = engine_elapsed / total_jobs if total_jobs > 0 else 0
    logger.info(f"Engine shutdown complete. Completed {completed_jobs}/{total_jobs} jobs")
    logger.info(f"Total engine time: {engine_elapsed:.2f}s ({engine_elapsed/60:.2f}m)")
    logger.info(f"Average time per job: {avg_job_time:.2f}s")
