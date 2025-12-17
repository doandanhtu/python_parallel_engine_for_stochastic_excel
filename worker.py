# worker.py
import traceback
import pythoncom
import win32com.client
import time
import logging
from excel_io import write_policy_csv

logger = logging.getLogger("stochastic_engine")


def worker_loop(worker_id, task_queue, result_queue,
                model_path, output_dir, n_sims,
                worksheet_name="Inputs", rng_assump_addr="I7:Z7",
                rng_policy_addr="I3:T3", rng_out_addr="U11:V11",
                max_retries=3, retry_delay=1.0, retry_backoff=2.0):

    logger.info(f"Worker {worker_id} initializing")
    worker_start = time.time()
    
    pythoncom.CoInitialize()
    excel = None
    wb = None
    
    try:
        excel = win32com.client.DispatchEx("Excel.Application")
        excel.Visible = False
        excel.DisplayAlerts = False
        excel.EnableEvents = False
        excel.ScreenUpdating = False
        logger.debug(f"Worker {worker_id} created Excel application")

        # Open workbook with retry
        max_open_retries = 3
        for attempt in range(1, max_open_retries + 1):
            try:
                wb = excel.Workbooks.Open(model_path)
                logger.debug(f"Worker {worker_id} opened workbook")
                break
            except Exception as e:
                if attempt < max_open_retries:
                    logger.warning(f"Worker {worker_id} failed to open workbook (attempt {attempt}): {e}")
                    time.sleep(1.0 * attempt)
                else:
                    raise
        
        # Set calculation mode with error handling
        try:
            excel.Calculation = -4135  # xlManual
            logger.debug(f"Worker {worker_id} set manual calculation mode")
        except Exception as e:
            logger.warning(f"Worker {worker_id}: couldn't set Excel Calculation mode: {e}")

        ws = wb.Worksheets(worksheet_name)
        rng_assump = ws.Range(rng_assump_addr)
        rng_policy = ws.Range(rng_policy_addr)
        rng_out    = ws.Range(rng_out_addr)
        
        init_elapsed = time.time() - worker_start
        logger.info(f"Worker {worker_id} ready to process jobs (init time: {init_elapsed:.2f}s)")

        current_scenario = None

        while True:
            msg = task_queue.get()

            if msg["type"] == "SHUTDOWN":
                logger.debug(f"Worker {worker_id} received shutdown signal")
                break

            if msg["type"] == "SET_SCENARIO":
                current_scenario = msg["scenario_id"]
                
                def set_scenario():
                    rng_assump.Value = msg["assumptions"]
                
                try:
                    # Retry setting scenario
                    for attempt in range(1, max_retries + 1):
                        try:
                            set_scenario()
                            logger.debug(f"Worker {worker_id} set scenario {current_scenario}")
                            break
                        except Exception as e:
                            if attempt < max_retries:
                                logger.warning(f"Worker {worker_id} failed to set scenario (attempt {attempt}): {e}")
                                time.sleep(retry_delay * (retry_backoff ** (attempt - 1)))
                            else:
                                raise
                    
                    result_queue.put({
                        "worker": worker_id,
                        "event": "SCENARIO_SET",
                        "scenario": current_scenario
                    })
                except Exception as e:
                    logger.error(f"Worker {worker_id} failed to set scenario: {e}")
                    raise

            elif msg["type"] == "RUN_POLICY":
                policy_id = msg["policy_id"]
                job_start = time.time()
                
                def set_policy():
                    rng_policy.Value = msg["policy_data"]
                
                try:
                    # Retry setting policy
                    for attempt in range(1, max_retries + 1):
                        try:
                            set_policy()
                            logger.debug(f"Worker {worker_id} set policy {policy_id}")
                            break
                        except Exception as e:
                            if attempt < max_retries:
                                logger.warning(f"Worker {worker_id} failed to set policy (attempt {attempt}): {e}")
                                time.sleep(retry_delay * (retry_backoff ** (attempt - 1)))
                            else:
                                raise
                    
                    outputs = []
                    calc_start = time.time()
                    for sim_num in range(n_sims):
                        # Retry logic for individual simulation
                        calc_success = False
                        for attempt in range(1, max_retries + 1):
                            try:
                                excel.Calculate()
                                output_value = rng_out.Value
                                logger.debug(f"Worker {worker_id} sim {sim_num} output type: {type(output_value)}, value: {output_value}")
                                outputs.append(output_value)
                                calc_success = True
                                break
                            except Exception as e:
                                if attempt < max_retries:
                                    logger.warning(f"Worker {worker_id} sim {sim_num} failed (attempt {attempt}/{max_retries}): {e}")
                                    time.sleep(retry_delay * (retry_backoff ** (attempt - 1)))
                                else:
                                    logger.error(f"Worker {worker_id} sim {sim_num} failed after {max_retries} attempts: {e}")
                        
                        if not calc_success:
                            raise RuntimeError(f"Simulation {sim_num} failed after {max_retries} retry attempts")
                    
                    calc_elapsed = time.time() - calc_start

                    out_file = (
                        f"{output_dir}/scenario_{current_scenario}/"
                        f"policy_{policy_id}.csv"
                    )
                    write_start = time.time()
                    write_policy_csv(out_file, outputs)
                    write_elapsed = time.time() - write_start
                    
                    logger.debug(f"Worker {worker_id} saved policy {policy_id} results")

                    job_elapsed = time.time() - job_start
                    logger.debug(f"Worker {worker_id} job stats - "
                               f"policy set: init, calculations: {calc_elapsed:.2f}s, "
                               f"file write: {write_elapsed:.2f}s, total: {job_elapsed:.2f}s")

                    result_queue.put({
                        "worker": worker_id,
                        "event": "POLICY_DONE",
                        "scenario": current_scenario,
                        "policy": policy_id
                    })
                except Exception as e:
                    logger.error(f"Worker {worker_id} failed running policy {policy_id}: {e}")
                    raise

    except Exception as e:
        traceback.print_exc()
        logger.error(f"Worker {worker_id} encountered fatal error: {e}", exc_info=True)
        try:
            result_queue.put({
                "worker": worker_id,
                "event": "ERROR",
                "error": str(e)
            })
        except Exception as q_err:
            logger.error(f"Worker {worker_id} couldn't put error in queue: {q_err}")

    finally:
        if wb is not None:
            try:
                wb.Close(SaveChanges=False)
            except Exception as e:
                logger.warning(f"Worker {worker_id} error closing workbook: {e}")
        
        if excel is not None:
            try:
                excel.Quit()
            except Exception as e:
                logger.warning(f"Worker {worker_id} error quitting Excel: {e}")
        
        try:
            pythoncom.CoUninitialize()
        except Exception as e:
            logger.warning(f"Worker {worker_id} error in CoUninitialize: {e}")
        
        logger.info(f"Worker {worker_id} shut down")
