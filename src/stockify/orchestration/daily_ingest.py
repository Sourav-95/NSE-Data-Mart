import asyncio
import time
from datetime import date
import psutil
import os
from stockify.utils.logger import logger as logger_file
from stockify.utils.logger import logger_terminal as log_terminal
from src.stockify.ingest.ingest_main import api_ingestion_load
from src.stockify.utils.tickers import load_ticker_list
from src.stockify.config import load_config

""" This job runs daily in batch to update the following """

async def trigger_daily_ingestion(func):

    symbols = load_ticker_list()
    symbols = symbols[0:400]  # For testing, limit to first 400 symbols. Remove this line for full run.

    JOB_RUN_DATE = date.today()
    CHUNK_SIZE = load_config().get("ingestion", {}).get("batch_size", 50)
    COOLDOWN_SEC = load_config().get("ingestion", {}).get("cool_down", 60)

    for i in range(0, len(symbols), CHUNK_SIZE):
        chunk = symbols[i : i + CHUNK_SIZE]
        log_terminal.info(
            f"Processing chunk {i//CHUNK_SIZE + 1}"
            f"(out of {round(len(symbols)/len(chunk))})")
        
        tasks = []
            
        for stock in chunk:
            tasks.append(
                api_ingestion_load(
                    stock_symbol=stock,
                    job_run_date=JOB_RUN_DATE,
                    func=func,
                    period=None
                )
            )
    
        # Run all task concurrently
        await asyncio.gather(*tasks, return_exceptions=True)

        # Do not sleep after last chunk
        if i + CHUNK_SIZE < len(symbols):
            log_terminal.info(
                f"Chunk complete. Cooling down for {COOLDOWN_SEC}s..."
            )
            await asyncio.sleep(COOLDOWN_SEC)
            
        process = psutil.Process(os.getpid())
        process.cpu_percent(interval=None)   
        cpu_usage = process.cpu_percent(interval=1)             # CPU usage over short interval
        memory_info = process.memory_info()                     # Memory usage 
        memory_mb = memory_info.rss / (1024 * 1024)
        log_terminal.info("Memory usage: %.2f MB", memory_mb)    

    log_terminal.info("All chunks completed.")


if __name__ == "__main__":

    log_terminal.info("Daily Market Ingestion Started....")
    start_time = time.perf_counter()

    methods = ['get_news', 'get_actions', 'earnings_dates', 'calendar' ]
    COOLDOWN_BETWEEN_METHODS = 600  # 10 minutes (600 seconds)
    async def run_all_methods():
        for method in methods:
            log_terminal.info(f"Starting method: `{method}`")
            try:
                await trigger_daily_ingestion(func=method)
                log_terminal.info(f"`{method}` completed successfully.")

                # Wait only if not last method
                if method != methods[-1]:
                    log_terminal.info(
                        f"Going to next METHOD. Cooling down for {COOLDOWN_BETWEEN_METHODS//60} minutes..."
                    )
                    await asyncio.sleep(COOLDOWN_BETWEEN_METHODS)

            except Exception as e:
                logger_file.error(f"{method} failed. Skipping cooldown.", exc_info=True)

    asyncio.run(run_all_methods())

    end_time = time.perf_counter()
    total_time = end_time - start_time

    logger_file.info("Total runtime: %.2f seconds", total_time)
    log_terminal.info("Total runtime: %.2f seconds", total_time)
    log_terminal.info("Daily Market Ingestion Completed....")