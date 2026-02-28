import asyncio
from stockify.config import get_raw_data_path
from stockify.utils.logger import logger as logger_file
from stockify.ingest.writer import RawDataWriter
from stockify.ingest.fetcher import FetchMetaData
from stockify.ingest.fetcher import executor


import warnings
warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=FutureWarning)


async def api_ingestion_load(stock_symbol, job_run_date, func, period):

    as_dict_flag = True if func not in ["history", "get_news"]  else False             # history dataset will be handled as DataFrame, others as dict for easier JSON writing                           
    """ we can change the function argument to test different datasets (history, info, balancesheet) 
        without modifying the FetchMetaData class or the fetch_meta_data method. 
        This makes our testing more flexible and modular"""
    
    # 1.  Instance creation to get data from API from `fetcher.py` module
    meta_data_obj = FetchMetaData(symbol=stock_symbol, 
                                  retries=3, 
                                  period=period
                                  )
    
    result = await meta_data_obj \
                        .fetch_meta_data(func=func, 
                                         as_dict_flag=as_dict_flag
                                         )
    
    logger_file.debug("Data type for %s: %s", func, type(result))
    logger_file.info("Total length of `%s` data ingested for %s: %s", func, stock_symbol, len(result)) # type: ignore

    # 2. Write If not empty
    try:
        if result is None:
            logger_file.warning("%s -> failed or timed out", stock_symbol)
            return

        # Handle empty DataFrame
        if hasattr(result, "empty") and result.empty:
            logger_file.warning("Empty DataFrame for %s (%s)", stock_symbol, func)
            return

        # Handle empty dict or list
        if isinstance(result, (dict, list)) and not result:
            logger_file.warning("Empty result for %s (%s)", stock_symbol, func)
            return

        symbol_clean = stock_symbol.replace(".NS", "")

        raw_writer_obj = RawDataWriter(root_dir=get_raw_data_path(), 
                                            symbol=symbol_clean, 
                                            func=func, 
                                            data=result,                      # type: ignore
                                            batch_date=job_run_date
                                            )
        
        loop = asyncio.get_running_loop()

        # Offload blocking writer to threadpool
        # change the `None` inside `run_in_executor` to `executor` instance made above for optimization.
        await loop.run_in_executor(executor,
                                raw_writer_obj.write_data_to_raw_layer
                                )
    except Exception as e:
        logger_file.error("Error in ingest_main: %s", e, exc_info=True)