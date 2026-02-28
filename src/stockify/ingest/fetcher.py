import asyncio
import random
from functools import partial
from typing import Any, Optional
import pandas as pd
import yfinance as yf
from yfinance.exceptions import YFRateLimitError
from concurrent.futures import ThreadPoolExecutor
from stockify.ingest.scrapper import api_trigger
from stockify.utils.logger import logger as logger_file
from stockify.config import load_config

# CONFIGURATION
_config = load_config()
_MAX_CONCURRENCY = _config.get("ingestion", {}).get("max_concurrency", 5)
_DEFAULT_TIMEOUT = _config.get("ingestion", {}).get("request_timeout_seconds", 30)
_RATE_LIMIT_COOLDOWN = 120

executor = ThreadPoolExecutor(max_workers=_MAX_CONCURRENCY)
semaphore = asyncio.Semaphore(_MAX_CONCURRENCY)


class FetchMetaData:

    def __init__(self, symbol: str, retries: int = 3, period: Optional[str] = None):
        self.symbol = symbol
        self.retries = retries
        self.period = period

    # Variant Generator
    def _generate_symbol_variants(self) -> list[str]:
        base = self.symbol.split(".")[0]

        if self.symbol.endswith(".NS"):
            return [f"{base}.NS", f"{base}.BO"]
        elif self.symbol.endswith(".BO"):
            return [f"{base}.BO", f"{base}.NS"]
        else:
            return [self.symbol]

    # Result Validator 
    def _is_valid_result(self, result: Any) -> bool:

        if result is None:
            return False

        if isinstance(result, pd.DataFrame):
            # empty DataFrame should be treated as failure
            if result.empty:
                return False
            return True

        if isinstance(result, dict):
            # empty dict should be treated as failure
            if not result:
                return False
            return True

        return True

    # Fetch Logic
    async def fetch_meta_data(self,func: str,as_dict_flag: Optional[bool] = None,
                              timeout: Optional[float] = None,) -> Optional[Any]:

        timeout = timeout or _DEFAULT_TIMEOUT
        loop = asyncio.get_running_loop()
        variants = self._generate_symbol_variants()

        for variant in variants:
            ticker = yf.Ticker(variant)
            for attempt in range(1, self.retries + 1):
                try:
                    async with semaphore:
                        # Build task
                        if func == "history":
                            task = loop.run_in_executor(
                                executor,
                                partial(
                                    api_trigger,ticker,func,period=self.period)
                            )

                        elif func in ["get_news", "get_actions", "earnings_dates"]:
                            task = loop.run_in_executor(
                                executor,
                                partial(api_trigger, ticker, func)
                            )

                        else:
                            task = loop.run_in_executor(
                                executor,
                                partial(
                                    api_trigger,
                                    ticker,
                                    func,
                                    as_dict=as_dict_flag
                                )
                            )

                        result = await asyncio.wait_for(task, timeout=timeout)

                        # Validate result
                        if self._is_valid_result(result):
                            return result

                        # Empty result → stop retrying this variant
                        logger_file.warning(
                            "Empty or invalid result for %s (%s). Trying next variant.",
                            variant,
                            func
                        )
                        break

                except asyncio.TimeoutError:
                    logger_file.warning("Timeout fetching %s for %s (attempt %d/%d)", func,variant,attempt,self.retries)

                except Exception as e:
                    error_str = str(e)

                    # Rate limit handling
                    if isinstance(e, YFRateLimitError) or "Too Many Requests" in error_str:
                        logger_file.error("Rate limit hit. Cooling down %ds...",_RATE_LIMIT_COOLDOWN)
                        await asyncio.sleep(_RATE_LIMIT_COOLDOWN)
                        continue

                    # Delisted or no data handling
                    if "delisted" in error_str.lower() or "no data found" in error_str.lower():
                        logger_file.warning("Variant %s appears inactive/delisted. Trying next variant.",variant)
                        break  # Stop retrying this variant

                    logger_file.warning("Retry %d for %s of %s",attempt,func,variant,exc_info=True)

                if attempt < self.retries:
                    backoff = min(30, (2 ** attempt)) + random.uniform(0, 1)
                    await asyncio.sleep(backoff)

            logger_file.warning("Variant failed completely: %s", variant)

        logger_file.error(
            "All symbol variants failed for %s (%s)",
            self.symbol,
            func
        )

        return None