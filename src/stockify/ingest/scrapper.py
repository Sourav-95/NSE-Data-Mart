import io, sys
import functools
from contextlib import redirect_stdout, redirect_stderr
from stockify.utils.logger import logger as logger_file

def silence_output(func):
    """
    Suppresses stdout and stderr during execution of the wrapped function.
    Useful for noisy libraries like yfinance.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        stdout_buffer = io.StringIO()
        stderr_buffer = io.StringIO()

        with redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            result = func(*args, **kwargs)

        stdout_val = stdout_buffer.getvalue()
        stderr_val = stderr_buffer.getvalue()

        if stdout_val:
            logger_file.debug("STDOUT suppressed:\n%s", stdout_val)

        if stderr_val:
            logger_file.debug("STDERR suppressed:\n%s", stderr_val)

        return result

    return wrapper


@silence_output
def api_trigger(ticker_obj, attr: str, **kwargs):
    try:

        obj = getattr(ticker_obj, attr)
        return obj(**kwargs) if callable(obj) else obj

    except Exception:
        logger_file.error("Scraping failed for %s when calling %s", attr, exc_info=True)
        raise

