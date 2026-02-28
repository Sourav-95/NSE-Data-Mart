import csv
from stockify.config import get_ticker_list_path
from stockify.utils.logger import logger_terminal as logger

def load_ticker_list():
    try:
        ticker_list_path = get_ticker_list_path()
        with open(ticker_list_path, mode='r') as file:
            reader = csv.reader(file)
            tickers = [row[0] for row in reader if row]  # Assuming tickers are in the first column
            symbol = [item+'.NS' for item in tickers if 'Ticker' not in item] 
            # logger.info(f"Type of symbol: {type(symbol)}")
            # logger.info(f'Length of ticker: {len(symbol)}')
            # logger.info(symbol)
        return symbol
    except Exception as e:
        logger.error(f"{e}")
        return []