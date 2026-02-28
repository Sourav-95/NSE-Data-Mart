# src/stockify/ingest/writer.py
from typing import Union
import json
from pathlib import Path
from typing import Any, Optional
import pandas as pd
from stockify.utils.logger import logger as logger_file

class RawDataWriter:
    """
    Writer class to handle all write operations for raw data.
    This can be extended in the future to support different formats 
    (Parquet, databases, etc.) or additional datasets.
    """
    def __init__(self, root_dir: Path, symbol: str, func: str,
                 data: Union[dict, pd.DataFrame], batch_date: str):               
        self.root_dir = root_dir
        self.symbol = symbol
        self.func = func
        self.data = data
        self.batch_date = batch_date
        self._validate_root_dir(self.root_dir)                  # change this if path changes
        # self.data_copy = pd.DataFrame()

    @staticmethod
    def _validate_root_dir(root_dir: Path) -> None:
        if root_dir.exists() and not root_dir.is_dir():
            raise ValueError(f"Root path exists but is not a directory: {root_dir}")

    @staticmethod
    def build_dataset_dir(root_dir: Path, func: str, batch_date: str) -> Path:
        """
        Build dataset path like:
        root_dir/yfinance/<dataset>/as_of_date=YYYY-MM-DD
        """
        path = root_dir/"yf"/func/f"{batch_date}"
        path.mkdir(parents=True, exist_ok=True)
        return path
        

    def add_feature_labels(self) -> pd.DataFrame:
        """
        Add feature labels to the data if needed. 
        This is a placeholder for any transformations or metadata additions before writing.
        For example, we could add a timestamp, source information, or any other relevant metadata.
        """
        if isinstance(self.data, pd.DataFrame):
            if self.func == "history":
                self.data['Date'] = self.data.index                               
                self.data.reset_index(drop=True, inplace=True)                    
            else:
                self.data[f"{self.func}_features"] = self.data.index                   
                self.data.reset_index(drop=True, inplace=True)
            logger_file.debug("Added feature labels to DataFrame for %s (%s)", self.symbol, self.func)                  
        else:
            logger_file.warning("Data is not a DataFrame, skipping feature label addition")

        return self.data  # type: ignore

    def write_data_to_raw_layer(self) -> Optional[Path]:
        if isinstance(self.data, list):
            if len(self.data) == 0:
                logger_file.warning("Empty `List` for %s, skipping write", self.symbol)
                return None
            else:
                out_dir = self.build_dataset_dir(self.root_dir, self.func, self.batch_date)             # Ensure correct dataset directory based on function (e.g., "info", "history")
                out_file = out_dir/f"{self.symbol}.json"                                                # Output file named after the symbol, e.g., "AAPL.json" 
                if out_file.exists():
                    logger_file.info("Overwriting existing file: %s", out_file)
                    return None
                
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(self.data, f, indent=2, default=str)
                logger_file.info(f"Loaded `{self.func}` for {self.symbol} -> {out_file}")
                return out_file
                
                
        elif isinstance(self.data, dict):
            if not self.data:
                logger_file.warning("Empty `dict` for %s, skipping write", self.symbol)
                return None
            else:
                out_dir = self.build_dataset_dir(self.root_dir, self.func, self.batch_date)             # Ensure correct dataset directory based on function (e.g., "info", "history")
                out_file = out_dir/f"{self.symbol}.json"                                                # Output file named after the symbol, e.g., "AAPL.json" 
                if out_file.exists():
                    logger_file.info("Overwriting existing file: %s", out_file)
                    return None

                # Convert Timestamp keys to strings for JSON serialization
                json_serializable_data = {str(k): v for k, v in self.data.items()}
                
                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(json_serializable_data, f, indent=2, default=str)
                logger_file.info(f"Loaded `{self.func}` for {self.symbol} -> {out_file}")
                return out_file

        elif isinstance(self.data, pd.DataFrame):
            if self.data.empty:
                logger_file.warning("Empty dataframe for %s (%s), skipping write", self.symbol, self.func)
                return None
            else:
                out_dir = self.build_dataset_dir(self.root_dir, self.func, self.batch_date)             # Ensure correct dataset directory based on function (e.g., "info", "history")
                out_file = out_dir / f"{self.symbol}.csv"                                               # Output file named after the symbol, e.g., "AAPL.csv"
                if out_file.exists():
                    logger_file.info("File already exists, skipping write: %s", out_file)
                    return None
                else:
                    self.df_copy = self.add_feature_labels()                                            # Add feature labels before writing
                    self.data.to_csv(out_file, index=False)
                    logger_file.info(f"Wrote `{self.func}` for {self.symbol} -> {out_file}")
                    return out_file
        else:
            logger_file.error("Unsupported data type for writing: %s", type(self.data))
            return None