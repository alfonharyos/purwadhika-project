import logging
import time
import os

def init_logger(log_path: str, name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent adding duplicate handlers
    if not logger.handlers:
        os.makedirs(log_path, exist_ok=True)
        run_date = time.strftime("%Y%m%d", time.gmtime())
        log_file = os.path.join(log_path, f"{run_date}_{name if name else 'app'}.log")
        file_handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s UTC - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger