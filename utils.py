import logging
import os


def setup_logger() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        path = os.path.dirname(os.path.abspath(__file__))
        file_handler = logging.FileHandler(os.path.join(path, 'debug.log'))
        file_handler.setLevel(logging.DEBUG)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


def get_database_path() -> str:
    current_folder = os.path.dirname(os.path.abspath(__file__))
    database_path = os.path.join(current_folder, "blockchain.db")
    return database_path
