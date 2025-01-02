import hashlib

import json
import time
from typing import Optional
from transaction import Transaction
from utils import setup_logger

logger = setup_logger()


class Block:
    def __init__(self, index: int, previous_hash: str, transactions: list[Transaction], timestamp: Optional[float] = None, nonce=0, hash: Optional[str] = None):
        self.index = index
        self.previous_hash = previous_hash
        self.transactions = transactions
        self.nonce = nonce
        self.timestamp = timestamp if timestamp is not None else time.time()
        self.hash = hash if hash is not None else self.calculate_hash()

    def calculate_hash(self) -> str:
        block_data = json.dumps({
            "index": self.index,
            "previous_hash": self.previous_hash,
            "transactions": [transaction.tx_hash for transaction in self.transactions],
            "nonce": self.nonce,
            "timestamp": self.timestamp
        }, sort_keys=True)
        return hashlib.sha256(block_data.encode()).hexdigest()

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, indent=4)

    def to_dict(self):
        return {
            "index": self.index,
            "previous_hash": self.previous_hash,
            "transactions": [transaction.to_dict() for transaction in self.transactions],
            "nonce": self.nonce,
            "timestamp": self.timestamp,
            "hash": self.hash
        }

    def mine_block(self, difficulty: int):
        target = "0" * difficulty
        while not self.hash.startswith(target):
            self.nonce += 1
            self.hash = self.calculate_hash()

    def is_valid(self, difficulty: int, previous_block: 'Block') -> bool:
        logger.debug(f"Validating the block: {self.hash}")
        if not self.previous_hash == previous_block.hash:
            logger.debug(f"The previous hash from your blockchain is not the same as the previous hash of the received block")
            return False

        if not self.hash.startswith("0" * difficulty):
            logger.debug(f"The hash doesn't start with enough zeros. Difficulty: {difficulty}")
            return False

        if not self.hash == self.calculate_hash():
            logger.warning(f"The hash doesn't match with the calculated hash")
            return False

        for transaction in self.transactions:
            if not transaction.is_valid():
                return False

        return True
