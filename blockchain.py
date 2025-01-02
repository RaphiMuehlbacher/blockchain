import json
import time

from block import Block
from transaction import Transaction
from utils import setup_logger

logger = setup_logger()


class Blockchain:
    def __init__(self, difficulty: int):
        self.chain: list[Block] = [self.create_genesis()]
        self.pending_transactions: list[Transaction] = []
        self.difficulty = difficulty

    @staticmethod
    def create_genesis() -> Block:
        genesis_block = Block(0, "0", [], timestamp=0)
        genesis_block.hash = "00000000000000000000000000000000"
        return genesis_block

    def get_last_block(self) -> Block:
        return self.chain[-1]

    def tx_in_mempool(self, transaction: Transaction) -> bool:
        return transaction.tx_hash in [transaction.tx_hash for transaction in self.pending_transactions]

    def add_transaction(self, transaction: Transaction) -> bool:
        if not transaction.is_valid() or self.tx_in_mempool(transaction):
            return False
        self.pending_transactions.append(transaction)
        return True

    def add_block(self, block: Block) -> bool:
        previous_block = self.get_last_block()
        if not block.is_valid(self.difficulty, previous_block):
            return False

        included_tx_hashes = {tx.tx_hash for tx in block.transactions}
        self.pending_transactions = [tx for tx in self.pending_transactions if tx.tx_hash not in included_tx_hashes]
        self.chain.append(block)
        return True

    def mine_pending_transactions(self, miner_address: str) -> Block | None:
        reward_transaction = Transaction(receiver=miner_address, amount=10.0, is_coinbase=True)

        while len(self.pending_transactions) < 3:
            time.sleep(5)

        sorted_pending_transactions = sorted(self.pending_transactions, key=lambda tx: tx.tx_hash)
        transactions_to_include = [reward_transaction] + sorted_pending_transactions

        new_block = Block(len(self.chain), self.get_last_block().hash, transactions_to_include)
        new_block.mine_block(self.difficulty)

        logger.debug(f"Successfully mined the block {new_block.hash}. Now validating and adding the block.")
        if self.add_block(new_block):
            included_tx_hashes = {tx.tx_hash for tx in transactions_to_include}

            self.pending_transactions = [tx for tx in self.pending_transactions if tx.tx_hash not in included_tx_hashes]
            logger.debug(f"Block: {new_block.hash} got added successfully.")
            return new_block

        logger.warning(f"Block: {new_block.hash} didn't get added successfully")
        return None

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), sort_keys=True, indent=4)

    def to_dict(self):
        return {
            "chain": [block.to_dict() for block in self.chain],
            "pending_transactions": [transaction.to_dict() for transaction in self.pending_transactions],
            "difficulty": self.difficulty
        }

    def is_valid(self) -> bool:
        logger.debug(f"Validating the Blockchain")
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i - 1]

            if current_block.previous_hash != previous_block.hash:
                logger.debug(f"The previous hash of the block: {current_block} doesn't match with the hash of the previous block in the blockchain.")
                return False

            if not current_block.is_valid(difficulty=self.difficulty, previous_block=previous_block):
                return False
        logger.debug(f"The Blockchain is valid!")
        return True


