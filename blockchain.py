import json
import sqlite3
import time
from decimal import getcontext, Decimal

from block import Block
from transaction import Transaction
from utils import setup_logger, get_database_path

logger = setup_logger()


class Blockchain:
    def __init__(self, difficulty: int):
        self.chain: list[Block] = [self.create_genesis()]
        self.pending_transactions: list[Transaction] = []
        self.difficulty = difficulty
        self.account_manager = AccountManager()

    @staticmethod
    def create_genesis() -> Block:
        genesis_block = Block(0, "0", [], timestamp=0)
        genesis_block.hash = "00000000000000000000000000000000"
        return genesis_block

    def get_last_block(self) -> Block:
        return self.chain[-1]

    def tx_in_mempool(self, transaction: Transaction) -> bool:
        return transaction.tx_hash in {transaction.tx_hash for transaction in self.pending_transactions}

    def add_transaction(self, transaction: Transaction) -> bool:
        if not transaction.is_valid() or self.tx_in_mempool(transaction):
            return False

        sender_balance = self.account_manager.get_balance(transaction.sender)
        all_senders_spending = self.get_all_spendings_from_transactions(transaction.sender,
                                                                        self.pending_transactions) + transaction.amount
        if not sender_balance:
            return False
        if sender_balance < all_senders_spending:
            return False

        self.pending_transactions.append(transaction)
        return True

    def add_block(self, block: Block) -> bool:
        previous_block = self.get_last_block()
        if not block.is_valid(self.difficulty, previous_block):
            return False

        logger.info(f"Block {block.hash} is valid.")
        if not self.apply_transactions(block.transactions):
            logger.warning(f"Transactions couldn't be applied.")
            return False

        included_tx_hashes = {tx.tx_hash for tx in block.transactions}
        self.pending_transactions = [tx for tx in self.pending_transactions if tx.tx_hash not in included_tx_hashes]
        self.chain.append(block)
        logger.info(f"Block {block.hash} got added successfully.")
        return True

    def apply_transactions(self, transactions: list[Transaction]) -> bool:
        for transaction in transactions:
            if transaction.sender == "COINBASE":
                continue

            amount_from_all_transactions = self.get_all_spendings_from_transactions(transaction.sender, transactions)

            sender_balance = self.account_manager.get_balance(transaction.sender)
            if not sender_balance:
                return False

            new_balance = sender_balance - amount_from_all_transactions
            if new_balance < 0:
                return False

        for transaction in transactions:
            if not transaction.sender == "COINBASE":
                sender_balance = self.account_manager.get_balance(transaction.sender)
                self.account_manager.upsert_balance(transaction.sender, sender_balance - transaction.amount)

            receiver_balance = self.account_manager.get_balance(transaction.receiver) or 0
            self.account_manager.upsert_balance(transaction.receiver, receiver_balance + transaction.amount)
        return True

    @staticmethod
    def get_all_spendings_from_transactions(sender: str, transactions: list[Transaction]) -> float:
        amount = 0
        for transaction in transactions:
            if transaction.sender == sender:
                amount += transaction.amount
        return amount

    def mine_pending_transactions(self, miner_address: str) -> Block | None:
        reward_transaction = Transaction(receiver=miner_address, amount=10.0, is_coinbase=True)

        while len(self.pending_transactions) < 3:
            time.sleep(5)

        logger.info("Started mining transactions.")
        sorted_pending_transactions = sorted(self.pending_transactions, key=lambda tx: tx.tx_hash)
        transactions_to_include = [reward_transaction] + sorted_pending_transactions

        new_block = Block(len(self.chain), self.get_last_block().hash, transactions_to_include)
        new_block.mine_block(self.difficulty)

        logger.debug(f"Successfully mined the block {new_block.hash}. Now validating and adding the block.")
        if self.add_block(new_block):
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
                logger.debug(
                    f"The previous hash of the block: {current_block} doesn't match with the hash of the previous block in the blockchain.")
                return False

            if not current_block.is_valid(difficulty=self.difficulty, previous_block=previous_block):
                return False
        logger.debug(f"The Blockchain is valid!")
        return True


class AccountManager:
    def __init__(self):
        self.database_path = get_database_path()
        self.create_account_table()
        self.set_initial_balance()
        getcontext().prec = 10

    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.database_path, check_same_thread=False)

    def create_account_table(self):
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS account (
                        public_key TEXT PRIMARY KEY,
                        balance INTEGER DEFAULT 0
                    )
                """)
                logger.debug("Table 'account' created or verified successfully.")
        except sqlite3.Error as e:
            logger.critical(f"Error creating table: {e}")

    def set_initial_balance(self):
        try:
            initial_balance = Decimal(10) * Decimal(1e6)
            with self.get_connection() as conn:
                conn.execute("INSERT INTO account (public_key, balance) VALUES (?, ?)",
                             (
                                 '5c1b76e3bfa6131bc640ddc4500ada2900355f08794fb3a5f631e208b91bb4f5a519392236d2880304b75670b6be79997508e252db07fc4710b270163efdc47b',
                                 int(initial_balance)))

        except sqlite3.Error as e:
            logger.critical(f"Error setting initial balance: {e}")

    def upsert_balance(self, public_key: str, balance: float = 10) -> None:
        smallest_unit_balance = int(Decimal(balance) * Decimal(1e6))
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    INSERT INTO account (public_key, balance)
                    VALUES (?, ?) 
                    ON CONFLICT(public_key) DO UPDATE SET
                        balance = excluded.balance
                """, (public_key, smallest_unit_balance))

        except sqlite3.Error as e:
            logger.critical(f"Error adding an account: {e}")

    def get_balance(self, public_key: str) -> float | None:
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT balance FROM account WHERE public_key=?", (public_key,)).fetchone()
                if result is not None:
                    return float(Decimal(result[0]) / Decimal(1e6))
                return None
        except sqlite3.Error as e:
            logger.critical(f"Error getting the balance from {public_key}, Error: {e}")
