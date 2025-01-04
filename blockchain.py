import json
import sqlite3
import time
from decimal import Decimal

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

    def tx_in_mempool(self, tx: Transaction) -> bool:
        return tx.nonce in {transaction.nonce for transaction in self.pending_transactions if transaction.sender == tx.sender}

    def add_transaction(self, transaction: Transaction) -> bool:
        if not transaction.is_valid():
            return False

        sender_balance = self.account_manager.get_balance(transaction.sender)
        all_senders_spending = self.get_all_spendings_from_transactions(transaction.sender, self.pending_transactions) + transaction.amount
        if sender_balance is None or sender_balance < all_senders_spending:
            logger.debug(f"The transactions in the mempool are more than the balance from the sender. TX: {transaction}, sender_balance: {sender_balance}, total_spent: {all_senders_spending}")
            return False

        sender_nonce = self.account_manager.get_nonce(transaction.sender)
        if sender_nonce is None or transaction.nonce != sender_nonce:
            logger.debug(f"Transaction nonce is less than the senders nonce. transaction_nonce: {transaction.nonce}, sender_nonce: {sender_nonce}")
            return False

        self.account_manager.increment_nonce(transaction.sender)

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
        transactions_by_sender = {}
        coinbase_transactions = []
        for transaction in transactions:
            if transaction.sender == "COINBASE":
                coinbase_transactions.append(transaction)
                continue

            if transaction.sender not in transactions_by_sender:
                transactions_by_sender[transaction.sender] = []
            transactions_by_sender[transaction.sender].append(transaction)

        for sender, sender_transactions in transactions_by_sender.items():
            sender_transactions.sort(key=lambda tx: tx.nonce)
            current_nonce = self.account_manager.get_nonce(sender) - len(sender_transactions)
            sender_balance = float(self.account_manager.get_balance(sender))

            if sender_balance is None:
                logger.warning(f"The sender_balance is None. sender: {sender}")
                return False

            total_spent = 0.0

            for transaction in sender_transactions:
                if transaction.nonce != current_nonce:
                    logger.info(f"Transaction nonce is not the same as the current_nonce. transaction_nonce: {transaction.nonce}, current_nonce: {current_nonce}")
                    return False

                total_spent += transaction.amount
                current_nonce += 1

            if total_spent > sender_balance:
                logger.info(f"Total spent is more than sender_balance. total_spent: {total_spent}, sender_balance: {sender_balance}")
                return False

            for transaction in sender_transactions:
                current_balance = self.account_manager.get_balance(sender)
                transaction_amount = Decimal(transaction.amount).quantize(Decimal("0.0000001"))
                new_balance = (current_balance - transaction_amount).quantize(Decimal("0.0000001"))

                self.account_manager.upsert_balance(transaction.sender, new_balance)
                self.account_manager.increment_nonce(transaction.sender)

                receiver_balance = self.account_manager.get_balance(transaction.receiver) or Decimal(0).quantize(Decimal("0.0000001"))
                new_receiver_balance = (receiver_balance + transaction_amount).quantize(Decimal("0.0000001"))
                self.account_manager.upsert_balance(transaction.receiver, new_receiver_balance)

        if len(coinbase_transactions) != 1:
            logger.warning(f"There are {len(coinbase_transactions)} coinbase transactions.")
        coinbase_transaction = coinbase_transactions[0]
        receiver_balance = self.account_manager.get_balance(coinbase_transaction.receiver) or Decimal(0).quantize(Decimal("0.0000001"))
        new_receiver_balance = (receiver_balance + Decimal(10.0).quantize(Decimal("0.0000001"))).quantize(Decimal("0.0000001"))
        self.account_manager.upsert_balance(coinbase_transaction.receiver, new_receiver_balance)
        return True

    @staticmethod
    def get_all_spendings_from_transactions(sender: str, transactions: list[Transaction]) -> float:
        return sum(transaction.amount for transaction in transactions if transaction.sender == sender)

    @staticmethod
    def get_all_transactions_from_sender(sender: str, transactions: list[Transaction]) -> list[Transaction]:
        return [transaction for transaction in transactions if transaction.sender == sender]

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

    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.database_path, check_same_thread=False)

    def create_account_table(self):
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS account (
                        public_key TEXT PRIMARY KEY,
                        nonce INTEGER DEFAULT 0,
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

    def upsert_balance(self, public_key: str, balance: Decimal) -> None:
        smallest_unit_balance = int((Decimal(balance) * Decimal(1e6)).quantize(Decimal("0.0000001")))
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

    def get_balance(self, public_key: str) -> Decimal | None:
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT balance FROM account WHERE public_key=?", (public_key,)).fetchone()
                if result is not None:
                    return (Decimal(result[0]) / Decimal(1e6)).quantize(Decimal("0.0000001"))
                return None
        except sqlite3.Error as e:
            logger.critical(f"Error getting the balance from {public_key}, Error: {e}")

    def get_nonce(self, public_key: str) -> int | None:
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT nonce FROM account WHERE public_key=?", (public_key,)).fetchone()
                if result is not None:
                    return result[0]
                return None
        except sqlite3.Error as e:
            logger.critical(f"Error getting the nonce from {public_key}, Error: {e}")

    def increment_nonce(self, public_key: str) -> None:
        try:
            with self.get_connection() as conn:
                conn.execute("UPDATE account SET nonce = nonce + 1 WHERE public_key=?", (public_key,))
        except sqlite3.Error as e:
            logger.critical(f"Error incrementing the nonce from {public_key}, Error: {e}")
