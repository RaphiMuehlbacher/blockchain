import json
import hashlib
from typing import Optional

from ecdsa import VerifyingKey, SECP256k1, BadSignatureError, SigningKey
from utils import setup_logger

logger = setup_logger()


class Transaction:
    def __init__(self, receiver: str, amount: float, sender: Optional[str] = None, is_coinbase: bool = False, signature: Optional[str] = None, tx_hash: Optional[str] = None):
        self.is_coinbase = is_coinbase
        self.receiver = receiver
        self.amount = amount
        self.signature = signature

        if is_coinbase:
            self.sender = "COINBASE"
        else:
            self.sender = sender
        self.tx_hash = tx_hash if tx_hash is not None else self.calculate_hash()

    def calculate_hash(self) -> str:
        transaction_data = json.dumps({
            "is_coinbase": self.is_coinbase,
            "sender": self.sender,
            "receiver": self.receiver,
            "amount": self.amount,
        }, sort_keys=True)

        return hashlib.sha256(transaction_data.encode()).hexdigest()

    def to_json(self):
        return json.dumps(self.to_dict(), sort_keys=True, indent=4)

    def to_dict(self):
        return {
            "sender": self.sender,
            "receiver": self.receiver,
            "amount": self.amount,
            "is_coinbase": self.is_coinbase,
            "signature": self.signature,
            "tx_hash": self.tx_hash
        }

    def sign_transaction(self, private_key: SigningKey):
        if self.is_coinbase:
            return
        self.signature = private_key.sign_deterministic(self.tx_hash.encode()).hex()

    def is_valid(self) -> bool:
        logger.debug(f"Validating the transaction: {self.tx_hash}")
        if self.is_coinbase:
            logger.debug(f"The transaction is a coinbase transaction. Amount: {self.amount}")
            return self.amount == 10.0

        if not self.sender or not self.signature:
            logger.info(f"The transaction doesn't have a sender or a signature")
            return False

        try:
            public_key = VerifyingKey.from_string(bytes.fromhex(self.sender), curve=SECP256k1)
            public_key.verify(bytes.fromhex(self.signature), self.tx_hash.encode())
            logger.debug(f"The transaction ({self.tx_hash}) is valid.")
            return True
        except (BadSignatureError, ValueError):
            logger.debug(f"The signature isn't correct. The transaction ({self.tx_hash}) is not valid.")
            return False
