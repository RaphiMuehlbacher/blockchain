import time
from ecdsa import SigningKey, SECP256k1
from transaction import Transaction
from block import Block
import json


def create_mock_transaction(amount: float, valid_signature: bool = True) -> Transaction:
    """Helper to create a mock transaction with a valid or invalid signature."""
    private_key = SigningKey.generate(curve=SECP256k1)
    transaction = Transaction(
        sender=private_key.get_verifying_key().to_string().hex(),
        receiver="receiver_address",
        amount=amount
    )
    transaction.sign_transaction(private_key=private_key)

    if not valid_signature:
        transaction.signature = "a" * 128
    return transaction


def test_block_hash_changes_with_nonce():
    """Test that the block's hash changes when the nonce is updated."""
    transactions = [create_mock_transaction(amount=10.0)]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    initial_hash = block.hash
    block.nonce += 1
    block.hash = block.calculate_hash()

    assert initial_hash != block.hash, "Hashes should change when nonce changes."


def test_block_mining():
    """Test that a block successfully mines a hash meeting the difficulty target."""
    transactions = [create_mock_transaction(amount=10.0)]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    difficulty = 5
    block.mine_block(difficulty)

    assert block.hash.startswith("0" * difficulty), "Mined block hash should meet the difficulty target."


def test_block_is_valid_with_valid_transactions():
    """Test that a block with valid transactions and correct hash is valid."""
    transactions = [create_mock_transaction(amount=10.0)]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    difficulty = 5
    block.mine_block(difficulty)

    assert block.is_valid(difficulty) is True, "Block with valid transactions and hash should be valid."


def test_block_is_invalid_with_invalid_transactions():
    """Test that a block with invalid transactions is marked as invalid."""
    valid_transaction = create_mock_transaction(amount=10.0, valid_signature=True)
    invalid_transaction = create_mock_transaction(amount=5.0, valid_signature=False)

    transactions = [valid_transaction, invalid_transaction]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    difficulty = 5
    block.mine_block(difficulty)

    assert block.is_valid(difficulty) is False, "Block with invalid transactions should be invalid."


def test_block_is_invalid_with_wrong_hash():
    """Test that a block with tampered hash is marked as invalid."""
    transactions = [create_mock_transaction(amount=10.0)]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    difficulty = 5
    block.mine_block(difficulty)

    # Tamper with the block hash
    block.hash = "0" * 64

    assert block.is_valid(difficulty) is False, "Block with tampered hash should be invalid."


def test_block_json_serialization():
    """Test the block's JSON serialization and deserialization."""
    transactions = [create_mock_transaction(amount=10.0)]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    block_json = block.to_json()
    loaded_block = json.loads(block_json)

    assert loaded_block["index"] == block.index, "Index should match after JSON serialization."
    assert loaded_block["previous_hash"] == block.previous_hash, "Previous hash should match after JSON serialization."
    assert len(loaded_block["transactions"]) == len(block.transactions), "Transaction count should match."
    assert loaded_block["nonce"] == block.nonce, "Nonce should match after JSON serialization."
    assert loaded_block["timestamp"] == block.timestamp, "Timestamp should match after JSON serialization."


def test_block_timestamp_is_set():
    """Test that a block's timestamp is set correctly."""
    transactions = [create_mock_transaction(amount=10.0)]
    block = Block(index=0, previous_hash="0" * 64, transactions=transactions)

    assert time.time() >= block.timestamp, "Block timestamp should not be in the future."
