import pytest
from ecdsa import SigningKey, SECP256k1
from transaction import Transaction


def create_transaction(sender_public_key, receiver="receiver_address", amount=20.0):
    """Helper function to create a Transaction instance."""
    return Transaction(
        sender=sender_public_key,
        receiver=receiver,
        amount=amount,
    )


def create_coinbase_transaction(receiver="receiver_address", amount=10.0):
    return Transaction(
        receiver=receiver,
        amount=amount,
        is_coinbase=True
    )


def test_valid_coinbase_transaction():
    """Test that a valid coinbase transaction is valid."""
    transaction = create_coinbase_transaction()
    assert transaction.is_valid(), "Valid coinbase transaction should be valid."


def test_invalid_coinbase_transaction_amount():
    """Test that a coinbase transaction with an invalid amount is invalid."""
    transaction = create_coinbase_transaction(amount=9.0)
    assert not transaction.is_valid(), "Coinbase transaction with incorrect amount should be invalid."


def test_valid_transaction_with_signature():
    """Test a transaction with a valid signature."""
    private_key = SigningKey.generate(curve=SECP256k1)
    transaction = create_transaction(sender_public_key=private_key.get_verifying_key().to_string().hex(), amount=5.0)
    transaction.sign_transaction(private_key=private_key)
    assert transaction.is_valid(), "Transaction with valid signature should be valid."


def test_invalid_transaction_with_fake_signature():
    """Test a transaction with a fake or tampered signature."""
    private_key = SigningKey.generate(curve=SECP256k1)
    transaction = create_transaction(sender_public_key=private_key.get_verifying_key().to_string().hex(), amount=5.0)
    transaction.sign_transaction(private_key=private_key)
    transaction.signature = "a" * 128
    assert not transaction.is_valid(), "Transaction with a fake signature should be invalid."


def test_invalid_transaction_with_wrong_public_key():
    """Test a transaction where the public key does not match the signature."""
    private_key_1 = SigningKey.generate(curve=SECP256k1)
    private_key_2 = SigningKey.generate(curve=SECP256k1)
    transaction = create_transaction(sender_public_key=private_key_1.get_verifying_key().to_string().hex(), amount=5.0)
    transaction.sign_transaction(private_key=private_key_1)
    transaction.sender = private_key_2.get_verifying_key().to_string().hex()
    assert not transaction.is_valid(), "Transaction with the wrong public key should be invalid."


def test_transaction_hash_is_deterministic():
    """Test that the hash is consistent for the same transaction data."""
    private_key = SigningKey.generate(curve=SECP256k1)
    transaction1 = create_transaction(sender_public_key=private_key.get_verifying_key().to_string().hex(), amount=10.0)
    transaction2 = create_transaction(sender_public_key=private_key.get_verifying_key().to_string().hex(), amount=10.0)
    assert transaction1.tx_hash == transaction2.tx_hash, "Hashes should be identical for the same transaction data."


def test_transaction_hash_changes_with_data():
    """Test that the hash changes when the transaction data changes."""
    private_key = SigningKey.generate(curve=SECP256k1)
    transaction1 = create_transaction(sender_public_key=private_key.get_verifying_key().to_string().hex(), amount=10.0)
    transaction2 = create_transaction(sender_public_key=private_key.get_verifying_key().to_string().hex(), amount=20.0)
    assert transaction1.tx_hash != transaction2.tx_hash, "Hashes should change when transaction data changes."
