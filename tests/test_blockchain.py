from blockchain import Blockchain
from transaction import Transaction
from ecdsa import SigningKey, SECP256k1


def generate_signed_transaction(amount=5.0):
    """Generate a valid signed transaction with keys."""
    private_key = SigningKey.generate(curve=SECP256k1)
    transaction = Transaction(
        sender=private_key.get_verifying_key().to_string().hex(),
        receiver="receiver_address",
        amount=amount,
    )
    return transaction


def test_create_genesis_block():
    """Test that the genesis block is correctly created."""
    blockchain = Blockchain(difficulty=2)
    genesis_block = blockchain.chain[0]

    assert genesis_block.index == 0, "Genesis block index should be 0."
    assert genesis_block.previous_hash == "0", "Genesis block previous hash should be '0'."
    assert len(genesis_block.transactions) == 0, "Genesis block should have no transactions."


def test_mine_pending_transactions():
    """Test that mining adds a new block with transactions to the blockchain."""
    blockchain = Blockchain(difficulty=2)
    miner_address = "miner_123"

    # Add a few transactions
    transaction1 = generate_signed_transaction(amount=5.0)
    transaction2 = generate_signed_transaction(amount=2.5)
    blockchain.add_transaction(transaction1)
    blockchain.add_transaction(transaction2)

    # Mine pending transactions
    blockchain.mine_pending_transactions(miner_address)

    assert len(blockchain.chain) == 2, "Blockchain should now have 2 blocks."
    mined_block = blockchain.chain[-1]

    # Check transactions in the mined block
    assert len(mined_block.transactions) == 3, "Mined block should include 2 pending and 1 reward transaction."
    assert mined_block.transactions[0].is_coinbase is True, "First transaction should be a coinbase reward."


def test_blockchain_validity():
    """Test that a valid blockchain is recognized as valid."""
    blockchain = Blockchain(difficulty=2)
    miner_address = "miner_123"

    # Mine 2 blocks
    blockchain.add_transaction(generate_signed_transaction(amount=1.0))
    blockchain.mine_pending_transactions(miner_address)

    blockchain.add_transaction(generate_signed_transaction(amount=3.0))
    blockchain.mine_pending_transactions(miner_address)

    assert blockchain.is_valid() is True, "Blockchain should be valid after valid blocks are mined."


def test_blockchain_invalid_with_tampered_block():
    """Test that the blockchain becomes invalid if a block is tampered with."""
    blockchain = Blockchain(difficulty=2)
    miner_address = "miner_123"

    # Mine a block
    blockchain.add_transaction(generate_signed_transaction(amount=5.0))
    blockchain.mine_pending_transactions(miner_address)

    # Tamper with the block
    blockchain.chain[1].transactions[0].amount = 1000  # Modify a transaction
    blockchain.chain[1].hash = blockchain.chain[1].calculate_hash()

    assert blockchain.is_valid() is False, "Blockchain should be invalid if a block is tampered with."


def test_blockchain_invalid_with_incorrect_previous_hash():
    """Test blockchain validity when a block has an incorrect previous hash."""
    blockchain = Blockchain(difficulty=2)
    miner_address = "miner_123"

    # Mine two blocks
    blockchain.add_transaction(generate_signed_transaction(amount=4.0))
    blockchain.mine_pending_transactions(miner_address)

    blockchain.add_transaction(generate_signed_transaction(amount=6.0))
    blockchain.mine_pending_transactions(miner_address)

    # Tamper with previous_hash of the second block
    blockchain.chain[1].previous_hash = "1234567890abcdef"
    blockchain.chain[1].hash = blockchain.chain[1].calculate_hash()

    assert blockchain.is_valid() is False, "Blockchain should be invalid if previous hash is incorrect."


def test_mining_difficulty():
    """Test that blocks mined respect the given difficulty."""
    blockchain = Blockchain(difficulty=3)
    miner_address = "miner_123"

    blockchain.add_transaction(generate_signed_transaction(amount=2.0))
    blockchain.mine_pending_transactions(miner_address)

    mined_block = blockchain.chain[-1]
    assert mined_block.hash.startswith("0" * 3), "Mined block hash should meet the difficulty target."


def test_mining_resets_pending_transactions():
    """Test that pending transactions are cleared after mining."""
    blockchain = Blockchain(difficulty=2)
    miner_address = "miner_123"

    blockchain.add_transaction(generate_signed_transaction(amount=1.0))
    blockchain.add_transaction(generate_signed_transaction(amount=2.0))

    blockchain.mine_pending_transactions(miner_address)

    assert len(blockchain.pending_transactions) == 0, "Pending transactions should be cleared after mining."
