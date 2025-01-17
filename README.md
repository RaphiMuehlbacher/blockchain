# Blockchain Project

## Overview

This project implements a basic blockchain with functionalities for block mining, transaction validation, and peer-to-peer communication. The blockchain is designed to be decentralized, with nodes communicating and sharing information to maintain the integrity and consistency of the blockchain.

## Features

- **Block Mining**: Nodes can mine new blocks by solving a cryptographic puzzle.
- **Transaction Validation**: Transactions are validated before being added to the blockchain.
- **Peer-to-Peer Communication**: Nodes communicate with each other to share information about the blockchain and transactions.
- **Health Checks**: Nodes perform regular health checks to ensure peers are online and responsive.
- **SQLite Database**: Peer information is stored in an SQLite database for persistence.

## Components

### Node

The `Node` class handles the core functionalities of the blockchain, including communication with peers, block mining, and transaction validation.

- **Gossip Protocol**: Nodes use a gossip protocol to share information about peers.
- **Health Checks**: Nodes perform health checks to ensure peers are online.
- **Transaction Handling**: Nodes validate and process transactions.

### PeerManager

The `PeerManager` class manages the peer information stored in the SQLite database.

- **Add Peer**: Adds a new peer to the database.
- **Remove Peer**: Removes a peer from the database.
- **Set Online/Offline**: Updates the online status of a peer.
- **Get Peers**: Retrieves a list of peers from the database.


## How It Works

### Block Mining

Nodes mine new blocks by solving a cryptographic puzzle. When a block is mined, it is broadcasted to all peers. If a node receives a new block it validates the received block.

### Block Validation

Block validation involves several checks:
1. **Hash Verification**: The hash of the block must be correct and meet the difficulty target.
2. **Transaction Verification**: All transactions in the block must be valid and not double-spent.
3. **Previous Block Hash**: The block must correctly reference the hash of the previous block in the chain.
4. **Timestamp Validation**: The block's timestamp must be greater than the previous block's timestamp and less than the current time.

### Transaction Validation

Transactions are validated before being added to the blockchain. This includes checking:
1. **Signature Verification**: Ensuring the transaction is signed by the sender.
2. **Nonce Verification**: Ensuring the nonce is correct and prevents replay attacks.
3. **Balance Check**: Ensuring the sender has sufficient balance for the transaction.
Transactions with too high nonces are put on hold until they can be processed.

### Peer-to-Peer Communication

Nodes communicate with each other using a gossip protocol. They share information about peers and the blockchain to ensure consistency and integrity.

### Health Checks

Nodes perform regular health checks to ensure peers are online and responsive. If a peer is found to be offline, it is removed from the database.

### Bootstrap Node
A bootstrap node is a well-known node that new nodes only connect once to when they join the network. The bootstrap node provides information about other peers in the network, allowing the new node to establish connections and participate in the blockchain. 
