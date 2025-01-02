import concurrent.futures
import json
import socket
import sqlite3
import threading
import time
import os

from ecdsa import SigningKey
from typing_extensions import Optional


from block import Block
from blockchain import Blockchain
from transaction import Transaction
from utils import setup_logger

logger = setup_logger()

GENESIS_PORT = 3070
GENESIS_IP = "localhost"

GOSSIP_COUNT = 2
GOSSIP_RATE = 15


class BaseNode:
    def __init__(self, port):
        self.port = port
        self.host = "localhost"
        self.peer_manager = PeerManager()
        self.blockchain = Blockchain(difficulty=6)

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()

        logger.debug(f"Node started on {self.host}:{self.port}")
        threading.Thread(target=self.start_health_check, daemon=True).start()
        self.start_extra_threads()
        while True:
            client, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client,)).start()

    def start_extra_threads(self):
        pass

    def start_health_check(self):
        logger.debug("Starting health check")
        while True:
            self.perform_health_check()
            time.sleep(30)

    def perform_health_check(self):
        raise NotImplementedError

    def handle_client(self, client: socket.socket):
        buffer = b""
        try:
            while True:
                data = client.recv(1024)
                if not data:
                    break
                buffer += data

                while len(buffer) >= 4:
                    msg_len = int.from_bytes(buffer[:4], 'big')
                    total_msg_len = 4 + msg_len

                    if len(buffer) >= total_msg_len:
                        message = buffer[4:total_msg_len]
                        buffer = buffer[total_msg_len:]

                        try:
                            self.process_message(message.decode(), client)
                        except Exception as e:
                            logger.warning(f"Error handling message: {e}", exc_info=True)
                    else:
                        break

        except Exception as e:
            logger.warning(f"Error handling the client: {e}", exc_info=True)

    def process_message(self, data: str, client: socket.socket):
        raise NotImplementedError


class BootstrapNode(BaseNode):
    GOSSIP_COUNT = 3

    def __init__(self, port=GENESIS_PORT):
        super().__init__(port)

    def process_message(self, data: str, client: socket.socket):
        try:
            message: dict = json.loads(data)

            if message["type"] == "request_peers":
                peer_addr = message["address"]

                self.peer_manager.add_peer(peer_addr, None)

                peers = self.peer_manager.get_peers(self.GOSSIP_COUNT, peer_addr)

                while len(peers) < self.GOSSIP_COUNT:
                    logger.warning(f"Insufficient peers in database, {len(peers)} peers so far: {peers}")
                    time.sleep(5)
                    peers = self.peer_manager.get_peers(self.GOSSIP_COUNT, peer_addr)

                peers_message = {
                    "type": "from_genesis",
                    "peers": peers
                }

                logger.debug(f"Genesis is sending to {peer_addr[0]}:{peer_addr[1]}: {json.dumps(peers_message)}")
                client.sendall(json.dumps(peers_message).encode())

        except Exception as e:
            logger.critical(f"Error processing message: {e}", exc_info=True)

    def perform_health_check(self):
        logger.debug(f"Performing health check")
        try:
            for peer_addr in self.peer_manager.get_all_peers():
                try:
                    with socket.create_connection(peer_addr, timeout=30) as server:
                        message = {"type": "genesis_health_check"}
                        message_bytes = json.dumps(message).encode()
                        length_prefix = len(message_bytes).to_bytes(4, 'big')
                        server.sendall(length_prefix + message_bytes)

                        logger.debug(f"SENDING TO {peer_addr} HEALTH CHECK")
                        logger.debug(f"Genesis is sending to {peer_addr[0]}:{peer_addr[1]}: {message}")
                        response = server.recv(1024).decode('utf-8')
                        health_response: dict = json.loads(response)
                        if health_response["type"] == "genesis_health_check_response" and health_response["status"] == "healthy":
                            logger.debug(f"Received healthy status from {peer_addr[0]}:{peer_addr[1]}")
                            self.peer_manager.set_online(peer_addr)
                            continue

                        logger.warning(f"Didn't receive a healthy check response from {peer_addr[0]}:{peer_addr[1]}")
                        raise Exception("No response from peer")

                except (socket.timeout, ConnectionRefusedError, OSError) as e:
                    logger.warning(f"Peer {peer_addr[0]}:{peer_addr[1]} is offline. Setting to offline. Error: {e}")
                    self.peer_manager.set_offline(peer_addr)

        except Exception as e:
            logger.critical(f"Failed to perform health check: {e}", exc_info=True)


class Node(BaseNode):
    MAX_PEERS = 10

    def __init__(self, port, private_key: SigningKey):
        self.private_key = private_key
        super().__init__(port)

    def process_message(self, data: str, client: Optional[socket.socket]):
        try:
            message: dict = json.loads(data)

            if message["type"] == "send_peers":
                peer_addr_from_sender = message["address"]
                self.peer_manager.add_peer(peer_addr_from_sender, self.MAX_PEERS)
                for peer_addr in message["peers"]:
                    self.peer_manager.add_peer(peer_addr, self.MAX_PEERS)

            elif message["type"] == "from_genesis":
                for peer_addr in message["peers"]:
                    self.peer_manager.add_peer(peer_addr, self.MAX_PEERS)

            elif message["type"] == "health_check":
                peer_addr = message["address"]
                logger.debug(f"Received health check from {peer_addr[0]}:{peer_addr[1]}")
                self.peer_manager.add_peer(peer_addr, self.MAX_PEERS)

                message = {"type": "health_check_response", "status": "healthy"}
                client.sendall(json.dumps(message).encode())

            elif message["type"] == "genesis_health_check":
                logger.debug(f"Received health check from genesis")
                message = {"type": "genesis_health_check_response", "status": "healthy"}
                client.sendall(json.dumps(message).encode())

            elif message["type"] == "new_block_mined":
                block_data = message["block"]

                mined_block = Block(
                    index=block_data["index"],
                    previous_hash=block_data["previous_hash"],
                    transactions=[Transaction(**tx) for tx in block_data["transactions"]],
                    timestamp=block_data["timestamp"],
                    nonce=block_data["nonce"]
                )
                if self.blockchain.add_block(mined_block):
                    new_block_message = {
                        "type": "new_block_mined",
                        "block": mined_block.to_dict(),
                        "address": [self.host, self.port]
                    }
                    logger.info(f"Broadcasting received block: {mined_block.hash}")
                    self.broadcast(new_block_message)

            elif message["type"] == "new_transaction":
                transaction_data = message["transaction"]
                new_transaction = Transaction(
                    receiver=transaction_data["receiver"],
                    amount=float(transaction_data["amount"]),
                    sender=transaction_data["sender"],
                    is_coinbase=bool(transaction_data["is_coinbase"]),
                    signature=transaction_data["signature"],
                    tx_hash=transaction_data["tx_hash"]
                )
                if self.blockchain.add_transaction(new_transaction):
                    new_transaction_message = {
                        "type": "new_transaction",
                        "transaction": new_transaction.to_dict(),
                        "address": [self.host, self.port]
                    }
                    logger.debug(f"Added transaction and broadcasting it: {new_transaction.tx_hash}")
                    self.broadcast(new_transaction_message)

        except Exception as e:
            logger.critical(f"Error processing message: {e}", exc_info=True)

    def start_extra_threads(self):
        threading.Thread(target=self.gossip, daemon=True).start()
        threading.Thread(target=self.handle_user_input, daemon=True).start()
        threading.Thread(target=self.mine_blocks, daemon=True).start()

    def mine_blocks(self):
        while True:
            new_block = self.blockchain.mine_pending_transactions(self.private_key.get_verifying_key().to_string().hex())

            if new_block is not None:
                logger.info(f"You successfully mine a block and broadcasting it: {new_block.hash}")

                message = {
                    "type": "new_block_mined",
                    "block": new_block.to_dict(),
                    "address": [self.host, self.port]
                }

                self.broadcast(message)

    def broadcast(self, message: dict):
        logger.info(f"Starting broadcasting this message: {message}")

        def send_message_to_peer(peer):
            try:
                serialized_message = json.dumps(message).encode()
                length_prefix = len(serialized_message).to_bytes(4, "big")
                logger.info(f"Creating connection to {peer}...")
                with socket.create_connection(peer, timeout=10) as conn:
                    conn.sendall(length_prefix + serialized_message)
                    logger.info(f"Finished sending message to {peer}")
            except Exception as e:
                logger.warning(f"Error notifying peer {peer}: {e}", exc_info=True)
                self.peer_manager.remove_peer(peer)

        peers = self.peer_manager.get_all_peers()
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(peers)) as executor:
            futures = [executor.submit(send_message_to_peer, peer) for peer in peers]
            concurrent.futures.wait(futures)

        logger.info("Broadcasting completed.")

    def handle_user_input(self):
        while True:
            try:
                user_input = input("Enter command: ").strip().lower()
                if user_input == "exit":
                    logger.info("Shutting down the node... BUT NOT YET IMPLEMENTED")

                elif user_input == "peers":
                    peers = self.peer_manager.get_all_peers()
                    logger.info(f"Current peers: {peers}")
                elif user_input.startswith("add_peer"):

                    _, peer_info = user_input.split()
                    ip, port = peer_info.split(":")
                    self.peer_manager.add_peer((ip, int(port)), None)

                    logger.info(f"Added peer: {ip}:{port}")
                elif user_input == "health_check":
                    logger.info("Manually starting a health check...")
                    self.perform_health_check()

                elif user_input == "show_blockchain":
                    data = self.blockchain.to_json()
                    print(data)

                elif user_input.startswith("add_transaction"):
                    _, receiver, amount = user_input.split()
                    transaction = Transaction(sender=self.private_key.get_verifying_key().to_string().hex(), receiver=receiver, amount=float(amount))
                    transaction.sign_transaction(self.private_key)

                    if self.blockchain.add_transaction(transaction):
                        message = {
                            "type": "new_transaction",
                            "transaction": transaction.to_dict(),
                            "address": [self.host, self.port]
                        }
                        logger.debug(f"Added transaction from USER and broadcasting it: {transaction.to_json()}")
                        self.broadcast(message)

                elif user_input == "show_pending_transactions":
                    transactions = [tx.to_dict() for tx in self.blockchain.pending_transactions]
                    print(json.dumps(transactions, indent=4, sort_keys=True))

                elif user_input == "help":
                    logger.info("Available commands: peers, add_peer <ip:port>, health_check, add_transaction <receiver> exit")
                else:
                    logger.warning("Unknown command. Type 'help' for available commands.")
            except Exception as e:
                logger.error(f"Error handling user input: {e}", exc_info=True)

    def gossip(self):
        while True:
            peer_addresses = self.peer_manager.get_peers(GOSSIP_COUNT, None)
            if not peer_addresses:
                logger.debug("Not enough peers in database, connecting to genesis")
                self.connect_to_genesis()
            else:
                for peer_addr in peer_addresses:
                    self.gossip_with_peer(peer_addr)
            time.sleep(GOSSIP_RATE)

    def connect_to_genesis(self):
        try:
            with socket.create_connection((GENESIS_IP, GENESIS_PORT), timeout=60) as server:
                message = {"type": "request_peers", "address": [self.host, self.port]}

                logger.debug(f"Sending to genesis: {json.dumps(message)}")

                message_bytes = json.dumps(message).encode()
                length_prefix = len(message_bytes).to_bytes(4, 'big')
                server.sendall(length_prefix + message_bytes)

                response = server.recv(1024).decode('utf-8')
                logger.debug(f"Received from genesis: {response}")

                self.process_message(response, None)

        except Exception as e:
            logger.critical(f"Failed to connect to genesis node: {e}", exc_info=True)

    def gossip_with_peer(self, peer_addr: tuple[str, int]):
        logger.debug(f"Started gossiping with peer: {peer_addr[0]}:{peer_addr[1]}")
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.connect((peer_addr[0], peer_addr[1]))
            peers = self.peer_manager.get_peers(2, peer_addr)

            message = {"type": "send_peers", "peers": peers, "address": [self.host, self.port]}
            message_bytes = json.dumps(message).encode()
            length_prefix = len(message_bytes).to_bytes(4, 'big')

            logger.debug(f"Sending to peer {peer_addr[0]}:{peer_addr[1]}: {message}")
            server.sendall(length_prefix + message_bytes)

        except Exception as e:
            logger.warning(f"Error gossiping with peer {peer_addr}: {e}", exc_info=True)
            self.peer_manager.remove_peer(peer_addr)

    def perform_health_check(self):
        logger.debug(f"Performing health check")
        try:
            for peer_addr in self.peer_manager.get_all_peers():
                try:
                    with socket.create_connection(peer_addr, timeout=20) as server:
                        message = {"type": "health_check", "address": [self.host, self.port]}
                        message_bytes = json.dumps(message).encode()
                        length_prefix = len(message_bytes).to_bytes(4, 'big')

                        server.sendall(length_prefix + message_bytes)

                        logger.debug(f"SENDING TO {peer_addr} HEALTH CHECK")
                        response = server.recv(1024).decode('utf-8')
                        health_response: dict = json.loads(response)
                        if health_response["type"] == "health_check_response" and health_response["status"] == "healthy":
                            logger.debug(f"Received healthy status from {peer_addr[0]}:{peer_addr[1]}")
                            continue

                        logger.warning(f"Didn't receive a healthy check response from {peer_addr[0]}:{peer_addr[1]}")
                        raise Exception("No response from peer")

                except (socket.timeout, ConnectionRefusedError, OSError) as e:
                    logger.warning(f"Peer {peer_addr[0]}:{peer_addr[1]} is offline. Removing from database. Error: {e}")
                    self.peer_manager.remove_peer(peer_addr)

        except Exception as e:
            logger.critical(f"Failed to perform health check: {e}", exc_info=True)


class PeerManager:
    def __init__(self):
        self.database_path = self.get_database_path()
        self.create_table()

    @staticmethod
    def get_database_path() -> str:
        current_folder = os.path.dirname(os.path.abspath(__file__))
        database_path = os.path.join(current_folder, "blockchain.db")
        logger.debug(f"Using database at: {database_path}")
        return database_path

    def get_connection(self) -> sqlite3.Connection:
        return sqlite3.connect(self.database_path, check_same_thread=False)

    def create_table(self):
        try:
            with self.get_connection() as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS node (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ip TEXT NOT NULL,
                        port INTEGER NOT NULL,
                        is_offline BOOLEAN DEFAULT FALSE,
                        UNIQUE(ip, port)
                    )
                """)
                logger.debug("Table 'node' created or verified successfully.")
        except sqlite3.Error as e:
            logger.critical(f"Error creating table: {e}", exc_info=True)

    def set_online(self, peer_addr: tuple[str, int]):
        try:
            with self.get_connection() as conn:
                conn.execute("UPDATE node SET is_offline=FALSE WHERE ip=? AND port=?", peer_addr)
                logger.debug(f"Peer {peer_addr} set to online.")
        except sqlite3.Error as e:
            logger.critical(f"Error setting peer online: {e}", exc_info=True)

    def set_offline(self, peer_addr: tuple[str, int]):
        try:
            with self.get_connection() as conn:
                conn.execute("UPDATE node SET is_offline=TRUE WHERE ip=? AND port=?", peer_addr)
                logger.debug(f"Peer {peer_addr} set to offline.")
        except sqlite3.Error as e:
            logger.critical(f"Error setting peer offline: {e}", exc_info=True)

    def add_peer(self, peer_addr: tuple[str, int], max_peers: Optional[int]):
        current_rows = self.get_rows()
        logger.debug(f"Trying to add peer: {peer_addr[0]}:{peer_addr[1]}, rows: {current_rows}, max_peers: {max_peers} ")

        if max_peers is not None and self.get_rows() > max_peers:
            logger.debug(f"Already {max_peers} in database. Not inserting {peer_addr[0]}:{peer_addr[1]}")
            return

        try:
            with self.get_connection() as conn:
                cursor = conn.execute("INSERT OR IGNORE INTO node (ip, port) VALUES (?, ?)", peer_addr)
                if cursor.rowcount > 0:
                    logger.debug(f"Peer {peer_addr[0]}:{peer_addr[1]} got inserted successfully to the database")

        except sqlite3.Error as e:
            logger.critical(f"Error adding peer: {e}", exc_info=True)

    def get_rows(self) -> int:
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT COUNT(*) FROM node").fetchone()
                row_count = result[0] if result else 0
                return row_count
        except sqlite3.Error as e:
            logger.critical(f"Error fetching row count: {e}", exc_info=True)
            return 0

    def get_peers(self, count: int, user_addr: Optional[tuple[str, int]]) -> list[tuple[str, int]]:
        try:
            with self.get_connection() as conn:
                query = """
                    SELECT ip, port FROM node
                    WHERE (ip != ? OR port != ?) AND is_offline = FALSE
                    ORDER BY RANDOM() LIMIT ?
                """

                user_ip, user_port = user_addr or ("", 0)
                result = conn.execute(query, (user_ip, user_port, count)).fetchall()
                logger.debug(f"Fetched peers: {result}")
                return result
        except sqlite3.Error as e:
            logger.critical(f"Error fetching peers: {e}", exc_info=True)
            return []

    def get_all_peers(self) -> list[tuple[str, int]]:
        try:
            with self.get_connection() as conn:
                result = conn.execute("SELECT ip, port FROM node").fetchall()
                return result
        except sqlite3.Error as e:
            logger.critical(f"Error fetching all peers: {e}", exc_info=True)
            return []

    def remove_peer(self, peer: tuple[str, int]):
        try:
            with self.get_connection() as conn:
                cursor = conn.execute("DELETE FROM node WHERE ip=? AND port=?", peer)
                if cursor.rowcount > 0:
                    logger.debug(f"Peer {peer} removed successfully.")
                else:
                    logger.debug(f"Peer {peer} not found in the database.")
        except sqlite3.Error as e:
            logger.critical(f"Error removing peer: {e}", exc_info=True)
