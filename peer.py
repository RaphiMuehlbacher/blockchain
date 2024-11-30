import json
import socket
import sqlite3
import threading
import time
import os

from typing_extensions import Optional

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

GENESIS_PORT = 3070
GENESIS_IP = "localhost"

GOSSIP_COUNT = 2
GOSSIP_RATE = 15


class Genesis:
    GOSSIP_COUNT = 3
    RETRY_DELAY = 5

    def __init__(self, port=GENESIS_PORT):
        self.port = port
        self.host = "localhost"
        self.peer_manager = PeerManager()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()

        logging.debug(f"Genesis server started on {self.host}:{self.port}")

        while True:
            client, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client,)).start()

    def handle_client(self, client: socket.socket):
        while True:
            try:
                data = client.recv(1024).decode("utf-8")
                if not data:
                    break
                self.process_message(data, client)

            except ConnectionResetError:
                break

    def process_message(self, data: str, client: socket.socket):
        try:
            message: dict = json.loads(data)

            if message["type"] == "request_peers":
                peer_addr = message["address"]

                self.peer_manager.add_peer(peer_addr)

                peers = self.peer_manager.get_peers(self.GOSSIP_COUNT, peer_addr)

                while len(peers) < self.GOSSIP_COUNT:
                    logging.warning(f"Insufficient peers in database, {len(peers)} peers so far: {peers}")
                    time.sleep(5)
                    peers = self.peer_manager.get_peers(self.GOSSIP_COUNT, peer_addr)

                peers_message = {
                    "type": "from_genesis",
                    "peers": peers
                }

                logging.debug(f"Genesis is sending to {peer_addr[0]}:{peer_addr[1]}: {json.dumps(peers_message)}")
                client.sendall(json.dumps(peers_message).encode())

        except Exception as e:
            logging.critical(f"Error processing message: {e}")


class Peer:
    def __init__(self, port):
        self.port = port
        self.host = "localhost"
        self.peer_manager = PeerManager()

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.host, self.port))
        server.listen()

        logging.debug(f"Peer started on {self.host}:{self.port}")

        threading.Thread(target=self.gossip, daemon=True).start()

        while True:
            client, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client,)).start()

    def handle_client(self, client: socket.socket):
        while True:
            try:
                data = client.recv(1024).decode("utf-8")
                if not data:
                    break
                self.process_message(data)

            except ConnectionResetError:
                break

    def process_message(self, data: str):
        try:
            message: dict = json.loads(data)

            if message["type"] == "send_peers":
                peer_addr_from_sender = message["address"]
                self.peer_manager.add_peer(peer_addr_from_sender)
                for peer_addr in message["peers"]:
                    self.peer_manager.add_peer(peer_addr)

            elif message["type"] == "from_genesis":
                for peer_addr in message["peers"]:
                    self.peer_manager.add_peer(peer_addr)

        except Exception as e:
            logging.critical(f"Error processing message: {e}")

    def gossip(self):
        while True:
            peer_addresses = self.peer_manager.get_peers(GOSSIP_COUNT, None)
            if not peer_addresses:
                logging.debug("Not enough peers in database, connecting to genesis")
                self.connect_to_genesis()
            else:
                for peer_addr in peer_addresses:
                    self.gossip_with_peer(peer_addr)
            time.sleep(GOSSIP_RATE)

    def connect_to_genesis(self):
        try:
            with socket.create_connection((GENESIS_IP, GENESIS_PORT), timeout=60) as server:
                message = {"type": "request_peers", "address": [server.getsockname()[0], self.port]}

                logging.debug(f"Sending to genesis: {json.dumps(message).encode()}")
                server.sendall(json.dumps(message).encode())

                response = server.recv(1024).decode('utf-8')
                logging.debug(f"Received from genesis: {response}")

                self.process_message(response)

        except Exception as e:
            logging.critical(f"Failed to connect to genesis node: {e}")

    def gossip_with_peer(self, peer_addr: tuple[str, int]):
        logging.debug(f"Started gossipping with peer: {peer_addr[0]}:{peer_addr[1]}")
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.connect((peer_addr[0], peer_addr[1]))
            peers = self.peer_manager.get_peers(2, peer_addr)
            message = {"type": "send_peers", "peers": peers, "address": [server.getsockname()[0], self.port]}

            logging.debug(f"Sending to peer {peer_addr[0]}:{peer_addr[1]}: {message}")
            server.sendall(json.dumps(message).encode())

        except Exception as e:
            logging.critical(f"Error gossiping with peer {peer_addr}: {e}")


class PeerManager:
    def __init__(self):
        self.conn = self.create_db()
        self.create_table()

    @staticmethod
    def create_db() -> sqlite3.Connection:
        current_folder = os.path.dirname(os.path.abspath(__file__))
        database_path = os.path.join(current_folder, "blockchain.db")
        logging.debug(f"Created database at: {database_path}")
        return sqlite3.connect(database_path, check_same_thread=False)

    def create_table(self):
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS peer (
                id INTEGER PRIMARY KEY autoincrement,
                ip TEXT NOT NULL,
                port INTEGER NOT NULL,
                UNIQUE(ip, port)
            )
            """)

    def add_peer(self, peer_addr: tuple[str, int]):
        try:
            with self.conn:
                cursor = self.conn.execute("INSERT OR IGNORE INTO peer (ip, port) VALUES (?, ?)", peer_addr)
                if cursor.rowcount > 0:
                    logging.debug(f"Peer {peer_addr[0]}:{peer_addr[1]} was inserted successfully to the database")

        except sqlite3.Error as e:
            logging.critical(f"Error adding peer: {e}")

    def get_peers(self, count: int, user_addr: Optional[tuple[str, int]]) -> list[tuple[str, int]]:
        with self.conn:
            query = "SELECT ip, port FROM peer WHERE (ip != ? OR port != ?) ORDER BY RANDOM() LIMIT ?"
            user_ip, user_port = user_addr or ("", 0)
            result = self.conn.execute(query, (user_ip, user_port, count)).fetchall()
            return result
