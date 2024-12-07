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
MAX_PEERS = 10


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
        threading.Thread(target=self.start_health_check, daemon=True).start()

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

    def start_health_check(self):
        logging.debug("Starting health check")
        while True:
            self.health_check()
            time.sleep(30)

    def health_check(self):
        logging.debug(f"Performing health check")
        try:
            for peer_addr in self.peer_manager.get_all_peers():
                try:
                    with socket.create_connection(peer_addr, timeout=20) as server:
                        message = {"type": "genesis_health_check"}
                        server.sendall(json.dumps(message).encode())

                        logging.debug(f"SENDING TO {peer_addr} HEALTH CHECK")
                        response = server.recv(1024).decode('utf-8')
                        health_response: dict = json.loads(response)
                        if health_response["type"] == "genesis_health_check_response" and health_response["status"] == "healthy":
                            logging.debug(f"Received healthy status from {peer_addr[0]}:{peer_addr[1]}")
                            self.peer_manager.set_online(peer_addr)
                            continue

                        logging.warning(f"Didn't receive a healthy check response from {peer_addr[0]}:{peer_addr[1]}")
                        raise Exception("No response from peer")

                except (socket.timeout, ConnectionRefusedError, OSError) as e:
                    logging.warning(f"Peer {peer_addr[0]}:{peer_addr[1]} is offline. Setting to offline. Error: {e}")
                    self.peer_manager.set_offline(peer_addr)

        except Exception as e:
            logging.critical(f"Failed to perform health check: {e}")


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
        threading.Thread(target=self.start_health_check, daemon=True).start()

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

    def process_message(self, data: str, client: Optional[socket.socket]):
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

            elif message["type"] == "health_check":
                peer_addr = message["address"]
                logging.debug(f"Received health check from {peer_addr[0]}:{peer_addr[1]}")
                self.peer_manager.add_peer(peer_addr)

                message = {"type": "health_check_response", "status": "healthy"}
                client.sendall(json.dumps(message).encode())

            elif message["type"] == "genesis_health_check":
                logging.debug(f"Received health check from genesis")
                message = {"type": "genesis_health_check_response", "status": "healthy"}
                client.sendall(json.dumps(message).encode())

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
                message = {"type": "request_peers", "address": [self.host, self.port]}

                logging.debug(f"Sending to genesis: {json.dumps(message)}")
                server.sendall(json.dumps(message).encode())

                response = server.recv(1024).decode('utf-8')
                logging.debug(f"Received from genesis: {response}")

                self.process_message(response, None)

        except Exception as e:
            logging.critical(f"Failed to connect to genesis node: {e}")

    def gossip_with_peer(self, peer_addr: tuple[str, int]):
        logging.debug(f"Started gossiping with peer: {peer_addr[0]}:{peer_addr[1]}")
        try:
            server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server.connect((peer_addr[0], peer_addr[1]))
            peers = self.peer_manager.get_peers(2, peer_addr)
            message = {"type": "send_peers", "peers": peers, "address": [self.host, self.port]}

            logging.debug(f"Sending to peer {peer_addr[0]}:{peer_addr[1]}: {message}")
            server.sendall(json.dumps(message).encode())

        except Exception as e:
            logging.warning(f"Error gossiping with peer {peer_addr}: {e}")
            self.peer_manager.remove_peer(peer_addr)

    def start_health_check(self):
        logging.debug("Starting health check")
        while True:
            self.health_check()
            time.sleep(30)

    def health_check(self):
        logging.debug(f"Performing health check")
        try:
            for peer_addr in self.peer_manager.get_all_peers():
                try:
                    with socket.create_connection(peer_addr, timeout=20) as server:
                        message = {"type": "health_check", "address": [self.host, self.port]}
                        server.sendall(json.dumps(message).encode())

                        logging.debug(f"SENDING TO {peer_addr} HEALTH CHECK")
                        response = server.recv(1024).decode('utf-8')
                        health_response: dict = json.loads(response)
                        if health_response["type"] == "health_check_response" and health_response["status"] == "healthy":
                            logging.debug(f"Received healthy status from {peer_addr[0]}:{peer_addr[1]}")
                            continue

                        logging.warning(f"Didn't receive a healthy check response from {peer_addr[0]}:{peer_addr[1]}")
                        raise Exception("No response from peer")

                except (socket.timeout, ConnectionRefusedError, OSError) as e:
                    logging.warning(f"Peer {peer_addr[0]}:{peer_addr[1]} is offline. Removing from database. Error: {e}")
                    self.peer_manager.remove_peer(peer_addr)

        except Exception as e:
            logging.critical(f"Failed to perform health check: {e}")


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
                is_offline BOOLEAN DEFAULT FALSE,
                UNIQUE(ip, port)
            )
            """)

    def set_online(self, peer_addr: tuple[str, int]):
        with self.conn:
            self.conn.execute("UPDATE peer SET is_offline=FALSE where ip=? and port=?", peer_addr)

    def set_offline(self, peer_addr: tuple[str, int]):
        with self.conn:
            self.conn.execute("UPDATE peer SET is_offline=TRUE where ip=? and port=?", peer_addr)

    def add_peer(self, peer_addr: tuple[str, int]):
        if self.get_rows() > MAX_PEERS:
            logging.debug(f"Already {MAX_PEERS} in database. Not inserting {peer_addr[0]}:{peer_addr[1]}")
            return

        try:
            with self.conn:
                cursor = self.conn.execute("INSERT OR IGNORE INTO peer (ip, port) VALUES (?, ?)", peer_addr)
                if cursor.rowcount > 0:
                    logging.debug(f"Peer {peer_addr[0]}:{peer_addr[1]} got inserted successfully to the database")

        except sqlite3.Error as e:
            logging.critical(f"Error adding peer: {e}")

    def get_rows(self) -> int:
        with self.conn:
            return self.conn.execute("SELECT COUNT(*) FROM peer").fetchone()[0]

    def get_peers(self, count: int, user_addr: Optional[tuple[str, int]]) -> list[tuple[str, int]]:
        with self.conn:
            query = "SELECT ip, port FROM peer WHERE (ip != ? OR port != ?) and is_offline = FALSE ORDER BY RANDOM() LIMIT ?"
            user_ip, user_port = user_addr or ("", 0)
            result = self.conn.execute(query, (user_ip, user_port, count)).fetchall()
            return result

    def get_all_peers(self) -> list[tuple[str, int]]:
        with self.conn:
            return self.conn.execute("SELECT ip, port FROM peer").fetchall()

    def remove_peer(self, peer: tuple[str, int]):
        with self.conn:
            cursor = self.conn.execute("DELETE FROM peer WHERE ip=? and port=?", (peer[0], peer[1]))
            if cursor.rowcount > 0:
                logging.debug(f"Peer {peer[0]}:{peer[1]} got removed successfully from the database")
