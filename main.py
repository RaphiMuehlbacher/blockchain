from ecdsa import SigningKey

from node import Node


def main():

    peer = Node(8082, SigningKey.generate())
    peer.start()


if __name__ == "__main__":
    main()

