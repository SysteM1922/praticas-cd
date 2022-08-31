"""CD Chat client program"""
import fcntl
import logging
import os
import selectors
import socket
import sys

from .protocol import CDProto, RegisterMessage, TextMessage, JoinMessage

logging.basicConfig(filename=f"{sys.argv[0]}.log", level=logging.DEBUG)

class Client:
    """Chat Client process."""

    def __init__(self, name: str = "Foo"):
        """Initializes chat client."""
        self.name=name
        self.channel=None
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        orig_fl = fcntl.fcntl(sys.stdin, fcntl.F_GETFL)
        fcntl.fcntl(sys.stdin, fcntl.F_SETFL, orig_fl | os.O_NONBLOCK)

        self.m_selector = selectors.DefaultSelector()
        self.m_selector.register(sys.stdin, selectors.EVENT_READ, self.enterInfo)
        self.m_selector.register(self.sock, selectors.EVENT_READ,self.read)
        pass

    def connect(self):
        """Connect to chat server and setup stdin flags."""
        self.sock.connect(("localhost", 1236))
        CDProto.send_msg(self.sock, RegisterMessage(self.name))
        logging.debug('sent register')
        pass

    def loop(self):
        """Loop indefinetely."""
        while True: 
            for k, mask in self.m_selector.select():
                callback = k.data
                callback(k.fileobj)

    def enterInfo(self, stdin):
        msg = str(stdin.read())
        if msg.startswith("/join"): 
            msg = msg.replace("/join", "")
            self.channel=msg
            msg = JoinMessage(msg)
            CDProto.send_msg(self.sock, msg)
            logging.debug('sent /join"%s"',str(msg))
        elif msg.rstrip()=="exit":
            self.sock.close()
            quit()
        else:
            msg = TextMessage(msg,self.channel)
            CDProto.send_msg(self.sock, msg)
            logging.debug('sent "%s"', str(msg))

    def read(self, conn):
        d = CDProto.recv_msg(conn)
        if isinstance(d, TextMessage):
            print(d.message)
            logging.debug('received "%s"', str(d))
