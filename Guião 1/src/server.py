"""CD Chat server program."""
import logging
import selectors
import socket
from .protocol import CDProto, JoinMessage, TextMessage
logging.basicConfig(filename="server.log", level=logging.DEBUG)


class Server:
    """Chat Server process."""
    def __init__(self):
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('localhost', 1236))
        self.sock.listen()
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.sock, selectors.EVENT_READ, self.accept)
        self.users = {"Initial" : [] }

    def loop(self):
        """Loop indefinetely."""
        while True:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

    def accept(self, sock, mask):
        conn, mask = sock.accept()
        conn.setblocking(False)
        d=CDProto.recv_msg(conn)
        self.users["Initial"].append(conn)
        self.sel.register(conn, selectors.EVENT_READ, self.read)
        logging.debug('received "%s"', str(d))

    def read(self, conn, mask):
        d=CDProto.recv_msg(conn)
        logging.debug('received "%s"', str(d))
        if d:
            if isinstance(d,JoinMessage) :
                self.users.setdefault(d.channel, [] ) 
                for key in self.users.keys():
                    if conn in self.users[key]:
                        self.users[key].remove(conn)
                self.users[d.channel].append(conn) 

            elif isinstance(d,TextMessage):
                if d.channel is None:
                    for client in self.users["Initial"]:
                        CDProto.send_msg(client, d) 
                        logging.debug('sent "%s"', str(d))
                else:
                    for client in self.users[d.channel]:
                        CDProto.send_msg(client, d) 
                        logging.debug('sent "%s"', str(d))
        else:
            for key in self.users.keys():
                if conn in self.users[key]:
                    self.users[key].remove(conn)
            self.sel.unregister(conn)
            conn.close()