"""Message Broker"""
from base64 import decode
import enum
import json
import pickle
import socket
import selectors
import xml.etree.ElementTree as XML
from typing import Dict, List, Any, Tuple

class Serializer(enum.Enum):
    """Possible message serializers."""

    JSON = 0
    XML = 1
    PICKLE = 2

class Broker:
    """Implementation of a PubSub Message Broker."""

    def __init__(self):
        """Initialize broker."""
        self.canceled = False

        self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.broker.bind(('localhost', 5000))
        self.broker.listen()
        self.sel = selectors.DefaultSelector()
        self.sel.register(self.broker, selectors.EVENT_READ, self.accept)

        self.topicMessages={}
        self.topicConsumers={}
        self.consumersInfo={}

    def accept(self, sock, mask):
        conn, mask = sock.accept()
        self.sel.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn:socket.socket, mask):
        header = int.from_bytes(conn.recv(2), "big")
        msg = conn.recv(header)
        
        if header == 0:
            for topic in self.topicConsumers:
                self.unsubscribe(topic, conn)
            self.sel.unregister(conn)
            conn.close()
            return
        if conn in self.consumersInfo:
            format = self.consumersInfo[conn]
            dict_cond={
                "XML" : format == Serializer.XML,
                "PICKLE" : format == Serializer.PICKLE,
                "JSON" : format == Serializer.JSON
            }
            if dict_cond["XML"]:
                tree = XML.fromstring(msg)
                decodeMsg = {}
                for branch in tree:
                    decodeMsg[branch.tag] = branch.attrib["value"]
            elif dict_cond["PICKLE"]:
                decodeMsg = pickle.loads(msg)
            elif dict_cond["JSON"]:
                decodeMsg = json.loads(msg)
            else:
                return None
        else:
            decodeMsg = pickle.loads(msg)
            
        if decodeMsg == None:
            conn.close()
        else:
            method = decodeMsg["method"]
            if method == "ACK":
                format = decodeMsg["format"]
                format_cond={
                    "XML": format == "XML",
                    "PICKLE": format == "PICKLE",
                    "JSON": format == "JSON"
                }
                
                if format_cond["XML"]:
                    self.consumersInfo[conn] = Serializer.XML
                elif format_cond["PICKLE"]:
                    self.consumersInfo[conn] = Serializer.PICKLE
                else:
                    self.consumersInfo[conn] = Serializer.JSON

            elif method == "subscribe":
                self.subscribe(decodeMsg["topic"], conn, self.consumersInfo[conn])

            elif method == "publish":
                topic = decodeMsg["topic"]
                msg = decodeMsg["msg"]
                self.topicMessages[topic] = msg

                for t in self.topicConsumers:
                    if t in topic:
                        dic = {"method": "publish","topic": topic, "msg": msg}
                        for consumer in self.topicConsumers[t]:
                            conn = consumer[0]
                            f = consumer[1]
                            self.send(conn, dic, f) 

            elif method == "list_topics":
                self.send(conn, {"method": "list_topics", "topic":  None, "msg": self.list_topics()}, self.consumersInfo[conn])

            elif method == "cancel":
                self.topicMessages[decodeMsg["topic"]].remove(conn)
            
    def list_topics(self) -> List[str]:
        """Returns a list of strings containing all topics containing values."""
        return list(self.topicMessages.keys())

    def get_topic(self, topic):
        """Returns the currently stored value in topic."""
        if topic in self.topicMessages:
            return self.topicMessages[topic]
        return None

    def put_topic(self, topic, value):
        """Store in topic the value."""
        self.topicMessages[topic]=value

    def list_subscriptions(self, topic: str) -> List[Tuple[socket.socket, Serializer]]:
        """Provide list of subscribers to a given topic."""
        return self.topicConsumers[topic]

    def subscribe(self, topic: str, address: socket.socket, _format: Serializer = None):
        """Subscribe to topic by client in address."""
        consumer=tuple((address, _format))
        if consumer not in self.consumersInfo:
            self.consumersInfo[consumer]=_format

        if topic not in self.topicConsumers:
            self.topicConsumers[topic] = []
        self.topicConsumers[topic].append(consumer)

        if topic in self.topicMessages:
            msg = {"method": "subscribe", "topic": topic, "msg": self.topicMessages[topic]}
            self.send(address, msg, _format)

    def unsubscribe(self, topic, address):
        """Unsubscribe to topic by client in address."""
        for con in self.topicConsumers[topic]:
            if con[0]==address:
                self.topicConsumers[topic].remove(con)

    def send(self, address: socket.socket, msg,_format):
        if _format == Serializer.XML:
            root = XML.Element('root')
            for key in msg:
                XML.SubElement(root, str(key)).set("value", str(msg[key]))
            encodedMsg = XML.tostring(root)
        elif _format == Serializer.PICKLE:
            encodedMsg = pickle.dumps(msg)
        else:
            encodedMsg = json.dumps(msg).encode("utf-8")
        address.send(len(encodedMsg).to_bytes(2, "big") + encodedMsg)

    def run(self):
        """Run until canceled."""
        while not self.canceled:
            events = self.sel.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)
