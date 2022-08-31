"""Middleware to communicate with PubSub Message Broker."""
from collections.abc import Callable
from enum import Enum
import json
import pickle
from queue import LifoQueue, Empty
import socket
from typing import Any

import xml.etree.ElementTree as XML


class MiddlewareType(Enum):
    """Middleware Type."""

    CONSUMER = 1
    PRODUCER = 2


class Queue:
    """Representation of Queue interface for both Consumers and Producers."""

    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        """Create Queue."""
        self.type = _type
        self.topic = topic

        self.broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.broker.connect(("localhost", 5000))

    def push(self, value):
        """Sends data to broker."""
        self.broker.send(len(value).to_bytes(2, "big") + value)

    def pull(self) -> tuple((str, Any)):
        """Receives (topic, data) from broker.
        Should BLOCK the consumer!"""
        header = int.from_bytes(self.broker.recv(2), "big")
        body = self.broker.recv(header)
        if header == 0: 
            return
        return body, None

    def list_topics(self, callback: Callable):
        """Lists all topics available in the broker."""
        dic = {"method" : "list_topics"}
        self.broker.send(len(dic).to_bytes(2, "big") + dic)

    def cancel(self):
        """Cancel subscription."""
        dic = {"method" : "cancel", "topic" : self.topic }
        self.broker.send(len(dic).to_bytes(2, "big") + dic)


class JSONQueue(Queue):
    """Queue implementation with JSON based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        for i in range (2):
            super().__init__(topic, _type)
            msg = pickle.dumps({"method": "ACK", "format": "JSON"})
            self.broker.send(len(msg).to_bytes(2, "big") + msg)

            if _type == MiddlewareType.CONSUMER:
                encodedMsg = json.dumps({"method": "subscribe", "topic": topic }).encode("utf-8")
                self.broker.send(len(encodedMsg).to_bytes(2, "big") + encodedMsg)
            break

    def push(self, value):
        for i in range (2):
            encoded_msg = json.dumps({"method": "publish", "topic": self.topic, "msg": value}).encode("utf-8")
            self.broker.send(len(encoded_msg).to_bytes(2, "big") + encoded_msg)
            break
    
    def pull(self) -> tuple((str, Any)):
        for i in range (2):
            header = int.from_bytes(self.broker.recv(2), "big")
            body = self.broker.recv(header)
            if header == 0: 
                return
            (msg, x) = body, None
            return json.loads(msg)["topic"], json.loads(msg)["msg"]


class XMLQueue(Queue):
    """Queue implementation with XML based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        for i in range (2):
            super().__init__(topic, _type)

            msg = pickle.dumps({"method": "ACK", "format": "XML"})
            self.broker.send(len(msg).to_bytes(2, "big") + msg)

            if _type == MiddlewareType.CONSUMER:
                root = XML.Element('root')
                XML.SubElement(root, 'method').set("value", "subscribe")
                XML.SubElement(root, 'topic').set("value", str(topic))
                encoded_msg = XML.tostring(root)
                self.broker.send(len(encoded_msg).to_bytes(2, "big") + encoded_msg)
            break

    def push(self, value):
        for i in range (2):
            root = XML.Element('root')
            XML.SubElement(root, 'method').set("value", "publish")
            XML.SubElement(root, 'topic').set("value", self.topic)
            XML.SubElement(root, 'msg').set("value", str(value))

            msg = XML.tostring(root)
            self.broker.send(len(msg).to_bytes(2, "big") + msg)
            break
    
    def pull(self) -> tuple((str, Any)):
        for i in range (2):
            header = int.from_bytes(self.broker.recv(2), "big")
            body = self.broker.recv(header)
            if header == 0: 
                return
            (msg, x) = body, None

            decodeMsg = {}
            for branch in XML.fromstring(msg):
                decodeMsg[branch.tag] = branch.attrib["value"]

            return decodeMsg["topic"], decodeMsg["msg"]

class PickleQueue(Queue):
    """Queue implementation with Pickle based serialization."""
    def __init__(self, topic, _type=MiddlewareType.CONSUMER):
        for i in range (2):
            super().__init__(topic, _type)

            msg = pickle.dumps({"method": "ACK", "format": "PICKLE"})
            self.broker.send(len(msg).to_bytes(2, "big") + msg)

            if _type == MiddlewareType.CONSUMER:
                encodedMsg = pickle.dumps({"method": "subscribe", "topic": topic })
                self.broker.send(len(encodedMsg).to_bytes(2, "big") + encodedMsg)
            break

    def push(self, value):
        for i in range (2):
            encoded_msg = pickle.dumps({"method": "publish", "topic": self.topic, "msg": value})
            self.broker.send(len(encoded_msg).to_bytes(2, "big") + encoded_msg)
            break
    
    def pull(self) -> tuple((str, Any)):
        for i in range (2):
            header = int.from_bytes(self.broker.recv(2), "big")
            body = self.broker.recv(header)
            if header == 0: 
                return
            (msg, x) = body, None

            return pickle.loads(msg)["topic"], pickle.loads(msg)["msg"]