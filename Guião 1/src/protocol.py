"""Protocol for chat server - Computação Distribuida Assignment 1."""
import json
from datetime import datetime
from socket import socket


class Message:
    """Message Type."""
    def __init__(self, command):
        self.command=command

    def __repr__(self):
        return json.dumps({"command" : self.command })
    
class JoinMessage(Message):
    """Message to join a chat channel."""
    def __init__(self, channel):
        super().__init__("join")
        self.channel=channel

    def __repr__(self):
        return json.dumps({"command" : self.command , "channel" : self.channel})

class RegisterMessage(Message):
    """Message to register username in the server."""
    def __init__(self, user):
        super().__init__("register")
        self.user=user
    def __repr__(self):
        return json.dumps({"command" : self.command , "user" : self.user})
    
class TextMessage(Message):
    """Message to chat with other clients."""
    def __init__(self, message, channel = None, ts = None):
        super().__init__("message")
        self.message=message
        self.channel=channel
        if ts is None:
            self.ts = int(datetime.now().timestamp())
        else:
            self.ts=ts

    def __repr__(self):
        if self.channel is None:
            return json.dumps({"command" : self.command , "message" : self.message , "ts": self.ts})
        else:
            return json.dumps({"command" : self.command , "message" : self.message , "channel" : self.channel , "ts": self.ts})


class CDProto:
    """Computação Distribuida Protocol."""

    @classmethod
    def register(cls, username: str) -> RegisterMessage:
        """Creates a RegisterMessage object."""
        return RegisterMessage(username)

    @classmethod
    def join(cls, channel: str) -> JoinMessage:
        """Creates a JoinMessage object."""
        return JoinMessage(channel)

    @classmethod
    def message(cls, message: str, channel: str = None) -> TextMessage:
        """Creates a TextMessage object."""
        return TextMessage(message, channel)

    @classmethod
    def send_msg(cls, connection: socket, msg: Message):
        """Sends through a connection a Message object."""
        st=str(msg)
        connection.send(len(st).to_bytes(2,"big")+st.encode("utf-8"))

    @classmethod
    def recv_msg(cls, connection: socket) -> Message:
        """Receives through a connection a Message object."""
        s=int.from_bytes(connection.recv(2), "big")

        if s == 0:
            return None
            
        original=connection.recv(s)
        msg=original.decode("utf-8")

        try:
            msg=json.loads(msg)
        except:
            raise CDProtoBadFormat(original)

        if "command" not in msg.keys():
            raise CDProtoBadFormat(original)

        case=msg["command"]
        if case == "message":
            if "message" not in msg.keys():
                raise CDProtoBadFormat(original)
            if "ts" not in msg.keys():
                raise CDProtoBadFormat(original)
            if "channel" not in msg.keys():
                return TextMessage(msg["message"], ts=int(msg["ts"]))
            else:
                return TextMessage(msg["message"], msg["channel"], int(msg["ts"]))
        elif case == "join" :
            if "channel" not in msg.keys():
                raise CDProtoBadFormat(original)
            return JoinMessage(msg["channel"])
        elif case == "register":
            if "user" not in msg.keys():
                raise CDProtoBadFormat(original)
            return RegisterMessage(msg["user"])   




class CDProtoBadFormat(Exception):
    """Exception when source message is not CDProto."""

    def __init__(self, original_msg: bytes=None) :
        """Store original message that triggered exception."""
        self._original = original_msg

    @property
    def original_msg(self) -> str:
        """Retrieve original message as a string."""
        return self._original.decode("utf-8")
