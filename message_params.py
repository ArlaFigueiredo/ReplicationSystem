from enum import Enum


class MessageType(Enum):
    SQL_COMMAND = 1
    STAGE = 2
    CONFIRM = 3
    COMMIT = 4
    LEADER_ELECTION = 5
    HEART_BEAT = 6
    REQUEST = 7


class SenderTypes(Enum):
    SERVER_RM = 1
    SERVER_DBM = 2
    CLIENT = 3
