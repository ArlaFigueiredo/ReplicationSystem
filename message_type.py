from enum import Enum


class MessageType(Enum):
    CLIENT_SQL_COMMAND = 1
    STAGE = 2
    CONFIRM = 3
    COMMIT = 4
    LEADER_ELECTION = 5
    HEART_BEAT = 6
