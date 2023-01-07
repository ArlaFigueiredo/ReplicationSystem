from enum import Enum


class MessageType(Enum):

    GROUP_REQUEST = 1
    SQL_COMMAND = 2

    STAGE = 3
    CONFIRM = 4
    COMMIT = 5

    HEART_BEAT = 6

    LEADER_ELECTION = 7
    LEADER_ANNOUNCE = 8


class SenderTypes(Enum):
    SERVER_RM = 1
    SERVER_DBM = 2
    CLIENT = 3
