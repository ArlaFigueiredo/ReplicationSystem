import socket
import pickle
import logging

from multiprocessing import Process
from message_params import MessageType, SenderTypes
from config import settings

# Create a logger
logger = logging.getLogger(__name__)


class Node:
    def __init__(self, host, port):
        self.idf = None
        self.host = host
        self.port = port

        self.socket = socket.socket()
        self.socket_members = None

        self.members = {}
        self.is_leader = False
        self.leader_addr = None

        self.queue_commands = []
        self.message_stage = None

    def __start_connection(self, addr):
        """
        Start connection
        :return:
        """
        self.socket_members = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_members.connect(addr)

    def __close_connection(self):
        """
        Closes connection
        :return:
        """
        self.socket_members.close()

    def __bind(self):
        """
        Bind socket connection
        :return:
        """
        self.socket.bind((self.host, self.port))
        self.socket.listen(0)

    def __treat_rm_message(self, message: dict):
        """

        :param message:
        :return:
        """
        if message["type"] == MessageType.CONFIRM:
            self.members = message["content"]
            self.leader_addr = message["leader_addr"]

            if message["receiver"][0] == self.host and message["receiver"][1] == self.port:
                self.idf = message["identifier"]
                print(f"[DataBase {self.host}:{self.port}] Adicionado ao grupo com sucesso com o "
                            f"identificador {self.idf}")

                if len(self.members.keys()) == 1:
                    self.upgrade_to_leader()

        if message["type"] == MessageType.SQL_COMMAND and self.is_leader:
            print(f"[DataBase {self.host}:{self.port}] Líder recebeu o comando SQL e irá enviar aos demais.")
            self.queue_commands.append(message["content"])

    def upgrade_to_leader(self):

        print(f"[DataBase {self.host}:{self.port}] Upgrade para líder.")

        self.is_leader = True
        self.leader_addr = (self.host, self.port)

        msg = {
            "sender": SenderTypes.SERVER_DBM,
            "type": MessageType.LEADER_ANNOUNCE,
            "addr": (self.host, self.port),
            "content": "I'm the new coordinator."
        }

        # Send message to rm  and members
        self.send(msg=msg, addr=(settings.server.HOST, settings.server.PORT))
        self.send_to_all_members(msg=msg)

    def __treat_dbm_message(self, message: dict):

        if message["type"] == MessageType.CONFIRM:
            print(f"[DataBase {self.host}: {self.port}] Recebido confirmação de recebimento de {message['addr']}")
            self.message_stage["number_confirms"] += 1

        if message["type"] == MessageType.HEART_BEAT:
            # Tratar resposta do PING (HEART BEAT)
            pass

        if message["type"] == MessageType.STAGE:
            msg = {
                "sender": SenderTypes.SERVER_DBM,
                "type": MessageType.CONFIRM,
                "addr": (self.host, self.port),
                "content": "I am received the message."
            }
            self.message_stage = message["content"]

            print(f'[DataBase {self.host}: {self.port}] Comando {self.message_stage} escrito e enviando '
                        'confirmação de recebimento para líder.')
            self.send(msg=msg, addr=self.leader_addr)

        if message["type"] == MessageType.COMMIT:
            # TODO: Lógica para commitar a mensagem
            self.message_stage = None
            print(f"[DataBase {self.host}: {self.port}] Commitando ...")

        if message["type"] == MessageType.LEADER_ANNOUNCE:
            self.leader_addr = message["addr"]
            print(f"[DataBase {self.host}: {self.port}] Aceitando novo líder {self.leader_addr}")

    def send(self, msg: dict, addr):
        """
        Send SQL command to others server
        :param msg: msg
        :param addr: addr
        :return:
        """
        self.__start_connection(addr)
        self.socket_members.send(pickle.dumps(msg))
        self.__close_connection()

    def request_group_add(self):
        msg = {
            "sender": SenderTypes.SERVER_DBM,
            "type": MessageType.GROUP_REQUEST,
            "addr": (self.host, self.port),
            "content": "I want join to the group."
        }
        self.send(msg=msg, addr=(settings.server.HOST, settings.server.PORT))

    def send_to_all_members(self, msg):
        """

        :param msg:
        :return:
        """
        for idf, (host, port) in self.members.items():
            if port == self.port:
                continue
            self.send(msg, (host, port))

    def coordinate(self):
        """

        :return:
        """
        while True:
            if not self.is_leader or not self.queue_commands:
                continue

            if self.message_stage:
                if self.message_stage["number_confirms"] == len(self.members) - 1:
                    msg = {
                        "sender": SenderTypes.SERVER_DBM,
                        "type": MessageType.COMMIT,
                        "content": self.message_stage["command"]
                    }
                    self.message_stage = None
                else:
                    continue
            else:
                print(f"[DataBase {self.host}: {self.port}] Enviando novo comando para todos os membros")
                self.message_stage = {
                    "command": self.queue_commands.pop(),
                    "number_confirms": 0
                }
                msg = {
                    "sender": SenderTypes.SERVER_DBM,
                    "type": MessageType.STAGE,
                    "content": self.message_stage["command"]
                }

            self.send_to_all_members(msg=msg)

    def heart_beat(self):
        pass

    def listen_connections(self):
        """
        Listen external connections
        :return:
        """
        self.__bind()

        while True:
            conn, addr = self.socket.accept()

            encoded_message = conn.recv(1000)
            if len(encoded_message) == 0:
                pass
            else:
                message = pickle.loads(encoded_message)

                if message["sender"] == SenderTypes.SERVER_RM:
                    self.__treat_rm_message(message)

                if message["sender"] == SenderTypes.SERVER_DBM:
                    self.__treat_dbm_message(message)

            conn.close()


server = Node(host="localhost", port=8946)

process_list = list()

process_list.append(Process(target=server.request_group_add()))
process_list.append(Process(target=server.listen_connections()))
process_list.append(Process(target=server.coordinate()))

for t in process_list:
    t.start()

for t in process_list:
    t.join()
