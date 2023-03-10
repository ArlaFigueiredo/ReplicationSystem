import asyncio
import socket
import pickle
import logging
import argparse

from datetime import datetime

from message_params import MessageType, SenderTypes
from config import settings
from database.db import execute_sql

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

        self.alive_checker = {}
        self.leader_alive_checker = None

        self.last_execute = datetime.now()

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
        self.socket.listen(30)

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
            print(f"[DataBase {self.host}:{self.port}] L??der recebeu o comando SQL e ir?? enviar aos demais.")
            self.queue_commands.append(message["content"])

    def upgrade_to_leader(self):

        print(f"[DataBase {self.host}:{self.port}] Upgrade para l??der.")

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
            print(f"[DataBase {self.host}: {self.port}] Recebido confirma????o de recebimento de {message['addr']}")
            self.message_stage["number_confirms"] += 1

        if message["type"] == MessageType.HEART_BEAT:

            print(f"Recebendo um heart-beat de {message['addr']}")

            if self.is_leader:
                try:
                    self.alive_checker.pop(message["addr"])
                except KeyError:
                    pass
            else:
                self.leader_alive_checker = None

        if message["type"] == MessageType.STAGE:
            msg = {
                "sender": SenderTypes.SERVER_DBM,
                "type": MessageType.CONFIRM,
                "addr": (self.host, self.port),
                "content": "I am received the message."
            }
            self.message_stage = message["content"]

            print(f'[DataBase {self.host}: {self.port}] Comando {self.message_stage} escrito e enviando '
                  f'confirma????o de recebimento para l??der.')

            self.send(msg=msg, addr=self.leader_addr)

        if message["type"] == MessageType.COMMIT:
            execute_sql(idf=self.idf, sql_raw=self.message_stage)
            self.message_stage = None
            print(f"[DataBase {self.host}: {self.port}] Commitando ...")

        if message["type"] == MessageType.LEADER_ANNOUNCE:
            self.leader_addr = message["addr"]
            print(f"[DataBase {self.host}: {self.port}] Aceitando novo l??der {self.leader_addr}")

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

    async def request_group_add(self):
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

    async def coordinate(self):
        """

        :return:
        """
        while True:

            if not self.is_leader:
                await asyncio.sleep(3)
                continue

            if self.message_stage:
                if self.message_stage["number_confirms"] >= len(self.members.keys()) - 1:
                    print(f"[DataBase {self.host}: {self.port}] Confirma????o recebida por todos os membros, "
                          f"irei mandar o commit...")
                    msg = {
                        "sender": SenderTypes.SERVER_DBM,
                        "type": MessageType.COMMIT,
                        "content": self.message_stage["command"]
                    }
                    self.send_to_all_members(msg=msg)
                    execute_sql(idf=self.idf, sql_raw=self.message_stage["command"])
                    self.message_stage = None
                else:
                    continue

            if not self.message_stage and self.queue_commands:
                print(f"[DataBase {self.host}: {self.port}] Enviando novo comando para todos os membros...")
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

            await asyncio.sleep(1)

    def __start_election(self):
        """

        :return:
        """
        raise NotImplementedError

    def __remove_member(self, idf_member):
        """

        :param idf_member:
        :return:
        """

        self.members.pop(idf_member)

    async def heart_beat(self):
        """

        :return:
        """
        while True:
            print("Heart Beat")
            if (datetime.now() - self.last_execute).seconds < 10:
                return

            self.last_execute = datetime.now()

            msg = {
                "sender": SenderTypes.SERVER_DBM,
                "type": MessageType.HEART_BEAT,
                "addr": (self.host, self.port),
                "content": "Are you alive?",
            }

            if self.is_leader:
                print("[heart_beat] is leader")
                for idf, member_addr in self.members.items():
                    print(f"[heart_beat] enviando para {member_addr}")
                    if idf == self.idf:
                        continue
                    try:
                        self.send(msg=msg, addr=member_addr)
                    except ConnectionRefusedError:
                        print(f"Membro cujo ID ?? {idf} est?? off, vou remove-lo do grupo")
                        self.__remove_member(idf)

            else:
                print("[heart_beat] is replica")
                try:
                    print(f"[heart_beat] enviando para lider")
                    self.send(msg=msg, addr=self.leader_addr)
                except ConnectionRefusedError:
                    print("O l??der est?? off, vou iniciar uma elei????o.")
                    self.__start_election()

            await asyncio.sleep(2)

    async def listen_connections(self):
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
            await asyncio.sleep(1)


async def start():
    # Initialize parser
    parser = argparse.ArgumentParser()

    # Adding arguments
    parser.add_argument("-a", dest="host", help="Host of Node")

    parser.add_argument("-p", dest="port", help="Port of Node")

    # Read arguments from command line
    args = parser.parse_args()

    server = Node(host=args.host, port=int(args.port))

    task_list = list()

    task_list.append(asyncio.create_task(server.request_group_add()))
    task_list.append(asyncio.create_task(server.listen_connections()))
    task_list.append(asyncio.create_task(server.coordinate()))
    task_list.append(asyncio.create_task(server.heart_beat()))

    await asyncio.gather(*task_list)

asyncio.run(start())
