import pickle
import socket
import asyncio
from datetime import datetime

from message_params import MessageType, SenderTypes
from config import settings


class ReplicationManagerServer:
    def __init__(self):
        self.port = settings.server.PORT
        self.host = settings.server.HOST
        self.max_connections = 30

        self.socket = socket.socket()
        self.socket_dbm = None

        self.db_managers = {}
        self.leader_addr = None
        self.counter = 0

        self.last_execute = datetime.now()

    def __include_dbm_member(self, host: str, port: int):
        """

        :param host:
        :param port:
        :return:
        """
        self.db_managers[self.counter] = (host, port)

    def __treat_dbm_message(self, message: dict):
        """

        :param message:
        :return:
        """
        if message["type"] == MessageType.GROUP_REQUEST:
            print(f"[Server RM] Requisição de entrada no grupo de {message['addr']}")
            self.__include_dbm_member(host=message["addr"][0], port=message["addr"][1])
            msg = {
                "sender": SenderTypes.SERVER_RM,
                "receiver": message["addr"],
                "leader_addr": self.leader_addr,
                "identifier": self.counter,
                "type": MessageType.CONFIRM,
                "content": self.db_managers
            }
            self.__increment_counter()
            self.send_to_all_dbm(msg=msg)

        if message["type"] == MessageType.LEADER_ANNOUNCE:
            print("[Server RM] Novo líder do grupo foi elegido.")
            self.leader_addr = message["addr"]

    def __increment_counter(self):
        self.counter += 1

    def __start_connection(self, addr):
        """
        Start connection with RM
        :return:
        """
        self.socket_dbm = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_dbm.connect(addr)

    def __close_connection(self):
        """
        Closes connection with RM
        :return:
        """
        self.socket_dbm.close()

    def send(self, msg: dict, addr):
        """
        Send SQL command to RM server
        :param msg: msg
        :param addr: addr
        :return:
        """

        self.__start_connection(addr)
        self.socket_dbm.send(pickle.dumps(msg))
        self.__close_connection()

    def send_to_all_dbm(self, msg):
        """

        :param msg:
        :return:
        """
        for idf, addr in self.db_managers.items():
            self.send(msg, addr)

    def __bind(self):
        """
        Bind socket connection
        :return:
        """
        self.socket.bind((self.host, self.port))
        self.socket.listen(self.max_connections)

    async def heart_beat(self):
        """

        :return:
        """
        while True:
            if (datetime.now() - self.last_execute).seconds < 10:
                return

            self.last_execute = datetime.now()

            msg = {
                "sender": SenderTypes.SERVER_DBM,
                "type": MessageType.HEART_BEAT,
                "addr": (self.host, self.port),
                "content": "Are you alive?",
            }

            for idf, addr in self.db_managers.items():

                try:
                    self.send(msg, addr)
                except ConnectionRefusedError:
                    print(f"O nó {addr} tá OFF.")

    def listen_connections(self):
        """
        Listen external connections
        :return:
        """
        self.__bind()

        print("[Server RM] Escutando conexões")
        while True:
            conn, addr = self.socket.accept()

            encoded_message = conn.recv(1000)
            if len(encoded_message) == 0:
                pass
            else:
                print("[Server RM] Nova mensagem recebida.")
                message = pickle.loads(encoded_message)
                if message["sender"] == SenderTypes.CLIENT:
                    msg = {
                        "sender": SenderTypes.SERVER_RM,
                        "type": MessageType.SQL_COMMAND,
                        "content": message['content']
                    }
                    self.send_to_all_dbm(msg)
                if message["sender"] == SenderTypes.SERVER_DBM:
                    self.__treat_dbm_message(message)

            # Encerra Conexão com o Cliente
            conn.close()


def start():
    server = ReplicationManagerServer()
    task_list = list()

    task_list.append(asyncio.create_task(server.listen_connections()))
    task_list.append(asyncio.create_task(server.heart_beat()))

    await asyncio.gather(*task_list)


asyncio.run(start())
