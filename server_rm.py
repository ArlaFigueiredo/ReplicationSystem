import socket
from message import Message
from message_type import MessageType


class ReplicationManagerServer:
    def __init__(self):
        self.port = 8900
        self.host = 'localhost'
        self.socket = socket.socket()
        self.socket_servers = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servers = [
            ("localhost", 8901),
            ("localhost", 8902),
            ("localhost", 8903),
        ]

    def __start_connection(self, addr):
        """
        Start connection with RM
        :return:
        """
        self.socket_servers.connect(addr)

    def __close_connection(self):
        """
        Closes connection with RM
        :return:
        """
        self.socket_servers.close()

    def send(self, msg: str, addr):
        """
        Send SQL command to RM server
        :param msg: msg
        :param addr: addr
        :return:
        """
        # TODO: Encode na mensagem
        msg = Message(type=MessageType.CLIENT_SQL_COMMAND, content=msg)
        self.__start_connection(addr)
        self.socket_servers.send(msg)
        self.__close_connection()

    def send_to_all_servers(self, msg):
        """

        :param msg:
        :return:
        """

        for addr_server in self.servers:
            self.send(msg, addr_server)

    def __bind(self):
        """
        Bind socket connection
        :return:
        """
        self.socket.bind((self.host, self.port))
        self.socket.listen(0)

    def listen_clients(self):
        """
        Listen clients
        :return:
        """
        self.__bind()

        while True:
            client, addr = self.socket.accept()

            command = client.recv(1000)
            if len(command) == 0:
                pass
            else:
                self.send_to_all_servers(command)

            # Encerra Conexão com o Cliente
            client.close()


server = ReplicationManagerServer()
server.listen_clients()
