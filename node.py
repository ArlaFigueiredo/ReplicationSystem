import socket
from message_type import MessageType
from message import Message


class Node:
    def __init__(self, idf, port, is_leader):
        self.idf = idf
        self.port = port
        self.host = 'localhost'
        self.socket = socket.socket()
        self.socket_members = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        self.members = [
            ("localhost", 8901),
            ("localhost", 8902),
            ("localhost", 8903),
        ]
        self.is_leader = is_leader

    def __start_connection(self, addr):
        """
        Start connection with RM
        :return:
        """
        self.socket_members.connect(addr)

    def __close_connection(self):
        """
        Closes connection with RM
        :return:
        """
        self.socket_members.close()

    def send(self, msg: str, addr):
        """
        Send SQL command to RM server
        :param msg: msg
        :param addr: addr
        :return:
        """

        self.__start_connection(addr)
        self.socket_members.send(msg.encode('UTF-8'))
        self.__close_connection()

    def send_to_all_members(self, msg):
        """

        :param msg:
        :return:
        """
        for addr_server in self.members:
            self.send(msg, addr_server)

    def __bind(self):
        """
        Bind socket connection
        :return:
        """
        self.socket.bind((self.host, self.port))
        self.socket.listen(0)

    def listen(self):
        """
        Listen nodes
        :return:
        """
        self.__bind()

        while True:
            client, addr = self.socket.accept()

            msg = client.recv(1000)
            if len(msg) == 0:
                pass
            else:
                if msg.type == MessageType.CLIENT_SQL_COMMAND:
                    if self.is_leader:
                        new_msg = Message(type=MessageType.STAGE, content=msg.content)
                        self.send_to_all_members(new_msg)
                    else:
                        # Se não for o lider ele ignora
                        pass
                if msg.type == MessageType.CONFIRM:
                    new_msg = Message(type=MessageType.COMMIT, content=msg.content)
                    self.send_to_all_members(new_msg)

                if msg.type == MessageType.STAGE:
                    new_msg = Message(type=MessageType.CONFIRM, content=msg.content)
                    self.send_to_all_members(new_msg)

                if msg.type == MessageType.COMMIT:
                    # Faria o commit
                    pass

            # Encerra Conexão com o Cliente
            client.close()
