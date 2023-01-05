import socket

# IP do Servidor Replication-Manager
IP_RM = "localhost"
# Porta socket Servidor Replication-Manager
PORT_RM = 8900


class ClientSQL:
    def __init__(self):
        self.addr = (IP_RM, PORT_RM)
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def __start_connection(self):
        """
        Start connection with RM
        :return:
        """
        self.client_socket.connect(self.addr)

    def __close_connection(self):
        """
        Closes connection with RM
        :return:
        """
        self.client_socket.close()

    def send_sql_command(self, command: str):
        """
        Send SQL command to RM server
        :param command: SQL command
        :return:
        """

        self.__start_connection()
        self.client_socket.send(command.encode('UTF-8'))
        self.__close_connection()
