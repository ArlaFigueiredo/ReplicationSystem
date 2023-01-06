import socket
import logging
import pickle

from config import settings
from message_params import MessageType, SenderTypes

# Create a logger
logger = logging.getLogger(__name__)


class ClientSQL:
    def __init__(self):
        self.addr = (settings.server.HOST, settings.server.PORT)
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

        try:
            self.__start_connection()
            msg = {
                "sender": SenderTypes.CLIENT,
                "type": MessageType.SQL_COMMAND,
                "content": command
            }
            self.client_socket.send(pickle.dumps(msg))
            self.__close_connection()
        except Exception as e:
            logger.exception(e)
