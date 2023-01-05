from dataclasses import dataclass
from message_type import MessageType


@dataclass
class Message:
    """
    Classe que representa a estrutura das mensagens trocadas pelo sistema
    """
    type: MessageType
    content: str
