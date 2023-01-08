import logging
import random

from client.client_sql import ClientSQL

# Create a logger
logger = logging.getLogger(__name__)

# Client SQL instance
client_sql = ClientSQL()

commands = [
    "UPDATE banco SET saldo = 500 WHERE conta=1 ",
    "UPDATE banco SET saldo = 100 WHERE conta=1 ",
]

# Embaralhando ordem de execução
random.shuffle(commands)

for cmd in commands:
    client_sql.send_sql_command(cmd)
