import argparse
import logging

from client.client_sql import ClientSQL

# Create a logger
logger = logging.getLogger(__name__)

# Initialize parser
parser = argparse.ArgumentParser()

# Adding optional argument
parser.add_argument("-c", dest="command", help="SQL command")

# Read arguments from command line
args = parser.parse_args()

# Client SQL instance
client_sql = ClientSQL()

if args.command:
    try:
        client_sql.send_sql_command(args.command)
    except Exception as e:
        logger.exception(e)
    else:
        logger.info(f'Comando {args.command} executado com sucesso.')
