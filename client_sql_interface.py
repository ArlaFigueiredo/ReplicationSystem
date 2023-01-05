import argparse

from client_sql import ClientSQL

# Initialize parser
parser = argparse.ArgumentParser()

# Adding optional argument
parser.add_argument("-c", dest="create", help="Create a row, table ou database")
parser.add_argument("-d", dest="delete", help="Delete a row, table ou database")
parser.add_argument("-s", dest="select", help="Select a row, table ou database")
parser.add_argument("-u", dest="update", help="Update a row, table ou database")


# Read arguments from command line
args = parser.parse_args()

# Intancia cliente SQL
client_sql = ClientSQL()

if args.select:
    client_sql.send_sql_command(args.select)
    print(f"Displaying Output as: {args.select}")

if args.create:
    client_sql.send_sql_command(args.select)
    print(f"Displaying Output as: {args.create}")

if args.delete:
    client_sql.send_sql_command(args.select)
    print(f"Displaying Output as: {args.delete}")

if args.update:
    client_sql.send_sql_command(args.select)
    print(f"Displaying Output as: {args.update}")
