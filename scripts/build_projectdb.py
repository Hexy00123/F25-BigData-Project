"""
build_projectdb.py

This module contains functions to connect to a PostgreSQL database, create tables, import 
data from a CSV file, and execute test queries.
"""


import os
import time
from pprint import pprint
import psycopg2 as psql

# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r", encoding='utf-8') as file:
    password=file.read().rstrip()

# build connection string
HOST = "hadoop-04.uni.innopolis.ru"
PORT = "5432"
USER = "team11"
DBNAME = "team11_projectdb"
CONNECTION_STRING=f"host={HOST} port={PORT} user={USER} dbname={DBNAME} password={password}"

# Connect to the remote dbms
with psql.connect(CONNECTION_STRING) as conn:

    # Create a cursor for executing psql commands
    cur = conn.cursor()

    # Read the commands from the file and execute them.
    print('INFO: Creating tables...')
    with open(os.path.join("sql","create_tables.sql"), encoding='utf-8') as file:
        content = file.read()
        cur.execute(content)
    print('INFO: Created')
    conn.commit()

    # Read the commands from the file and execute them.
    start_time = time.time()
    print('INFO: Loading data to db...')
    with open(os.path.join("sql", "import_data.sql"), encoding='utf-8') as file:
        commands= file.readlines()
        with open(os.path.join("data","dataset.csv"), "r", encoding='utf-8') as taxi_trips:
            cur.copy_expert(commands[0], taxi_trips)
    conn.commit()
    end_time = time.time()
    print(f'INFO: Data loaded in {round((end_time - start_time) / 60, 2)} minutes')

    pprint(conn)
    print('INFO: Some test data:')
    cur = conn.cursor()
    with open(os.path.join("sql", "test_database.sql"), encoding='utf-8') as file:
        commands = file.readlines()
        for command in commands:
            cur.execute(command)
            pprint(cur.fetchall())
    print(cur)
