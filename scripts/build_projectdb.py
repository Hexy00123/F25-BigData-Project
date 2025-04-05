import psycopg2 as psql
from pprint import pprint
import time
import os

# Read password from secrets file
file = os.path.join("secrets", ".psql.pass")
with open(file, "r") as file:
        password=file.read().rstrip()
        
# build connection string
conn_string=f"host=hadoop-04.uni.innopolis.ru port=5432 user=team11 dbname=team11_projectdb password={password}"

# Connect to the remote dbms
with psql.connect(conn_string) as conn:
        
    # Create a cursor for executing psql commands
    cur = conn.cursor()
    
    # Read the commands from the file and execute them.
    print('INFO: Creating tables...')
    with open(os.path.join("sql","create_tables.sql")) as file:
        content = file.read()
        cur.execute(content)
    print('INFO: Created')
    conn.commit()
    
    # Read the commands from the file and execute them.
    start_time = time.time()
    print('INFO: Loading data to db...')
    with open(os.path.join("sql", "import_data.sql")) as file:
         commands= file.readlines()
         with open(os.path.join("data","dataset.csv"), "r") as taxi_trips:
            cur.copy_expert(commands[0], taxi_trips)
    conn.commit()
    end_time = time.time()
    print(f'INFO: Data loaded in {round((end_time - start_time) / 60, 2)} minutes')    

    pprint(conn)
    print('INFO: Some test data:')
    cur = conn.cursor()
    with open(os.path.join("sql", "test_database.sql")) as file:
            commands = file.readlines()
            for command in commands:
                    cur.execute(command)
                    pprint(cur.fetchall())            
    print(cur)