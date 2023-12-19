import sqlite3
import pandas as pd

'''
    Define the names of the two databases we want to read
'''
db1_name = '/home/werner/Documents/Upwork/file1'
db2_name = '/home/werner/Documents/Upwork/file2'
db_final = '/home/werner/Documents/Upwork/finaldb.db'
'''
    Establish connections to the two databases
'''
try:
    connection1 = sqlite3.connect(db1_name)
    cursor1 = connection1.cursor()
except:
    print('Could not connect to',db1_name)

try:
    connection2 = sqlite3.connect(db2_name)
    cursor2 = connection2.cursor()
except:
    print('Could not connect to',db2_name)

try:
    connection3 = sqlite3.connect(db_final)
    cursor3 = connection3.cursor()
except:
    print('Could not connect to',db_final)

'''
    Now we get all the tables in each of the databases
'''
tables_to_sync_1 = []
cursor1.execute("SELECT name FROM sqlite_master WHERE type='table';")

for table_name in cursor1.fetchall():
    tables_to_sync_1.append(table_name[0])

tables_to_sync_2 = []
cursor2.execute("SELECT name FROM sqlite_master WHERE type='table';")

for table_name in cursor2.fetchall():
    tables_to_sync_2.append(table_name[0])

'''
    Check which tables are not in both databases
'''
tables_to_sync = []
for table_name in tables_to_sync_1:
    if table_name not in tables_to_sync_2:
        print('Not found in database 2:', table_name)
    elif table_name not in tables_to_sync:
        tables_to_sync.append(table_name)

for table_name in tables_to_sync_2:
    if table_name not in tables_to_sync_1:
        print('Not found in database 1:', table_name)
    elif table_name not in tables_to_sync:
        tables_to_sync.append(table_name)


for table in tables_to_sync:
    sqlquery = "SELECT * from " + table
    df1 = pd.read_sql_query(sqlquery, connection1)
    df2 = pd.read_sql_query(sqlquery, connection2)
    df3 = pd.concat([df1,df2]).drop_duplicates().reset_index(drop=True)

    sqlquery = "PRAGMA table_info('" + table + "')"
    sqlquery = "SELECT sql FROM sqlite_master WHERE tbl_name = '" + table + "'"

    createscript = cursor1.execute(sqlquery).fetchall()[0][0]
    cursor3.execute(createscript)
    connection3.commit()
    df3.to_sql(table, connection3, if_exists='append')