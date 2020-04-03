import mysql.connector


def create_conn(dbname, hosname="localhost", username="mausolorio",
                pas="ducinALTUM7!"):
    """Create connection with database"""
    global mydb, cursor
    mydb = mysql.connector.connect(host=hosname, user=username,
                                   passwd=pas, database=dbname)
    cursor = mydb.cursor()
    print(f'Connection has been established with {dbname} database')


def close_connection(mydb, cursor):
    """Close connection data base"""
    mydb.close()
    cursor.close()
    print("MySQL connection is closed")


def convertTuple(tup):
    str = ''.join(tup)
    return str


create_conn("s&p500")
sql_Query = "DROP TABLE test1"
# sql_Query = "SELECT DISTINCT Symbol FROM consumerstaples "
# sql_Query = "SELECT * FROM industrials"
# sql_Query = "CREATE TABLE healthcare(date DATE, `1. open` FLOAT(53), `2. high` FLOAT(53), `3. low` FLOAT(53),`4. close` FLOAT(53),`5. volume` FLOAT(53),`Symbol` VARCHAR(20))"
cursor.execute(sql_Query)
# records = cursor.fetchall()
# print("Total number of rows in table is: ", cursor.rowcount)
close_connection(mydb, cursor)

# sectors = []
# for c in records:
#     string = convertTuple(c)
#     sectors.append(string.replace(" ", ""))
# print(sectors)
# create_conn("s&p500")
# for i in sectors:
#     sql_Query = "CREATE TABLE " + i + \
#         "(date DATE, `1. open` FLOAT(53), `2. high` FLOAT(53), `3. low` FLOAT(53),`4. close` FLOAT(53), `5. volume` FLOAT(53),`Symbol` VARCHAR(20))"
#     cursor.execute(sql_Query)
# close_connection(mydb, cursor)


# CREATE TABLE test5(
#     date DATETIME,
#     `1. open` FLOAT(53),
#         `2. high` FLOAT(53),
#         `3. low` FLOAT(53),
#         `4. close` FLOAT(53),
#         `5. volume` FLOAT(53),
#         `Symbol` TEXT
# )
# Industrials
# HealthCare
# InformationTechnology
# CommunicationServices
# ConsumerDiscretionary
# Utilities
# Financials
# Materials
# RealEstate
# ConsumerStaples
# Energy
