#! /usr/bin/python
import random, MySQLdb, datetime, string

def mysqlWorker(mysql, conn, query, y):
    """Function to process and execute queries given it, y=0 returns a single line, y=1 returns all columns, everything else returns nothing"""
    if int(y) == 0:
        mysql.execute(query)
        data = mysql.fetchall()[0]
        conn.commit()
        return data
    elif int(y) == 1:
        mysql.execute(query)
        data = mysql.fetchall()
        return data
    else:
        mysql.execute(query)
        conn.commit()

def nameGen():
    """Name generator"""
    vowels = "aeiou"
    CONSONANTS = "bcdfghjklmnpqrstvwxyz"
    name1 = "{}{}".format(random.choice(CONSONANTS.upper()), random.choice(vowels))
    result = ""
    for i in range(random.randint(1, 6)):
        y = random.randint(0, 1)
        if y == 1:
            z = random.choice(CONSONANTS)
        elif y == 0:
            z = random.choice(vowels)
        result += z
    name = "{}{}".format(name1, result)
    return name

def user():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    queries = ['Select max(user_id) from user', 'Update user set history_id = {} where user_id = {}']
    prov = ['Insert into user (firstname, lastname, ID, username, password, password_sha, password_salt, usergroup_id) values ("{}", "{}", "{}", "{}", "5f4dcc3b5aa765d61d8327deb882cf99", "83b616fc68dbc85b89e00d6cc1a517dfc05fc4d02fac0fab86e6110c220c675b", "dcdd64c9a7a3fbb08e07603bc489299b", 0)', 'Insert into user (firstname, lastname, ID, username, password, password_sha, password_salt, usergroup_id, isprovider) values ("{}", "{}", "{}", "{}", "5f4dcc3b5aa765d61d8327deb882cf99", "83b616fc68dbc85b89e00d6cc1a517dfc05fc4d02fac0fab86e6110c220c675b", "dcdd64c9a7a3fbb08e07603bc489299b", 0, 1)']
    number = int(raw_input("How many users would you like to create? "))
    for a in range(int(number)):
        firstname = nameGen()
        lastname = nameGen()
        username = "{}0".format(firstname)
        ID = "{}{}".format(firstname[1], lastname[1])
        mysqlWorker(mysql, conn, random.choice(prov).format(firstname, lastname, ID, username), 2)
        data = mysqlWorker(mysql, conn, queries[0], 0)
        userId = int(data[0])
        historyId = historyInOut(mysql, conn, userId)
        mysqlWorker(mysql, conn, queries[1].format(historyId, userId), 2)

def historyInOut(mysql, conn, userId):
    queries = ['Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "resource", {}, "localhost", 2, "{}", 59)', 'Select max(history_id) from macpractice_log.history']
    dts = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[0].format(userId, dts), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    return data[0]

if __name__ == "__main__":
    user()