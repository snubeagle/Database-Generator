#! /usr/bin/python

import MySQLdb, datetime, random, string

def resource_ins():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    sch = [open_close_1, open_close_2, open_close_3, open_close_4, open_close_5, open_close_6, open_close_7, open_close_8]
    queries = ['Select count(office_id) from office', 'Select office_id from office where office_id > 1', 'Insert into resource (name, code, type, flag_available, flag_day0, open01, close01, open02, close02, flag_day1, open11, close11, open12, close12, flag_day2, open21, close21, open22, close22, flag_day3, open31, close31, open32, close32, flag_day4, open41, close41, open42, close42, flag_day5, open51, close51, open52, close52, flag_day6, open61, close61, open62, close62, flag_selected, display_order, flag_autoremind, default_office_id) values ("{}", "{}", "Room", 1, 0, "{}", "{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", 1, "{}", "{}", "{}", "{}", 1, {},  0, {})', 'Select max(resource_id) from resource', 'Update resource set history_id = {} where resource_id = {}']
    o = int(raw_input("How many resources would you like to create? "))
    x = 1
    for a in range(int(o)):
        offices = []
        resource_name = nameGen()
        open01, close01, open02, close02 = random.choice(sch)()
        code = "E{}".format(x)
        data = mysqlWorker(mysql, conn, queries[0], 1)
        if data[0][0] > 1:
            z = random.choice(range (0, int(data[0][0])))
        else:
            z = int(data[0][0])
        data = mysqlWorker(mysql, conn, queries[1], 1)
        for i in data:
            offices.append(int(i[0]))
        officeId = random.choice(offices)
        mysqlWorker(mysql, conn, queries[2].format(resource_name, code, open01, close01, open02, close02, open01, close01, open02, close02, open01, close01, open02, close02, open01, close01, open02, close02, open01, close01, open02, close02, open01, close01, open02, close02, open01, close01, open02, close02, x, officeId), 2)
        data = mysqlWorker(mysql, conn, queries[3], 0)
        resourceId = int(data[0])
        historyId = historyInOut(mysql, conn, resourceId)
        mysqlWorker(mysql, conn, queries[4].format(historyId, resourceId), 2)
        x += 1

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
    consonants = "bcdfghjklmnpqrstvwxyz"
    name1 = "{}{}".format(random.choice(consonants.upper()), random.choice(vowels))
    result = ""
    for i in range(random.randint(1, 6)):
        y = random.randint(0, 1)
        if y == 1:
            z = random.choice(consonants)
        elif y == 0:
            z = random.choice(vowels)
        result += z
    name = "{}{}".format(name1, result)
    return name

def open_close_1():
    open01 = "2013-01-20 08:00:00"
    close01 = "2013-01-20 12:00:00"
    open02 = "2013-01-20 13:00:00"
    close02 = "2013-01-20 17:00:00"
    return open01, close01, open02, close02

def open_close_2():
    open01 = "2013-01-20 08:00:00"
    close01 = "2013-01-20 13:00:00"
    open02 = "2013-01-20 14:00:00"
    close02 = "2013-01-20 17:00:00"
    return open01, close01, open02, close02

def open_close_3():
    open01 = "2013-01-20 08:00:00"
    close01 = "2013-01-20 12:00:00"
    open02 = "2013-01-20 12:30:00"
    close02 = "2013-01-20 17:00:00"
    return open01, close01, open02, close02

def open_close_4():
    open01 = "2013-01-20 08:00:00"
    close01 = "2013-01-20 11:30:00"
    open02 = "2013-01-20 12:30:00"
    close02 = "2013-01-20 17:00:00"
    return open01, close01, open02, close02
    
def open_close_5():
    open01 = "2013-01-20 08:00:00"
    close01 = '2013-01-20 11:30:00'
    open02 = "2013-01-20 12:00:00"
    close02 = "2013-01-20 17:00:00"
    return open01, close01, open02, close02

def open_close_6():
    open01 = '2013-01-20 07:30:00'
    close01 = '2013-01-20 13:00:00'
    open02 = '2013-01-20 13:30:00'
    close02 = '2013-01-20 17:00:00'
    return open01, close01, open02, close02

def open_close_7():
    open01 = '2013-01-20 07:30:00'
    close01 = '2013-01-20 13:30:00'
    open02 = '2013-01-20 14:00:00'
    close02 = '2013-01-20 17:00:00'
    return open01, close01, open02, close02

def open_close_8():
    open01 = '2013-01-20 07:30:00'
    close01 = '2013-01-20 12:00:00'
    open02 = '2013-01-20 12:30:00'
    close02 = '2013-01-20 17:00:00'
    return open01, close01, open02, close02

def historyInOut(mysql, conn, resourceId):
    queries = ['Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "resource", {}, "localhost", 2, "{}", 59)', 'Select max(history_id) from macpractice_log.history']
    dts = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[0].format(resourceId, dts), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    return data[0]

if __name__ == "__main__":
    resource_ins()