#! /usr/bin/python

import MySQLdb, datetime, random, string, collections, random

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

def historyStuff(mysql, conn, ids, y):
    queries = ['Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "feeschedule", {}, "localhost", 2, "{}", 24)', 'Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "fee", {}, "localhost", 2, "{}", 23)', 'Select max(history_id) from macpractice_log.history']
    dts = str(datetime.datetime.now()).split('.')[0]
    if y == 0:
        mysqlWorker(mysql, conn, queries[0].format(ids, dts), 2)
        data = mysqlWorker(mysql, conn, queries[2], 0)
        historyId = data[0]
        return historyId
    elif y == 1:
        mysqlWorker(mysql, conn, queries[1].format(ids, dts), 2)
        data = mysqlWorker(mysql, conn, queries[2], 0)
        historyId = data[0]
        return historyId

def code_ins(mysql, conn, feescheduleId):
    """Insert procedure codes into database"""
    queries = ['Insert into fee (feeschedule_id, code, short_description, flag_taxable, pop_fee_calc_method, units, unit_fee, place_of_service_id, type_of_service_id, tax_type_id) values ({}, "{}", "{}", 0, 0, 1, "{}", 0, 0, 2)', 'Select max(fee_id) from fee', 'Update fee set history_id = {} where fee_id = {}']
    c = {'D0120':'Office Visit', 'D9440':'Office Visit', 99201:'Office Visit', 99209:'Office Visit', 99213:'Office Visit', 'D1110':'DENTAL PROPHYLAXIS ADULT', 'D1206':'TOPICAL FLUORIDE VARNISH', 'D0150':'COMPREHENSSVE ORAL EVALUATION', 'D0210':'INTRAOR COMPLETE FILM SERIES', 'D0270':'DENTAL BITEWING SINGLE FILM', 'D0272':'DENTAL BITEWINGS TWO FILMS', 99241:'office consultation', 99242:'office consultation'}
    codeDes = collections.OrderedDict(c)
    for i in codeDes:
        fee = '{}.{:02}'.format(random.randint(1, 250), random.randint(0, 99))
        mysqlWorker(mysql, conn, queries[0].format(feescheduleId, i, codeDes[i], fee), 2)
        data = mysqlWorker(mysql, conn, queries[1], 0)
        feeId = data[0]
        historyId = historyStuff(mysql, conn, feeId, 1)
        mysqlWorker(mysql, conn, queries[2].format(historyId, feeId), 2)

def feeschedule_ins(mysql, conn):
    """Create fee schedule entry in database"""
    number = 1
    x = 0
    queries = ['Insert into feeschedule (feeschedule_name) values ("{}")', 'Select max(feeschedule_id) from feeschedule', 'Update feeschedule set history_id = {} where feeschedule_id = {}']
    while x < number:
        feescheduleName = nameGen()
        mysqlWorker(mysql, conn, queries[0].format(feescheduleName), 2)
        data = mysqlWorker(mysql, conn, queries[1], 0)
        feescheduleId = data[0]
        historyId = historyStuff(mysql, conn, feescheduleId, 0)
        mysqlWorker(mysql, conn, queries[2].format(historyId, feescheduleId), 2)
        x += 1
    return feescheduleId

def run():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    feescheduleId = feeschedule_ins(mysql, conn)
    code_ins(mysql, conn, feescheduleId)

if __name__ == "__main__":
    run()