#! /usr/bin/python

import MySQLdb, datetime, random, string

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

def addressIDf(mysql, conn):
    query = "select address_id from address"
    data = mysqlWorker(mysql, conn, query, 1)
    addresses = []
    for i in data:
        addresses.append(int(i[0]))
    addressId = random.choice(addresses)
    return addressId

def insEmailf(mysql, conn):
    a = nameGen()
    b = nameGen()
    insEmail = "{}@{}.com".format(a, b)
    return insEmail
        
def HistoryInOut(mysql, conn, insuranceId):
    queries = ['Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "insurance", {}, "localhost", 2, "{}", 33)', 'Select max(history_id) from macpractice_log.history']
    dts = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[0].format(insuranceId, dts), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    return data[0]

def insuranceIns(mysql, conn, carrierName):
    queries = ['Insert into insurance (address_id, carrier, erased, email) values ({}, "{}", 0, "{}")', 'Select max(insurance_id) from insurance', 'Set foreign_key_checks = 0', 'Set foreign_key_checks = 1']
    addressId = addressIDf(mysql, conn)
    insEmail = insEmailf(mysql, conn)
    mysqlWorker(mysql, conn, queries[0].format(addressId, carrierName, insEmail), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    return data[0]

def planIns(mysql, conn, insuranceId):
    queries = ['Insert into plan (insurance_id, name, annualmax, deductible, plan_state, plan_participate, coordination, pop_deductible_apply, plan_address1, plan_city, plan_zip, ins_plan_type_id, plan_e_mail, erased, start_date, end_date) values ({}, "New Plan", "0.00", "0.00", "Ne", 1, 0, 1, "{}", "Lincoln", {}, {}, "{}", 0, "0000-00-00", "0000-00-00")', 'Select ins_plan_type_id from ins_plan_type where plan_type in ("PPO", "HMO", "EPO", "Commins")', 'Select max(plan_id) from plan']
    stype = ["Ave", "Street", "Place", "Circle", "Blvd", "Drive", "Road"]
    data = mysqlWorker(mysql, conn, queries[1], 1)
    plan_types = []
    for i in data:
        plan_types.append(int(i[0]))
    plan_type = random.choice(plan_types)
    a = nameGen()
    address1 = '{:04} {} {}'.format(random.randint(100, 9999), a, (random.choice(stype)))
    zipcode = "685{:02}".format(random.randint(0, 16))
    email = insEmailf(mysql, conn)
    mysqlWorker(mysql, conn, queries[0].format(insuranceId, address1, zipcode, random.choice(plan_types), email), 2)
    data = mysqlWorker(mysql, conn, queries[2], 0)
    return data[0]

def procTypesplanTie(mysql, conn, planId):
    queries = ['Insert into procedure_type (a_description, is_orthodontic) values ("{}", 0)', 'Insert into plan_tie (plan_id, procedure_type_id, percentage, flag_apply) values ({}, {}, "{}", 1)', 'Select procedure_type_id, a_description from procedure_type where procedure_type_id > 1', 'Select procedure_type_id, a_description from procedure_type']
    proc_types = {}
    percents = { 50:50, 55:45, 60:40, 65:35, 70:30, 75:25, 80:20, 85:15, 90:10, 95:5, 100:0 }
    data = mysqlWorker(mysql, conn, queries[3], 1)
    for i in data:
        proc_types[int(i[0])] = i[1]
    if len(data) < 3:
        for i in proc_types:
            mysqlWorker(mysql, conn, queries[0].format(i), 2)
    for i,g in proc_types.items():
        d = random.choice(percents.keys())
        percent = '{}/{}'.format(d, percents[d])
        mysqlWorker(mysql, conn, queries[1].format(planId, i, percent), 2)

def run_ins():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    name_list = ['Aetna', 'Blue Cross Blue Shield', 'Medicare', 'United Health Care', 'Cigna', 'Delta', 'Kaiser', 'Wellpoint', 'Highmark', 'Humana', 'Assurant', 'Aflac']
    for i in name_list:
        insuranceId = insuranceIns(mysql, conn, i)
        historyId = HistoryInOut(mysql, conn, insuranceId)
        planId = planIns(mysql, conn, insuranceId)
        procTypesplanTie(mysql, conn, planId)

if __name__ == "__main__":
    run_ins()