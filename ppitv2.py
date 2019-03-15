#! /usr/bin/python

import MySQLdb, random, string, datetime

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

def insuranceIdf(mysql, conn):
    query = 'select insurance_id from insurance where insurance_id > 1'
    data = mysqlWorker(mysql, conn, query, 1)
    insuranceList = []
    for i in data:
        insuranceList.append(int(i[0]))
    insuranceId = random.choice(insuranceList)
    return insuranceId

def personIdf(mysql, conn):
    query = 'select person_id from person where person_id not in (select person_id from patient) and person_id not in (select person_id from person_ins_tie) and person_id > 3'
    people = []
    data = mysqlWorker(mysql, conn, query, 1)
    for i in data:
        people.append(int(i[0]))
    return people

def pitIns(mysql, conn, startDate, personId):
    insuranceId = insuranceIdf(mysql, conn)
    queries = ['Insert into person_ins_tie (person_id, insurance_id, person_start_date) values ({}, {}, "{}")', 'Select max(person_ins_tie_id) from person_ins_tie']
#    print queries[0].format(personId, insuranceId, startDate)
    mysqlWorker(mysql, conn, queries[0].format(personId, insuranceId, startDate), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    return data[0]

def ppitIns(mysql, conn, pitId, startDate, personId):
    patientId = patient(mysql, conn, personId)
    query = 'insert into patient_person_ins_tie (patient_id, person_ins_tie_id, enabled, history_id, patient_start_date) values ({}, {}, 1, 0, "{}")'
    mysqlWorker(mysql, conn, query.format(patientId, pitId, startDate), 2)

def patient(mysql, conn, personId):
    queries = ['Select account_id from account where primary_person_id = {} or secondary_person_id = {}', 'Select patient_id from patient where account_id = {}']
    data1 = mysqlWorker(mysql, conn, queries[0].format(personId, personId), 0)
    data2 = mysqlWorker(mysql, conn, queries[1].format(data1[0]), 0)
    return data2[0]

def run_ppit():
    conn = MySQLdb.Connect(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    people = personIdf(mysql, conn)
    for i in people:
        startDate = "{}-{:02}-{:02}".format(2015, random.randint(1, 4), random.randint(1, 28))
        pitId = pitIns(mysql, conn, startDate, i)
        ppitIns(mysql, conn, pitId, startDate, i)

if __name__ == "__main__":
    run_ppit()