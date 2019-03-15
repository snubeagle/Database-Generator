#! /usr/bin/python

import random, MySQLdb, string, datetime

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
    
def ppa(mysql, conn):
    queries = ['Select max(patient_id) from patient', 'Select person.person_id as person_id, patient.patient_id as patient_id, patient.account_id as account_id from patient left join person on patient.person_id = person.person_id where patient_id = {}']
    data = mysqlWorker(mysql, conn, queries[0], 0)
    pim = data[0]
    data = mysqlWorker(mysql, conn, queries[1].format(random.randint(2, pim)), 0)
    personId = int(data[0])
    patientId = int(data[1])
    accountId = int(data[2])
    return personId, patientId, accountId

def getIncidentid(mysql, conn, personId, patientId, accountId, incidentTypeid, incidentnameId):
    incidentDate = get_incident_date(mysql, conn, patientId)
    signature = sig()
    queries = ['Select incidentname_id from incidentname', 'Insert into incident (patient_id, incidentname_id, incident_type_id, pop_first_symptom, attorney_notice_filing, pop_branch, pop_status, pop_eligibility, cda_reason_code, extraction_tooth, remaining_months_treatment, total_months_treatment, admitted_time, discharge_time, claim_codes, other_claim_id, other_claim_id_qualifier, flag_expanded) values ({}, {}, {}, 0, 0, 0, 0, 0, 0, "", 0, 0, "", "", "", "", "", 1)', 'Insert into a_action (name, ip, audit_id, signature, ts, owner, user, user_id) values ("New Incident", "172.27.13.162", "AD", {}, "{}", "IncidentsController", "User, Admin", 2)', 'Insert into a_incident (patient_id, incidentname_id, incident_type_id, pop_first_symptom, pop_condition_related_to, attorney_notice_filing, pop_branch, pop_status, pop_eligibility, cda_reason_code, extraction_tooth, remaining_months_treatment, total_months_treatment, admitted_time, discharge_time, claim_codes, incident_date, incident_id, b_action_id, e_action_id, tie_id, flag_expanded, other_claim_id, other_claim_id_qualifier) values ({}, {}, {}, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, "", "", "", "{}", {}, {}, 0, 0, 1, "", "")', 'Select max(a_action_id) from a_action', 'Select max(incident_id) from incident']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    if data[0][0] < 1:
        incidentnameId = incidentname(mysql, conn)
        incidentTypeid = incident_type_ins(mysql, conn)
    mysqlWorker(mysql, conn, queries[1].format(patientId, incidentnameId, incidentTypeid, incidentDate), 2)
    dts = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[2].format(signature, dts), 2)
    data = mysqlWorker(mysql, conn, queries[4], 0)
    b_action_id = data[0]
    data = mysqlWorker(mysql, conn, queries[5], 0)
    incidentId = data[0]
    treatmentplan(mysql, conn, incidentId, signature, patientId, incidentnameId, incidentTypeid, incidentDate)

def treatmentplan(mysql, conn, incidentId, signature, patientId, incidentnameId, incidentTypeid, incidentDate):
    queries = ['Insert into a_action (name, ip, audit_id, signature, ts, owner, user_id, user) values ("Add Default phase", "172.27.13.162", "AD", {}, "{}", "LedgerHelper", 2, "User, Admin")', 'Insert into txplan (name, incident_id) values ("New Treatment Plan", {})', 'Select max(txplan_id) from txplan', 'Insert into txphase (txplan_id, name) values ({}, "Phase 1")', 'Select max(txphase_id) from txphase', 'Insert into a_action (name, ip, audit_id, signature, ts, owner, user_id, user) values ("Add Default TxPlan to New Incident", "172.27.13.162", "AD", {}, "{}", "LedgerHelper", 2, "User, Admin")', 'Select max(txphase_id) from txphase', 'Insert into a_action (name, ip, audit_id, signature, ts, owner, user, user_id) values ("New Incident", "172.27.13.162", "AD", {}, "{}", "IncidentsController", "User, Admin", 2)', 'Select max(a_action_id) from a_action', 'Insert into a_txplan (tie_id, b_action_id, txplan_id, name, incident_id) values (0, {}, {}, "New Treatment Plan", {})', 'Insert into a_txphase (tie_id, b_action_id, txphase_id, name, txplan_id) values (0, {}, {}, "Phase 1", {})', 'Insert into a_incident (patient_id, incidentname_id, incident_type_id, pop_first_symptom, pop_condition_related_to, attorney_notice_filing, pop_branch, pop_status, pop_eligibility, cda_reason_code, extraction_tooth, remaining_months_treatment, total_months_treatment, admitted_time, discharge_time, claim_codes, incident_date, incident_id, b_action_id, e_action_id, tie_id, flag_expanded, other_claim_id, other_claim_id_qualifier) values ({}, {}, {}, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, "", "", "", "{}", {}, {}, 0, 0, 1, "", "")']
    mysqlWorker(mysql, conn, queries[1].format(incidentId), 2)
    data = mysqlWorker(mysql, conn, queries[2], 0)
    txplanId = data[0]
    mysqlWorker(mysql, conn, queries[3].format(txplanId), 2)
    data = mysqlWorker(mysql, conn, queries[6], 0)
    txphaseId = data[0]
    ts = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[0].format(signature, ts), 2)
    data = mysqlWorker(mysql, conn, queries[8], 0)
    bActionid = data[0]
    mysqlWorker(mysql, conn, queries[9].format(bActionid, txplanId, incidentId), 2)
    mysqlWorker(mysql, conn, queries[7].format(signature, ts), 2)
    data = mysqlWorker(mysql, conn, queries[8], 0)
    bActionid = data[0]
    mysqlWorker(mysql, conn, queries[11].format(patientId, incidentnameId, incidentTypeid, incidentDate, incidentId, bActionid), 2)
    mysqlWorker(mysql, conn, queries[5].format(signature, ts), 2)
    data = mysqlWorker(mysql, conn, queries[8], 0)
    bActionid = data[0]
    mysqlWorker(mysql, conn, queries[10].format(bActionid, txphaseId, txplanId), 2)

def sig():
    sign = ""
    for i in range(9):
        test = random.randint(1, 9)
        sign += str(test)
    return sign

def get_incident_date(mysql, conn, patientId):
    appointments = []
    query = 'Select date(created) from appointment where patient_id = {}'
    data = mysqlWorker(mysql, conn, query.format(patientId), 1)
    if data:
        for i in data:
            appointments.append(i[0].strftime("%Y-%m-%d"))
        incidentDate = random.choice(appointments)
    else:
        incidentDate = "{} 00:00:00".format(str(datetime.date.today()))
    return incidentDate

def incidentname(mysql, conn):
    nameslist = ["New Incident", "Encounter", "Office Visit", "Consultation", "Office Encounter"]
    queries = ['Select incidentname_id from incidentname', 'Insert into incidentname (a_description) values ("{}")']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    if len(data) < 5:
        for i in range(5):
            mysqlWorker(mysql, conn, queries[1].format(nameslist[i]), 2)
    incidentnameids = []
    for i in data:
        incidentnameids.append(int(i[0]))
    incidentnameId = random.choice(incidentnameids)
    return incidentnameId

def incident_type_ins(mysql, conn):
    queries =['Select incident_type_id from incident_type limit 4', "Insert into incident_type (a_description) values ('{}')", 'Select incident_type_id from incident_type where incident_type_id > 1']
    data = mysqlWorker(mysql, conn, queries[2], 1)
    if len(data) < 4:
        desc = nameGen()
        mysqlWorker(mysql, conn, queries[1].format(desc), 2)
        incident_type_ins(mysql, conn)
    else:
        data = mysqlWorker(mysql, conn, queries[2], 1)
        incident_type_ids = []
        for i in data:
            incident_type_ids.append(int(i[0]))
        incident_type_id = random.choice(incident_type_ids)
        return incident_type_id

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

def run():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    personId, patientId, accountId = ppa(mysql, conn)
    incidentTypeid = incident_type_ins(mysql, conn)
    incidentnameId = incidentname(mysql, conn)
    getIncidentid(mysql, conn, personId, patientId, accountId, incidentTypeid, incidentnameId)
    conn.close()
    mysql.close()
    
if __name__ == "__main__":
    run()