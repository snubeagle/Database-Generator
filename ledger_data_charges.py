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

def get_ppa(mysql, conn):
    queries = ['Select patient_id from patient where patient_id > 1', 'Select person.person_id as person_id, patient.patient_id as patient_id, patient.account_id as account_id, person.first, person.last from patient left join person on patient.person_id = person.person_id where patient_id = {}']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    patientIds = []
    for i in data:
        patientIds.append(int(i[0]))
    data = mysqlWorker(mysql, conn, queries[1].format(random.choice(patientIds)), 0)
    personId = int(data[0])
    patientId = int(data[1])
    accountId = int(data[2])
    flname = "{} {}".format(data[3], data[4])
    return personId, patientId, accountId, flname

def get_incident(mysql, conn, patientId):
    query = 'select incident_id from incident where patient_id = {}'
    data = mysqlWorker(mysql, conn, query.format(patientId), 1)
    incidentlist = []
    for i in data:
        incidentlist.append(i[0])
    if len(incidentlist) > 0:
        incidentId = random.choice(incidentlist)
    else:
        incidentDate = get_incident_date(mysql, conn, patientId)
        incidentId = incidentIn(mysql, conn, incidentDate, patientId)
    return incidentId

def incidentIn(mysql, conn, incidentDate, patientId):
    signature = sig(mysql, conn)
    queries = ['Select incidentname_id from incidentname', 'Insert into incident (patient_id, incidentname_id, incident_type_id, pop_first_symptom, attorney_notice_filing, pop_branch, pop_status, pop_eligibility, cda_reason_code, extraction_tooth, remaining_months_treatment, total_months_treatment, admitted_time, discharge_time, claim_codes, other_claim_id, other_claim_id_qualifier, flag_expanded) values ({}, {}, {}, 0, 0, 0, 0, 0, 0, "", 0, 0, "", "", "", "", "", 1)', 'insert into a_action (name, ip, audit_id, signature, ts, owner, user, user_id) values ("New Incident", "172.27.13.162", "AD", {}, "{}", "IncidentsController", "User, Admin", 2)', 'Insert into a_incident (patient_id, incidentname_id, incident_type_id, pop_first_symptom, pop_condition_related_to, attorney_notice_filing, pop_branch, pop_status, pop_eligibility, cda_reason_code, extraction_tooth, remaining_months_treatment, total_months_treatment, admitted_time, discharge_time, claim_codes, incident_date, incident_id, b_action_id, e_action_id, tie_id, flag_expanded, other_claim_id, other_claim_id_qualifier) values ({}, {}, {}, 0, 0, 0, 0, 0, 0, 0, "", 0, 0, "", "", "", "{}", {}, {}, 0, 0, 1, "", "")', 'select max(a_action_id) from a_action', 'select max(incident_id) from incident']
    incidentnameId = incidentname_id(mysql, conn)
    incidentTypeid = incident_type_ins(mysql, conn)
    mysqlWorker(mysql, conn, queries[1].format(patientId, incidentnameId, incidentTypeid, incidentDate), 2)
    dts = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[2].format(signature, dts), 2)
    data = mysqlWorker(mysql, conn, queries[4], 0)
    b_action_id = data[0]
    data = mysqlWorker(mysql, conn, queries[5], 0)
    incidentId = data[0]
    mysqlWorker(mysql, conn, queries[3].format(patientId, incidentnameId, incidentTypeid, incidentDate, incidentId, b_action_id), 2)
    return incidentId

def sig(mysql, conn):
    sign = ""
    for i in range(9):
        test = random.randint(1, 9)
        sign += str(test)
    return sign

def get_incident_date(mysql, conn, patientId):
    appointments = []
    query = 'select date(created) from appointment where patient_id = {}'
    data = mysqlWorker(mysql, conn, query.format(patientId), 1)
    if data:
        for i in data:
            appointments.append(i[0].strftime("%Y-%m-%d"))
        incidentDate = random.choice(appointments)
    else:
        incidentDate = "{} 00:00:00".format(str(datetime.date.today()))
    return incidentDate

def incidentname_id(mysql, conn):
    nameslist = ["New Incident", "Encounter", "Office Visit", "Consultation", "Office Encounter"]
    queries = ['Select incidentname_id from incidentname', 'Insert into incidentname (a_description) values ("{}")', 'Select max(incidentname_id) from incidentname']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    incidentnames = []
    if len(data) < 5:
        for i in range(5):
            mysqlWorker(mysql, conn, queries[1].format(nameslist[i]), 2)
    for i in data:
        incidentnames.append(int(i[0]))
    incidentnameId = random.choice(incidentnames)
    return incidentnameId

def incident_type_ins(mysql, conn):
    queries = ['Select incident_type_id from incident_type limit 4', "Insert into incident_type (a_description) values ('{}')", 'Select incident_type_id from incident_type where incident_type_id > 1']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    if len(data) < 4:
        for i in range(4):
            desc = nameGen()
            mysqlWorker(mysql, conn, queries[1].format(desc), 2)
    data = mysqlWorker(mysql, conn, queries[2], 1)
    incident_ids = []
    for i in data:
        incident_ids.append(int(i[0]))
    incidentTypeid = random.choice(incident_ids)
    return incidentTypeid

def fee_info(mysql, conn):
    query = 'select fee.fee_id, fee.feeschedule_id, fee.unit_fee, fee.short_description, fee.code, feeschedule.feeschedule_name from fee left join feeschedule on fee.feeschedule_id = feeschedule.feeschedule_id'
    data = mysqlWorker(mysql, conn, query, 1)
    x = random.choice(data)
    feeId = x[0]
    feescheduleId = x[1]
    feeAmt = x[2]
    feeShort = x[3]
    feeCode = x[4]
    feescheduleName = x[5]
    return feeId, feescheduleId, feeAmt, feeShort, feeCode, feescheduleName

def historyIns(mysql, conn, patientId, Id, y):
    queries = ["""Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id, affects_patient_id) values ("I", "charge", {}, "localhost", 2, "{}", 13, {})""", """Update charge set history_id = {} where charge_id = {}""", """Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id, affects_patient_id) values ("I", "ledger", {}, "localhost", 2, "{}", 37, {})""", """Update ledger set history_id = {} where ledger_id = {}""", 'Select max(history_id) from macpractice_log.history']
    dts = str(datetime.datetime.now()).split('.')[0]
    if y == 0:
        mysqlWorker(mysql, conn, queries[0].format(Id, dts, patientId), 2)
        data = mysqlWorker(mysql, conn, queries[4], 0)
        mysqlWorker(mysql, conn, queries[1].format(data[0], Id), 2)
        return data[0]
    elif y == 1:
        mysqlWorker(mysql, conn, queries[2].format(Id, dts, patientId), 2)
        data = mysqlWorker(mysql, conn, queries[4], 0)
        mysqlWorker(mysql, conn, queries[3].format(data[0], Id), 2)

def get_provider(mysql, conn):
    queries = ['select user_id, ID from user where isprovider = 1', 'select office_id, id from office where office_id > 1']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    providerIds = random.choice(data)
    providerId = int(providerIds[0])
    data1 = mysqlWorker(mysql, conn, queries[1], 1)
    officeIds = []
    officeIdss = random.choice(data1)
    prov = "{}{}".format(providerIds[1], officeIdss[1])
    return providerId, officeIdss[0], prov

def ledger_ins(mysql, conn):
    chargeId, providerId, officeId, prov, personId, patientId, accountId, flname, feeId, feescheduleId, feeAmt, feeShort, feeCode, schName, incidentId, procDate = charge_ins(mysql, conn)
    sign = sig(mysql, conn)
    dts = str(datetime.datetime.now()).split('.')[0]
    historyId = historyIns(mysql, conn, patientId, chargeId, 0)
    a_charge_data(mysql, conn, sign, dts, chargeId, providerId, officeId, incidentId, historyId, feescheduleId, feeId, feeCode, procDate, feeAmt, feeShort)
    queries = ['Insert into ledger (patient_id, incident_id, account_id, provider_id, office_id, table_type, table_id, table_name, posted_date, code, prov, debit, unpaid, insurance_portion, patient_portion, description, units, proc_date, audit_id, fee, fee_tax, is_lab, original_fee, discount, schedule, thru_date, modifier1, modifier2, modifier3, modifier4, lab_fee, lab_cost, lab_place, time, category, blood, tooth, surface, discharge_status, tos, pos) values ({}, {}, {}, {}, {}, 0, {}, "charge", "{}", "{}", "{}", "{}", "{}", "0.00", "{}", "{}", "1.00", "{}", "AD", "{}", "0.00", 0, "{}", "0.00", "{}", "00-00-00 00:00:00", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", "Medical Care (1)", "Office (11)")', 'Select max(ledger_id) from ledger', 'Insert into a_ledger (patient_id, incident_id, account_id, provider_id, office_id, table_type, table_id, table_name, posted_date, code, prov, debit, unpaid, insurance_portion, patient_portion, description, units, proc_date, audit_id, fee, fee_tax, is_lab, original_fee, discount, schedule, thru_date, modifier1, modifier2, modifier3, modifier4, lab_fee, lab_cost, lab_place, time, category, blood, tooth, surface, discharge_status, tos, pos, tie_id, b_action_id, ledger_id) values ({}, {}, {}, {}, {}, 0, {}, "charge", "{}", "{}", "{}", "{}", "{}", "0.00", "{}", "{}", "1.00", "{}", "AD", "{}", "0.00", 0, "{}", "0.00", "{}", "00-00-00 00:00:00", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", " ", "Medical Care (1)", "Office (11)", 0, {}, {})', 'Insert into a_action (name, ip, audit_id, signature, ts, owner, user_id, user) values ("New Charge", "172.27.13.162", "AD", {}, "{}", "Patient", 2, "User, Admin")', 'select max(a_action_id) from a_action']
    mysqlWorker(mysql, conn, queries[0].format(patientId, incidentId, accountId, providerId, officeId, chargeId, dts, feeCode, prov, feeAmt, feeAmt, feeAmt, feeShort, procDate, feeAmt, feeAmt, schName), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    mysqlWorker(mysql, conn, queries[3].format(sign, dts), 2)
    data1 = mysqlWorker(mysql, conn, queries[4], 0)
    mysqlWorker(mysql, conn, queries[2].format(patientId, incidentId, accountId, providerId, officeId, chargeId, dts, feeCode, prov, feeAmt, feeAmt, feeAmt, feeShort, procDate, feeAmt, feeAmt, schName, data1[0], data[0]), 2)
    return patientId, data[0]

def a_charge_data(mysql, conn, sign, dts, chargeId, providerId, officeId, incidentId, historyId, feescheduleId, feeId, feeCode, procDate, feeAmt, feeShort):
    queries = ['insert into a_action (name, ip, audit_id, signature, ts, owner, user_id, user) values ("New Charge", "172.27.13.162", "AD", {}, "{}", "Patient", 2, "User, Admin")', 'select max(a_action_id) from a_action', 'Insert into a_charge (tie_id, b_action_id, charge_id, provider_id, office_id, incident_id, history_id, feeschedule_id, procedure_id, procedure_code, procedure_date, pop_fee_calculation, units, unit_fee, procedure_fee, time_units, pop_unit_type, laboratory, lab_costs, material_costs, tooth, surface, procedure_description, alias, flag_taxable, flag_emergency, procedure_category_id, modifier1, patient_paid, insurance_paid, insurance_portion, primary_deductible, sec_plan_id, sec_insurance_pay_percent, sec_deductible, plan1_paid, plan2_paid, allowed1, allowed2, wrt1, size, type, copay1, cda_lab_code1, cda_lab_fee1, cda_lab_code2, cda_lab_fee2, start1, start2, start3, end1, end2, end3, ppit_id1, ppit_id2, admitted_time, discharge_time, pop_discharge_status, claim_provider_id, patient_referrals_tie_id) values (0, {}, {}, {}, {}, {}, {}, {}, {}, "{}", "{}", 0, 1, "{}", "{}", "", 0, "", "0.00", "0.00", "", "", "{}", "", "", "", 0, "", "0.00", "0.00", 0, 0, 0, 0, 0, "", "", "", "", "", "0.0", "0.00", "0.0", "{}", "", "{}", "{}", "", "", "", "", "", "", 0, 0, "", "", 0, 0, 0)']
    mysqlWorker(mysql, conn, queries[0].format(sign, dts), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    mysqlWorker(mysql, conn, queries[2].format(data[0], chargeId, providerId, officeId, incidentId, historyId, feescheduleId, feeId, feeCode, procDate, feeAmt, feeAmt, feeShort, feeAmt, feeAmt, feeAmt), 2)

def charge_ins(mysql, conn):
    providerId, officeId, prov = get_provider(mysql, conn)
    personId, patientId, accountId, flname = get_ppa(mysql, conn)
    feeId, feescheduleId, feeAmt, feeShort, feeCode, feescheduleName = fee_info(mysql, conn)
    incidentDate = get_incident_date(mysql, conn, patientId)
    incidentId = get_incident(mysql, conn, patientId)
    dts = str(datetime.datetime.now()).split('.')[0]
    procDate = get_proc_date(mysql, conn, patientId)
    ppitId = ppit_id(mysql, conn, patientId)
    queries = ['Select max(charge_id) from charge', 'insert into charge (procedure_code, provider_id, office_id, incident_id, feeschedule_id, procedure_id, procedure_date, pop_fee_calculation, units, unit_fee, procedure_fee, time_units, pop_unit_type, laboratory, lab_costs, material_costs, tooth, surface, procedure_description, place_of_service_id, type_of_service_id, los_days, fee, lab_fee, tax, modifier1, modifier2, modifier3, modifier4, insurance_pay_percent, primary_deductible, sec_insurance_pay_percent, sec_insurance_portion, sec_deductible, patient_portion, plan1_paid, plan2_paid, allowed1, allowed2, wrt1, wrt2, copay1, cda_lab_code1, cda_lab_fee1, cda_lab_code2, cda_lab_fee2, start1, start2, start3, end1, end2, end3, ppit_id1, ppit_id2, admitted_time, discharge_time, pop_discharge_status, claim_provider_id) values ("{}", {}, "{}", {}, {}, {}, "{}", 0, 1, "{}", "{}", "", 0, "", "0.00", "0.00", "", "", "{}", 0, 0, 0, "", "0.00", "0.00", "", "", "", "", "0.0", "0.00", "0.0", "0.00", "0.00", "{}", "", "", "{}", "{}", "", "", "", "", "", "", "", "", "", "", "", "", "", {}, {}, "", "", 0, {})']
    mysqlWorker(mysql, conn, queries[1].format(feeCode, providerId, officeId, incidentId, feescheduleId, feeId, procDate, feeAmt, feeAmt, feeShort, feeAmt, feeAmt, feeAmt, ppitId, "null", providerId), 2)
    data = mysqlWorker(mysql, conn, queries[0], 0)
    return data[0], providerId, officeId, prov, personId, patientId, accountId, flname, feeId, feescheduleId, feeAmt, feeShort, feeCode, feescheduleName, incidentId, procDate

def ppit_id(mysql, conn, patientId):
    query = 'select patient_person_ins_tie_id from patient_person_ins_tie where patient_id = {}'
    data = mysqlWorker(mysql, conn, query.format(patientId), 1)
    ppitids = []
    if data:
        for i in data:
            ppitids.append(int(i[0]))
            ppitId = random.choice(ppitids)
    else:
        ppitId = "null"
    return ppitId

def get_proc_date(mysql, conn, patientId):
    appt_list = []
    dts = datetime.datetime.now()
    query = 'select end from appointment where patient_id = {}'.format(patientId)
    data = mysqlWorker(mysql, conn, query, 1)
    if len(data):
        for i in data:
            appt_list.append(i[0])
        procDates = random.choice(appt_list)
        procDate = procDates
    else:
        procDate = datetime.datetime.now()
    if dts < procDate:
        procDate = procDate.replace(procDate.year - 1)
    return procDate

def run():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    x = int(raw_input("How many charges would you like to add? "))
    for a in range(int(x)):
        patientId, ledgerId = ledger_ins(mysql, conn)
        historyIns(mysql, conn, patientId, ledgerId, 1)

if __name__ == '__main__':
    run()