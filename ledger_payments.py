#! /usr/bin/python

import MySQLdb, datetime, random, string, sys

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

def get_hist_id(mysql, conn):
    query = 'Select max(history_id) from macpractice_log.history'
    datas = mysqlWorker(mysql, conn, query, 0)
    return datas[0]

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

def pmt_type(mysql, conn, amount, chrgIds, patientId):
    queries = ['select pay_type, pay_desc, payment_category_id from payment_type left join payment_category on payment_type.payment_type_id = payment_category.payment_type_id where pay_type > 0 and pay_type <> (select pay_type from payment_type where pay_desc like "Collections") and pay_type <> (select pay_type from payment_type where pay_desc like "Negative Adj.") and pay_type <> (select pay_type from payment_type where pay_desc like "Co-payment")', 'select patient_person_ins_tie_id from patient_person_ins_tie where patient_id = {}']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    x = random.choice(data)
    data1 = mysqlWorker(mysql, conn, queries[1].format(patientId), 1)
#    x = (6, 'Insurance Payment', 6)
    if x[1] == "Insurance Payment":
        if len(data1) < 1:
            y = tuple(p for p in data if p != x)
            x = random.choice(y)
        else:
            ppitid, insuranceId, insCarrier = ins_pmt(mysql, conn, amount, chrgIds, patientId)
            desc = x[1]
    if x[1] != "Insurance Payment":
        desc = "Payment {}".format(x[1])
        ppitid = 0
        insuranceId = 0
        insCarrier = 0
    payType = x[0]
    paymentCategoryid = x[2]
    return payType, paymentCategoryid, desc, ppitid, insuranceId, insCarrier

def cc():
    cc1 = ""
    for i in range(1):
        i = random.randint(1000, 9999)
        cc1 += str(i)
    return cc1

def ins_pmt(mysql, conn, amount, chrgIds, patientId):
    query = 'Select ppit.patient_person_ins_tie_id, pit.insurance_id, ins.carrier from patient_person_ins_tie as ppit left join person_ins_tie as pit on pit.person_ins_tie_id = ppit.person_ins_tie_id left join insurance as ins on ins.insurance_id = pit.insurance_id where ppit.patient_id = {} and ppit.archived <> 1 and pit.archived <> 1'
    data = mysqlWorker(mysql, conn, query.format(patientId), 1)
    if len(data) > 1:
        data1 = random.choice(data)
        data = data1
    ppitid = data[0][0]
    insuranceId = data[0][1]
    insCarrier = data[0][2]
    return ppitid, insuranceId, insCarrier

def enable_form(mysql, conn):
    queries = ["Select form_id, flag_visible from form where form_name like '%(02/12) NPI%' or form_name like '%FC CC' order by form_id", 'Update form set flag_visible = 1 where form_id = {} or form_id = {}', 'Update user_pref set pval = {} where pkey like "Form_Insurance"', 'Update user_pref set pval={} where pkey like "Form_Incident_Statement" or pkey like "Form_Account_Statement"']
    data = mysqlWorker(mysql, conn, queries[0], 1)
    forms = []
    sums = 0
    for i in data:
        forms.append(int(i[0]))
        sums += int(i[1])
    if sums < 2:
        mysqlWorker(mysql, conn, queries[1].format(forms[0], forms[1]), 2)
    mysqlWorker(mysql, conn, queries[2].format(forms[1]), 2)
    mysqlWorker(mysql, conn, queries[3].format(forms[0]), 2)
    return forms[1]

def create_claim(mysql, conn, formId, ppitid, amount):
    dts = str(datetime.datetime.now()).split('.')[0]
    queries = ["Insert into claim (form_id, is_primary_claim, primary_patient_person_ins_tie_id, date_created, claim_amount, date_sent, claim_serial_number) values ({}, 1, {}, '{}', '{}', '{}', {})", 'Select max(claim_id) from claim', 'Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "ledger", {}, "localhost", 2, "{}", 37)', 'Select max(history_id) from macpractice_log.history', 'Update claim set history_id = {} where claim_id = {}', 'Insert into claim_data (claim_id) values ({})', 'select max(claim_serial_number) from claim']
    data = mysqlWorker(mysql, conn, queries[6], 0)
    if data[0]:
        serial = data[0] + 1
    else:
        serial = 1
    mysqlWorker(mysql, conn, queries[0].format(formId, ppitid, dts, amount, dts, serial), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    mysqlWorker(mysql, conn, queries[2].format(data[0], dts), 2)
    data1 = mysqlWorker(mysql, conn, queries[3], 0)
    mysqlWorker(mysql, conn, queries[4].format(data1[0], data[0]), 2)
    mysqlWorker(mysql, conn, queries[5].format(data[0]), 2)
    return data[0]

def create_claim_tie(mysql, conn, chrgIds, claimId):
    queries = ['Insert into claim_tie (charge_id, claim_id) values ({}, {})','Select max(claim_tie_id) from claim_tie', 'Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "claim_tie", {}, "localhost", 2, "{}", 113)', 'Select max(history_id) from macpractice_log.history', 'Update claim_tie set history_id = {} where claim_tie_id = {}']
    dts = str(datetime.datetime.now()).split('.')[0]
    for i in chrgIds:
        mysqlWorker(mysql, conn, queries[0].format(i, claimId), 2)
        data = mysqlWorker(mysql, conn, queries[1], 0)
        mysqlWorker(mysql, conn, queries[2].format(data[0], dts), 2)
        data1 = mysqlWorker(mysql, conn, queries[3], 0)
        mysqlWorker(mysql, conn, queries[4].format(data1[0], data[0]), 2)
    return data[0]

def create_pmt_claim_tie(mysql, conn, paymentId, claimId):
    queries = ["Insert into payment_claim_tie (payment_id, claim_id) values ({}, {})", "Select max(payment_claim_tie_id) from payment_claim_tie", "Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ('I','payment_claim_tie', {}, 'localhost', 2, '{}', 125)", "Select max(history_id) from macpractice_log.history", "Update payment_claim_tie set history_id = {} where payment_claim_tie_id = {}"]
    dts = updt_dts()
    mysqlWorker(mysql, conn, queries[0].format(paymentId, claimId), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    mysqlWorker(mysql, conn, queries[2].format(data[0], dts), 2)
    data1 = mysqlWorker(mysql, conn, queries[3], 0)
    mysqlWorker(mysql, conn, queries[4].format(data1[0], data[0]), 2)

def updt_dts():
    dts = str(datetime.datetime.now()).split('.')[0]
    return dts

def close_claim(mysql, conn, claimId):
    queries = ['update claim set claim_status = 5 where claim_id = {}', 'insert into claim_status_log (claim_id, status_change_reason, old_status, new_status, updated_user_id, updated, eclaim_report_description_id) values ({}, 8, 0, 5, 2, "{}", 0)', 'update ledger set flag_status=5 where table_id={} and table_name like "InsuranceClaim"']
    dts = updt_dts()
    mysqlWorker(mysql, conn, queries[1].format(claimId, dts), 2)
    mysqlWorker(mysql, conn, queries[0].format(claimId), 2)
    mysqlWorker(mysql, conn, queries[2].format(claimId), 2)

def pmt_tie_ins(mysql, conn, chrgIds, chrgAmts, chrgPmtamts, wrtOff, amount, personId, patientId, accountId, flname, incidentId):
    queries = ['Insert into payment_tie (charge_id, payment_id, amount, provider_id, office_id, posted_date) values ({}, {}, {}, {}, {}, "{}")', 'Insert into payment_tie (charge_id, payment_id, amount, provider_id, office_id, posted_date, claim_id, allowed, deductible_amount, coins_amount, adjustment_amount, copay_amount) values ({}, {}, "{}", {}, {}, "{}", {}, {}, "", "", "", "")', 'Update ledger set patient_portion = 0.00, unpaid = 0.00 where table_id = {} and table_name like "charge"', 'Select max(payment_tie_id) from payment_tie', 'Insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("U", "payment_tie", {}, "localhost", 2, "{}", 37)', 'Update payment_tie set history_id = {} where payment_tie_id = {}', 'Insert into ledger (table_name, patient_id, incident_id, account_id, provider_id, office_id, table_type, table_id, posted_date, code, credit, pay_amt, credit_balance, description, proc_date, audit_id, insurance_id) values ("payment", {}, {}, {}, {}, {}, 2, {}, "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}", {})', 'Select max(ledger_id) from ledger', 'Update ledger set history_id = {} where ledger_id = {}', 'Insert into ledger (patient_id, incident_id, account_id, provider_id, office_id, table_type, table_id, table_name, posted_date, code, prov, credit, proc_date, audit_id, record_num, type_id, insurance_id) values ({}, {}, {}, {}, 2, 4, {}, "write_off", "{}", "Commins Write-Off", "{}", "{}", "{}", "R", "{}", 12, "{}")', 'update charge set plan1_paid = "{}", allowed1 = "{}", wrt1 = "{}" where charge_id = {}', 'update ledger set patient_paid = {}, unpaid = "{}", patient_portion = "{}" where table_id = {} and table_name like "charge"', 'update ledger set insurance_paid = {} where table_id = {} and table_name like "charge"', 'Insert into ledger (table_name, patient_id, incident_id, account_id, provider_id, office_id, table_type, table_id, posted_date, code, credit, pay_amt, credit_balance, description, proc_date, audit_id) values ("payment", {}, {}, {}, {}, {}, 2, {}, "{}", "{}", "{}", "{}", "{}", "{}", "{}", "{}")']
    paymentId, providerId, procDate, claimId, insuranceId, prov, desc, incidentId, refNo, claimId, payType, officeId = payment_ins(mysql, conn, amount, personId, patientId, accountId, flname, chrgIds, incidentId)
    dts = str(datetime.datetime.now()).split('.')[0]
    payTypedesc = "{} #{} ({})".format(desc, refNo, amount)
    for i in chrgIds:
        if payType != 6:
            mysqlWorker(mysql, conn, queries[0].format(i, paymentId, chrgPmtamts[(chrgIds.index(i))], providerId, officeId, procDate), 2)
            mysqlWorker(mysql, conn, queries[11].format(chrgPmtamts[(chrgIds.index(i))], wrtOff[chrgIds.index(i)], wrtOff[chrgIds.index(i)], i), 2)
        else:
            mysqlWorker(mysql, conn, queries[1].format(i, paymentId, chrgPmtamts[(chrgIds.index(i))], providerId, officeId, procDate, claimId, chrgPmtamts[(chrgIds.index(i))]), 2)
            mysqlWorker(mysql, conn, queries[9].format(patientId, incidentId, accountId, providerId, i, dts, prov, wrtOff[(chrgIds.index(i))], procDate, paymentId, insuranceId), 2)
            mysqlWorker(mysql, conn, queries[10].format(chrgPmtamts[(chrgIds.index(i))], chrgPmtamts[(chrgIds.index(i))], wrtOff[(chrgIds.index(i))], i), 2)
            mysqlWorker(mysql, conn, queries[12].format(chrgPmtamts[(chrgIds.index(i))], i), 2)
        data = mysqlWorker(mysql, conn, queries[3], 0)
        mysqlWorker(mysql, conn, queries[4].format(data[0], dts), 2)
        historyId = get_hist_id(mysql, conn)
        mysqlWorker(mysql, conn, queries[5].format(historyId, data[0]), 2)
    if payType == 6:
        mysqlWorker(mysql, conn, queries[6].format(patientId, incidentId, accountId, providerId, officeId, paymentId, dts, desc, amount, amount, "0.00", payTypedesc, procDate, "AD", insuranceId), 2)
    else:
        mysqlWorker(mysql, conn, queries[13].format(patientId, incidentId, accountId, providerId, officeId, paymentId, dts, desc, amount, amount, "0.00", payTypedesc, procDate, "AD"), 2)
    data1 = mysqlWorker(mysql, conn, queries[7], 0)
    historyId = get_hist_id(mysql, conn)
    mysqlWorker(mysql, conn, queries[8].format(historyId, data1[0]), 2)
    if payType == 6:
        close_claim(mysql, conn, claimId)

def payment_ins(mysql, conn, amount, personId, patientId, accountId, flname, chrgIds, incidentId):
    queries = ['Insert into payment (account_id, reference_no, pop_payment_type, amount, date, description, proc_date, provider_id, office_id, payment_category_id, remain, ppit_id) values ({}, {}, {}, "{}", "{}", "{}", "{}", {}, {}, {}, "0.00", {})', 'Insert into payment (account_id, reference_no, pop_payment_type, amount, date, description, proc_date, provider_id, office_id, payment_category_id, remain) values ({}, {}, {}, "{}", "{}", "{}", "{}", {}, {}, {}, "0.00")', 'Select max(payment_id) from payment', "Insert into ledger (patient_id, incident_id, account_id, provider_id, office_id, table_type, table_id, table_name, posted_date, code, prov, description, proc_date, accept, ins_plan, ins_holder, audit_id, is_lab, insurance_id, form_amount) values ({}, {}, {}, {}, 2, 1, {}, 'InsuranceClaim', '{}', 'Insurance Claim', '{}', '{}', '{}', 1, '{}', '{}', '{}', 0, {}, '{}')", 'Select max(ledger_id) from ledger', 'Update claim set ledger_id = {}, claim_amount = "{}" where claim_id = {}', 'Update payment set ppit_id = {} where payment_id = {}']
    providerId, officeId, prov = get_provider(mysql, conn)
    dts = str(datetime.datetime.now()).split('.')[0]
    procDate = get_proc_date(mysql, conn, patientId)
    payType, paymentCategoryid, desc, ppitid, insuranceId, insCarrier = pmt_type(mysql, conn, amount, chrgIds, patientId)
    refNo = cc()
    if payType == 6:
        formId = enable_form(mysql, conn)
        claimId = create_claim(mysql, conn, formId, ppitid, amount)
        create_claim_tie(mysql, conn, chrgIds, claimId)
        mysqlWorker(mysql, conn, queries[0].format(accountId, refNo, payType, amount, dts, desc, procDate, providerId, officeId, paymentCategoryid, ppitid), 2)
    else:
        mysqlWorker(mysql, conn, queries[1].format(accountId, refNo, payType, amount, dts, desc, procDate, providerId, officeId, paymentCategoryid), 2)
    paymentId = mysqlWorker(mysql, conn, queries[2], 0)
    if payType == 6:
        mysqlWorker(mysql, conn, queries[6].format(ppitid, paymentId[0]), 2)
        mysqlWorker(mysql, conn, queries[3].format(patientId, incidentId, accountId, providerId, claimId, dts, prov, insCarrier, procDate, insCarrier, flname, prov, insuranceId, amount), 2)
        data = mysqlWorker(mysql, conn, queries[4], 0)
        mysqlWorker(mysql, conn, queries[5].format(data[0], amount, claimId), 2)
        create_pmt_claim_tie(mysql, conn, paymentId[0], claimId)
    else:
        claimId = 0
    return paymentId[0], providerId, procDate, claimId, insuranceId, prov, desc, incidentId, refNo, claimId, payType, officeId

def get_amt_f(mysql, conn, patientId, chrgAmts, chrgIds, chrgPmtamts, wrtOff, amount):
    query = 'select incident.incident_id, ledger.table_id, ledger.unpaid from incident left join ledger on incident.incident_id = ledger.incident_id where incident.patient_id = {} and ledger.unpaid > "0.00"'
    data = mysqlWorker(mysql, conn, query.format(patientId), 1)
    if data:
        incidentId = int(random.choice(data)[0])
        for i in data:
            if i[0] == incidentId:
                chrgIds.append(int(i[1]))
                chrgAmts.append(float(i[2]))
        percent = random.randint(50, 100) * .01
        remain = 1 - percent
        for i in chrgAmts:
            chrgPmtamts.append('{:.2f}'.format(float(i) * percent))
            wrtOff.append('{:.2f}'.format(float(i) * remain))
        for i in chrgPmtamts:
            amount += float(i)
        remain = 0
        for i in chrgPmtamts:
            remain = float(chrgAmts[chrgPmtamts.index(i)] - float(i))
            wrtOff.append(remain)
    else:
        incidentId = 0
    return chrgIds, chrgAmts, chrgPmtamts, wrtOff, amount, incidentId
def get_amt_s(mysql, conn, patientId, chargeAmts, chrgIds, chrgPmtamts, wrtOff, amount):
    query = ['select incident_id from incident where patient_id = {}', 'select ledger.debit, ifnull(sum(payment_tie.amount), 0) from ledger left join payment_tie on ledger.table_id = payment_tie.charge_id and table_name like "Charge"']
    

def get_good_data(mysql, conn):
    amount = 0.00
    incidentId = 0
    while amount == 0.00 and incidentId == 0:
        chrgAmts = []
        chrgIds = []
        chrgPmtamts = []
        wrtOff = []
        personId, patientId, accountId, flname = get_ppa(mysql, conn)
        chrgIds, chrgAmts, chrgPmtamts, wrtOff, amount, incidentId = get_amt_f(mysql, conn, patientId, chrgAmts, chrgIds, chrgPmtamts, wrtOff, amount)
    return chrgIds, chrgAmts, chrgPmtamts, wrtOff, amount, personId, patientId, accountId, flname, incidentId

def run():
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
    chrgIds, chrgAmts, chrgPmtamts, wrtOff, amount, personId, patientId, accountId, flname, incidentId = get_good_data(mysql, conn)
    pmt_tie_ins(mysql, conn, chrgIds, chrgAmts, chrgPmtamts, wrtOff, amount, personId, patientId, accountId, flname, incidentId)

if __name__ == '__main__':
    run()