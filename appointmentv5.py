import os, MySQLdb, random, time, datetime, hashlib

def work():
    """Main Function to get good dates, get good times, gather relavent fields, etc"""
    conn = MySQLdb.Connection(db="macpractice", host="localhost", user="root", passwd="")
    mysql = conn.cursor()
    year = int(raw_input('What year would you like to create appointments for? '))
    minOptions = [0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
    durOptions = [15, 20, 25, 30, 35, 40, 45, 50, 55]
    days = days1(year)
    resource = resourceChoice(mysql, conn)
    for a in days:
        for x in range(10):
            dotw = a.weekday()
            resourceId = random.choice(resource)
            aptStart, aptEnd = startTimepossibilities(mysql, conn,resourceId, dotw, a, durOptions)
            providerId = provider(mysql, conn)
            patientId = patient(mysql, conn)
            aptStatus = apptStatus(mysql, conn)
            aptType = apptType(mysql, conn)
            appointmentId = appt(mysql, conn, aptStart, aptEnd, providerId, patientId, aptStatus, aptType, resourceId)
            historyId = apptHistoryid(mysql, conn, appointmentId, patientId)
            query = 'update appointment set history_id = {} where appointment_id = {}'.format(historyId, appointmentId)
            mysqlWorker(mysql, conn, query, 2)

def apptHistoryid(mysql, conn, appointmentId, patientId):
    """Function to add history record for appointment"""
    dt = str(datetime.datetime.now()).split('.')[0]
    query = 'Insert into macpractice_log.history (action, tablename, table_row, updated, affects_patient_id, history_description, ip, updated_user_id, table_info_id) values ("I", "appointment", {}, "{}", {}, "Saved New Appointment", "localhost", 2, 8)'.format(appointmentId, dt, patientId)
    mysqlWorker(mysql, conn, query, 2)
    query = 'Select max(history_id) from macpractice_log.history'
    historyId = mysqlWorker(mysql, conn, query, 0)
    return historyId[0]

def appt(mysql, conn, aptStart, aptEnd, providerId, patientId, aptStatus, aptType, resourceId):
    """Insert appointment record into database"""
    query = "Insert into appointment (patient_id, resource_id, appointment_type_id, provider_id, start, end, created, appointmentstatus_id, created_user_id, updated_user_id, last_updated, status_last_updated, flag_autoremind) values ({}, {}, {}, '{}', '{}', '{}', '{}', {}, 2, 2, '{}', '{}', 0)".format(patientId, resourceId, aptType, providerId, aptStart, aptEnd, aptStart, aptStatus, aptStart, aptStart)
    mysqlWorker(mysql, conn, query, 2)
    query = 'Select max(appointment_id) from appointment'
    appointmentId = mysqlWorker(mysql, conn, query, 0)
    return appointmentId[0]

def provider(mysql, conn):
    """Function to get provider_id"""
    query = 'Select user_id from user where isprovider = 1'
    tmpProv = mysqlWorker(mysql, conn, query, 1)
    providerIds = []
    for i in tmpProv:
        providerIds.append(i[0])
    providerId = random.choice(providerIds)
    return providerId

def patient(mysql, conn):
    """Function to get patient_id"""
    query = 'Select max(patient_id) from patient'
    maxPatientid = mysqlWorker(mysql, conn, query, 0)
    patientId = random.randint(2, maxPatientid[0])
    return patientId

def apptStatus(mysql, conn):
    query = 'Select appointmentstatus_id from appointmentstatus where appointmentstatus_id <> 1 and a_description not like "To Reschedule" and a_description not like "Missed" and a_description not like "Cancelled"'
    data = mysqlWorker(mysql, conn, query, 1)
    return random.choice(data)[0]

def apptType(mysql, conn):
    query = 'Select appointment_type_id from appointment_type where appointment_type_id > 1'
    data = mysqlWorker(mysql, conn, query, 1)
    if data:
        if len(data) > 3:
            aptType = random.choice(data)[0]
            return aptType
        else:
            apptTypecreate(mysql, conn)
            aptType = apptType(mysql, conn)
            return aptType
    else:
        apptTypecreate(mysql, conn)
        aptType = apptType(mysql, conn)
        return aptType

def apptTypecreate(mysql, conn):
    types = ['Recall', 'Cleaning', 'Follow-up', 'New Patient']
    queries = ['Insert into appointment_type (appointment_desc) values ("{}")', 'Select max(appointment_type_id) from appointment_type', 'Update appointment_type set history_id = {} where appointment_type_id = {}', 'Select appointment_type_id, history_id, appointment_desc from appointment_type where appointment_desc = "{}"', 'Insert into a_appointment_type (appointment_type_id, history_id, appointment_desc) values ({}, {}, "{}")']
    for i in types:
        mysqlWorker(mysql, conn, queries[0].format(i), 2)
        aptTypeid = mysqlWorker(mysql, conn, queries[1], 0)
        aptHistoryid = aptHistory(mysql, conn, aptTypeid[0])
        mysqlWorker(mysql, conn, queries[2].format(aptHistoryid, aptTypeid[0]), 2)
        aAppointmenttype = mysqlWorker(mysql, conn, queries[3].format(i), 1)
        mysqlWorker(mysql, conn, queries[4].format(aAppointmenttype[0][0], aAppointmenttype[0][1], aAppointmenttype[0][2]), 2)

def aptHistory(mysql, conn, aptTypeid):
    dt = str(datetime.datetime.now()).split('.')[0]
    queries = ['insert into macpractice_log.history (action, tablename, table_row, ip, updated_user_id, updated, table_info_id) values ("I", "appointment_type", {}, "localhost", 2, "{}", 9)', 'select max(history_id) from macpractice_log.history']
    mysqlWorker(mysql, conn, queries[0].format(aptTypeid, dt), 2)
    aptHistoryid = mysqlWorker(mysql, conn, queries[1], 0)
    return aptHistoryid[0]

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

def resourceChoice(mysql, conn):
    """Function to get resource_ids"""
    query = 'select resource_id from resource where resource_id > 1'
    resources = []
    data = mysqlWorker(mysql, conn, query, 1)
    for i in data:
        resources.append(int(i[0]))
    return resources

def startTimepossibilities(mysql, conn,resourceId, dotw, a, durOptions):
    queries = ['select open{}1, close{}1, open{}2, close{}2 from resource where resource_id = {}']
    data = mysqlWorker(mysql, conn, queries[0].format(dotw, dotw, dotw, dotw, resourceId), 1)
    for i in data:
        start1 = i[0]
        end1 = i[1]
        start2 = i[2]
        end2 = i[3]
    time1 = Generator(start1, end1, datetime.timedelta(minutes=5), 1)
    time2 = Generator(start2, end2, datetime.timedelta(minutes=5), 1)
    time1 = [x for x in time1 if not ((x + datetime.timedelta(minutes=55)) >= end1)]
    time2 = [x for x in time2 if not ((x + datetime.timedelta(minutes=55)) >= end2)]
    times = time1 + time2
    dur = random.choice(durOptions)
    aptStart = datetime.datetime.combine(a.date(), random.choice(times).time())
    aptEnd = aptStart + datetime.timedelta(minutes=dur)
    aptStart, aptEnd = overlaps(mysql, conn, resourceId, a, times, dur, aptStart, aptEnd)
    return aptStart, aptEnd

def Generator(start, end, delta, y):
    if y == 1:
        times = []
        while start.time() < end.time():
            times.append(start)
            start += delta
        return times
    else:
        dates = []
        while start < end:
            dates.append(start)
            start += delta
        return dates

def overlaps(mysql, conn, resourceId, a, times, dur, aptStart, aptEnd):
    query = 'Select start, end from appointment where resource_id = {} and date(start) like "{}"'
    data = mysqlWorker(mysql, conn, query.format(resourceId, a.date()), 1)
    for i in data:
        while i[0] <= aptEnd <= i[1]:
            times = [x for x in times if not (x == aptStart)]
            aptStart = datetime.datetime.combine(a.date(), random.choice(times).time())
            aptEnd = aptStart + datetime.timedelta(minutes=dur)
        while i[0] <= aptStart <= i[1]:
            times = [x for x in times if not (x == aptStart)]
            aptStart = datetime.datetime.combine(a.date(), random.choice(times).time())
        aptEnd = aptStart + datetime.timedelta(minutes=dur)
    return aptStart, aptEnd
    
def days1(year):
    daysOff = [(12, 25), (7, 4), (10, 12), (9, 7), (1, 19), (2, 16), (11, 26), (11, 27), (12, 24), (5, 24), (1, 1)]
    holidays = [datetime.datetime(year, d[0], d[1]) for d in daysOff]
    allDays = Generator(datetime.datetime(year, 01, 02), datetime.datetime((year + 1), 01, 01), datetime.timedelta(days=1), 2)
    allDays = [x for x in allDays if not (x.weekday() >= 5)]
    for i in holidays:
        allDays = [x for x in allDays if not (x == i)]
    return allDays

if __name__ == '__main__':
    work()