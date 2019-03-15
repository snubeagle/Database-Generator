#! /usr/bin/python

import MySQLdb, random, string, datetime

def provider(mysql, conn):
    """Function to get provider_id"""
    query = 'Select user_id from user where isprovider = 1'
    tmpProv = mysqlWorker(mysql, conn, query, 1)
    providerIds = []
    for i in tmpProv:
        providerIds.append(i[0])
    providerId = random.choice(providerIds)
    return providerId

def csz(mysql, conn):
    """Function to add CityStateZip record for address"""
    queries = ['Insert into citystatezip (city, state, zip) values ("Lincoln", "NE", "{}")', 'Select max(citystatezip_id) from citystatezip', "Insert into macpractice_log.History (action, tablename, table_row, updated, ip, updated_user_id, table_info_id) values ('I', 'citystatezip', {}, '{}', 'localhost', 2, 14)", 'Select max(history_id) from macpractice_log.history', 'Update citystatezip set history_id = {} where citystatezip_id = {}']
    mysqlWorker(mysql, conn, queries[0].format(("685{:02}".format(random.randint(0, 16)))), 2)
    data = mysqlWorker(mysql, conn, queries[1], 0)
    cszid = data[0]
    dt = str(datetime.datetime.now()).split('.')[0]
    mysqlWorker(mysql, conn, queries[2].format(cszid, dt), 2)
    data = mysqlWorker(mysql, conn, queries[3], 0)
    historyId = data[0]
    mysqlWorker(mysql, conn, queries[4].format(historyId, cszid), 2)
    return cszid
    
def address(mysql, conn):
    """Function to create street address"""
    stype = ["Ave", "Street", "Place", "Circle", "Blvd", "Drive", "Road"]
    street = "{} {}{} {}".format((random.randint(100, 9999)), (''.join(random.choice(string.uppercase) for i in range(1))), (''.join(random.choice(string.lowercase) for i in range(4, 10))), (random.choice(stype)))
    cszid = csz(mysql, conn)
    query = ['Insert into address (address1, citystatezip_id) values ("{}", {})', 'Select max(address_id) from address']
    mysqlWorker(mysql, conn, query[0].format(street, cszid), 2)
    addressId = mysqlWorker(mysql, conn, query[1], 0)
    return addressId
    
def names(mysql, conn):
    """Function to generate distinct name"""
    name_list = ['Jim', 'James', 'Billy', 'Tim', 'Walter', 'Jerald', 'Tony', 'Anthony', 'William', 'Sean', 'Seth', 'Spectre', 'Megan', 'Meagan', 'Tom', 'Ryan', 'Brian', 'Rian', 'Bryan', 'Ted', 'Jack', 'Chris', 'Albert', 'Miriam', 'Mirian', 'Max', 'Maxwell', 'Alton', 'Becky', 'Brad', 'Brandon', 'Charles', 'Candice', 'Clarence', 'David', 'Donald', 'Daffy', 'Donel', 'Edward', 'Elizabeth', 'Echo', 'Frank', 'Francis', 'Florence', 'Faust', 'Gordon', 'George', 'Geoff', 'Georgina', 'Harry', 'Harold', 'Hessia', 'Ingrid', 'Isbeth', 'Isabella', 'Ivan', 'Jared', 'Josephine', 'Kevin', 'Karol', 'Kapper', 'Lumen', 'Destro', 'Morgan', 'Morganna', 'Mordrid', 'Nancy', 'Norman', 'Hecktor', 'Oscar', 'Oliver', 'Patrick', 'Paul', 'Peter', 'Patricia', 'Quistis', 'Samantha', 'Susan', 'Sherie', 'Umbra', 'Vicktor', 'Vigo', 'Vega', 'Verra', 'Vincent', 'Zelda', 'Zach', 'Caroline', 'Sasha', 'Tiz', 'Edea', 'Agnes', 'Ramses', 'Dovakin', 'Dart', 'Rose', 'Arthur', 'Alexander', 'Eric', 'Cisco', 'Jessica', 'Teresa', 'Light', 'Andrea', 'Lyle', 'Terra', 'Sabin', 'Edgar']
    first = random.choice(name_list)
    last = random.choice(name_list)
    query = 'Select person_id from person where first like "{}" and last like "{}"'.format(first, last)
    data = mysqlWorker(mysql, conn, query, 1)
    if data:
        names(mysql, conn)
    return first, last

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
    """Function to get last inserted history record"""
    history_id = 'Select max(history_id) from macpractice_log.history'
    data = mysqlWorker(mysql, conn, history_id, 0)
    return data[0]

def let_it_begin():
    conn = MySQLdb.Connect(db="macpractice", host="localhost", user="root")
    mysql = conn.cursor()
#    j = int(raw_input("Input number of patients you would like to create? "))
    queries = ['Set foreign_key_checks = 0', 'Insert into address (address1, citystatezip_id) values ("{}", {})', 'Select max(address_id) from address', 'Insert into person (last, first, ssn, address_id, birthday, phone1, flag_signature_on_file, flag_release_of_info, email) values ("{}", "{}", "{}", "{}", "{}", "{}", 1, 1, "{}")', 'select max(person_id) from person', 'Insert into account (primary_person_id, secondary_person_id, provider_id, office_id) values ({}, 0, {}, 4178)', 'select max(account_id) from account', 'Insert into patient (person_id, account_id, display_id, primary_patient_relationship_id, secondary_patient_relationship_id, provider_id) values ("{}", "{}", "{}-1", 8, 0, {})', 'update account set primary_person_id = "{}" where account_id = "{}"', 'Select max(patient_id) from patient', 'Insert into macpractice_log.History (action, tablename, table_row, updated, ip, updated_user_id, table_info_id, affects_patient_id) values ("I", "person", {}, "{}", "localhost", 2, 50, {})', 'Update person set history_id = {} where person_id = {}', 'Insert into macpractice_log.History (action, tablename, table_row, updated, ip, updated_user_id, table_info_id, affects_patient_id) values ("I", "patient", {}, "{}", "localhost", 2, 44, {})', 'Update patient set history_id = {} where patient_id = {}', 'Insert into macpractice_log.history (action, tablename, table_row, updated, ip, updated_user_id, table_info_id, affects_patient_id) values ("I", "account", {}, "{}", "localhost", 2, 1, {})', 'Update account set history_id = {} where account_id = {}', 'Set foreign_key_checks = 1']
    mysql.execute(queries[0])
    names = [('Amber','Bertram','Downtown Endodontic Group LLC','info@dtendo.com'), ('Hilary','Butsch','Downtown Endodontic Group LLC','info@dtendo.com'), ('Diane','Etter','Downtown Endodontic Group LLC','info@dtendo.com'), ('John','Johnson','Downtown Endodontic Group LLC','info@dtendo.com'), ('Elizabeth','McCourt','Downtown Endodontic Group LLC','info@dtendo.com'), ('Ann','Carr','Harkins and Associates, Inc','anncarrccfc@msn.com'), ('Jennifer','Jockers','Harkins and Associates, Inc','jjockers33@yahoo.com'), ('Lori','Corbin','Imagine Smiles','lori@imagine-smiles.com'), ('Heidi','Green','Imagine Smiles','heidi@imagine-smiles.com'), ('Judith','Callahan','Periodontal Associates of Georgia','drsjacquotmcdevitt@gmail.com'), ('Nina','Grasso','Periodontal Associates of Georgia','drsjacquotmcdevitt@gmail.com'), ('Dianne','Henry','Periodontal Associates of Georgia','drsjacquotmcdevitt@gmail.com'), ('John','Jacquot','Periodontal Associates of Georgia','drsjacquotmcdevitt@gmail.com'), ('Carolyn','Miller','Periodontal Associates of Georgia','drsjacquotmcdevitt@gmail.com'), ('Cynthia','Wilkins','Periodontal Associates of Georgia','drsjacquotmcdevitt@gmail.com'), ('Amber','Dorn','Samer Saiedy MD, PA - Maryland Vascular Specialists','ADorn@mdvascularspecialists.com'), ('Scott','Hughey','Samer Saiedy MD, PA - Maryland Vascular Specialists','SHughey@mdvascularspecialists.com'), ('Holly','Miller','Samer Saiedy MD, PA - Maryland Vascular Specialists','HMiller@mdvascularspecialists.com'), ('Steve','Gregg','Steven W Gregg, DDS','stevenwgreggdds@gmail.com'), ('January','Dillard','East Cove Psychiatric Services','januaryd.psr@gmail.com'), ('Tammi','Bockheim','Greentree Dental','info@greentreedental.net'), ('Dr. Douglas','Koch ','Greentree Dental','drdoug@greentreedental.net'), ('Jana','Koch ','Greentree Dental','info@greentreedental.net'), ('Scott ','Warner','PROSPECT - Desert Vein Clinic','scott@desertveinclinic.com'), ('Ted','Fisher','PROSPECT - Desert Vein Clinic','tyfmdpc@gmail.com'), ('Terrie','Vandiver','PROSPECT - Desert Vein Clinic','terrie@desertveinclinic.com'), ('Cherie','Salisbury','Idaho Sports Medicine Institute',' '), ('Matthew','Varner',' Institute for Orthopedics & Chiropractic PLLC',' '), ('Christina','Love Stone',' Institute for Orthopedics & Chiropractic PLLC',''), ('Alex','Jones','Dental Product Owner',''), ('Amanda','Hornburg','Billing Product Owner',''), ('Amos','Meek','Support Specialist',''), ('Andrea','Hummel','Design Specialist',''), ('Andy','Brace','Software Engineer',''), ('Ashley','Stutheit','EMR/Core/Meaningful Use Product Owner',''), ('Becky','Henderson','Lead Software Engineer',''), ('Ben','Weese','Quality Assurance',''), ('Bob','Davis','Product Manager',''), ('Brent','Peekenschneider','Quality Assurance',''), ('Brett','Lowe','EDI Specialist',''), ('Brian','Niemeyer','Lab/HL7 Supervisor',''), ('Carlos','Gonzalez','Digital Radiography/Imaging Specialist',''), ('Chris','Novak','Director of Support',''), ('Christopher','Beaner','Sales Associate',''), ('Cole','Kurkowsky','Software Engineer Intern',''), ('Corey','Jergensen','Software Engineer',''), ('Deanna','Swearingen','Billing Specialist',''), ('Dmitri','Lombard','Quality Assurance',''), ('Doug','Fenn','Sales Associate',''), ('Dustin','Johns','Software Engineer',''), ('Eric','Sewell','MacPractice Corporate Trainer',''), ('Erik','Wrenholt','Director of Product Development',''), ('Erin','Brown','ZenDesk Coordinator',''), ('James','Brophy','Quality Assurance',''), ('James','Dingwell','Facilities Coordinator',''), ('Jason','Flaherty','Software Engineer',''), ('Jeff','See','Software Engineer',''), ('Jennifer','Loecker','Software Engineer',''), ('Jeremy','Thiessen','EHR Product Owner',''), ('Jim','Kimes','Quality Assurance',''), ('Joe','DeMarco','Quality Assurance',''), ('John','Kinnison','Clerical Product Owner',''), ('Jonathan','Gordan','Conversions Specialist',''), ('Jessica','Maas','Chat Specialist',''), ('Josh','Jay','Software Engineer',''), ('Joshua','Wiltshire','Support Supervisor',''), ('Katie','Hamilton','Conversions Specialist',''), ('Kelley','Cavanagh','EDI Manager',''), ('Kelly','Krueger','Director of Sales',''), ('Kevin','Lanik','Software Engineer Manager',''), ('Kun','Lu','Lead Software Engineer',''), ('Kyle','Brown','Software Engineer',''), ('Kyria','Spooner','Traveling Corporate Trainer',''), ('Leighton','Heusinger','Traveling Corporate Trainer',''), ('Liz','Lohse','Enrollments Specialist',''), ('Logan','Young','Support Quality Control',''), ('Loretta','Young','Director of Human Resources',''), ('Luke','Sticka','Template Specialist',''), ('Lyle','Dick','Data Specialist',''), ('Mahender','Marrivagu','Software Engineer Intern',''), ('Marilyn','Slattery','Billing Specialist',''), ('Mark','Hoefler','EMR Manager',''), ('Mat','Castelo','Software Engineer Intern',''), ('Maurice','Shinall','Quality Assurance',''), ('Micah','Walles','Lead Software Engineer',''), ('Mike','Ramos','Accounting Specialist',''), ('Nathan','Hughes','Support Manager',''), ('Nicholas','Stanley','Quality Assurance',''), ('Nick','Middlekauff','Client Relations Specialist',''), ('Nick','Snyder','Quality Assurance',''), ('Pan','Yi','Software Engineer',''), ('Patrick','Clyne','President',''), ('Phil','Johnson','Quality Assurance',''), ('Phillip','Rapp','Software Engineer',''), ('Prachi','Ghadge','Software Engineer',''), ('Prasanna','Challa','Software Engineer',''), ('Rich','Prokasky','EDI Supervisor',''), ('Rob','Pickel','Quality Assurance',''), ('Rob','Rothe','Lead Software Engineer',''), ('Robin','Watkins','Traveling Corporate Trainer',''), ('Roger','Crum','EMR Supervisor',''), ('Rosy','Sunuwar','Software Engineer',''), ('Ryan','Feather','Lead Software Engineer',''), ('Ryan','McCullough','Support Specialist',''), ('Sara','Miller','Enrollments Specialist',''), ('Sarah','Dempsey','Support Supervisor',''), ('Seth','Yant','Implementation Coordinator',''), ('Shakthi','Bachala','Software Engineer',''), ('Shelby','Wertz','Support Specialist',''), ('Sonja','Egger','Training Supervisor',''), ('Ted','Jedlicka','Director of Client Relations',''), ('Tiffany','Yanagida','Meaningful Use Specialist',''), ('Tim','Carpenter','Template Specialist',''), ('Todd','Brown','Design Specialist',''), ('Tom','Lombard','Quality Assurance Manager',''), ('Tracy','Zhao','Software Engineer',''), ('Travis','Hort','Support Specialist','')]
#    while j > 0:
    for i in names:
        first = i[0]
        last = i[1]
        address1 = i[2]
        email = i[3]
        dts = str(datetime.datetime.now()).split('.')[0]
        providerId = provider(mysql, conn)
#        first, last = names(mysql, conn)
        cszId = csz(mysql, conn)
#        address1 = address(mysql, conn)
        mysqlWorker(mysql, conn, queries[1].format(address1, cszId), 2)
        data = mysqlWorker(mysql, conn, queries[2], 1)
        add_id = int(data[0][0])
        fullssn = "{:03}-{:02}-{:04}".format((random.randint(0, 999)), (random.randint(0, 99)), (random.randint(0, 9999)))
        birth = "19{}-{:02}-{:02}".format((random.randint(60, 99)), (random.randint(1, 12)), (random.randint(1, 28)))
        full_phone = '{:03} {:03}-{:04}'.format(random.randint(100, 999), random.randint(100, 999), random.randint(1000, 9999))
        mysqlWorker(mysql, conn, queries[3].format(last, first, fullssn, add_id, birth, full_phone, email), 2)
        data = mysqlWorker(mysql, conn, queries[4], 0)
        primaryPersonid = int(data[0])
        mysqlWorker(mysql, conn, queries[5].format(primaryPersonid, providerId), 2)
        data = mysqlWorker(mysql, conn, queries[6], 0)
        account_id = int(data[0])
        mysqlWorker(mysql, conn, queries[3].format(last, first, fullssn, add_id, birth, full_phone, email), 2)
        data = mysqlWorker(mysql, conn, queries[4], 0)
        person_id = int(data[0])
        mysqlWorker(mysql, conn, queries[7].format(person_id, account_id, account_id, providerId), 2)
        data = mysqlWorker(mysql, conn, queries[9], 0)
        patient_id = int(data[0])
        mysqlWorker(mysql, conn, queries[10].format(person_id, dts, patient_id), 2)
        history_id = get_hist_id(mysql, conn)
        mysqlWorker(mysql, conn, queries[11].format(history_id, person_id), 2)
        mysqlWorker(mysql, conn, queries[12].format(patient_id, dts, patient_id), 2)
        history_id = get_hist_id(mysql, conn)
        mysqlWorker(mysql, conn, queries[13].format(history_id, patient_id), 2)
        mysqlWorker(mysql, conn, queries[14].format(account_id, dts, patient_id), 2)
        history_id = get_hist_id(mysql, conn)
        mysqlWorker(mysql, conn, queries[15].format(history_id, account_id), 2)
#        j -= 1
    mysqlWorker(mysql, conn, queries[16], 2)

if __name__ == '__main__':
    let_it_begin()
