import pandas
import pymongo
import neo4j
import csv
import mysql.connector

k = []
k1 = []
k2 = []
k3 = []
v =[]
v1 = []
v2 = []
v3 = []
c = []
c1 = []
c2 = []
c3 = []


connect = pymongo.MongoClient("mongodb://localhost:27017")
database = connect["testdb"]
for j in ('250','500','750','1000'):
    xk = []
    xk1 = []
    xk2 = []
    xk3 = []
    dbread = pandas.read_csv('data/ExportCSv'+j+'.csv')
    dbread = dbread.to_dict(orient="records")
    code_ape_coll = database['code_ape']
    company_coll = database['comapny']
    person_coll = database['person']
    legal_status_coll = database['legal_status']
    for row in dbread:
        code_ape_coll.insert_one({'code_ape':row['code_ape'],'siren':row['siren']})
        company_coll.insert_one({'name':row['company_name'],'siren':row['siren'],'address':row['address'],'postal_code':row['postal_code'],'town':row['town'],'country':row['country'],'reg_date':row['reg_date']})
        person_coll.insert_one({'first_name':row['first_name'],'last_name':row['last_name'],'email':row['email_address'],'siren':row['siren']})
        legal_status_coll.insert_one({'legal_status':row['legal_status'],'activity':row['activity_area'],'siren':row['siren']})
    for i in range(0,31):
        mydoc = person_coll.find({}).explain()
        time = mydoc['executionStats']['executionTimeMillis']
        xk.append(time)
        mydoc = company_coll.find({"country":"Italy"}).explain()
        time = mydoc['executionStats']['executionTimeMillis']
        xk1.append(time)
        query3 = legal_status_coll.find({'legal_status':'Accounting'})
        query3_time = query3.explain()
        query3_sirens = []
        for x in query3:
            query3_sirens.append(x['siren'])
        y = company_coll.find({'siren':{'$in':query3_sirens}},{'name':1,'siren':1,'postal_code':1,'_id':0}).explain()
        time = y['executionStats']['executionTimeMillis'] + query3_time['executionStats']['executionTimeMillis']
        xk2.append(time)
        query4_codes = code_ape_coll.find({'code_ape':{'$gt':69999,'$lt':80000}})
        query4_time = query4_codes.explain()
        query4_sirens = []
        for x in query4_codes:
            query4_sirens.append(x['siren'])
        query4_person = person_coll.find({'siren':{'$in':query4_sirens}},{'first_name':1,'last_name':1,'address':1,'_id':0}).explain()
        query4_company = company_coll.find({'siren':{'$in':query4_sirens}},{'country':1,'address':1,'_id':0}).explain()
        time = query4_time['executionStats']['executionTimeMillis']+query4_person['executionStats']['executionTimeMillis']+query4_company['executionStats']['executionTimeMillis']
        xk3.append(time)
    k3.append(xk3)
    k2.append(xk2)
    k1.append(xk1)
    k.append(xk)
    code_ape_coll.drop()
    company_coll.drop()
    person_coll.drop()
    legal_status_coll.drop()
connect.close()


connect = neo4j.GraphDatabase.driver("neo4j://localhost:7687",auth=('neo4j','password'))
database = connect.session()
for i in ('250','500','750','1000'):
    xv = []
    xv1 = []
    xv2 = []
    xv3 = []
    database.run("create constraint on (c:company) assert c.siren is unique")
    database.run("create constraint on (c:person) assert c.email is unique")
    database.run("create constraint on (c:apes) assert c.code_ape is unique")
    database.run("create constraint on (c:status) assert c.legal_status is unique")
    database.run("using periodic commit 2000 load csv with headers from 'file:///ExportCSV"+i+".csv' as line FIELDTERMINATOR',' with line create (b:company {siren: tointeger(line.siren)}) merge (c:person{email: (line.email_address)}) merge (d:apes{code_ape:tointeger(line.code_ape)}) merge (e:status {legal_status:line.legal_status}) set b.name = line.company_name, b.address = line.address, b.post_code = tointeger(line.postal_code) , b.town = line.town, b.country = line.country , b.reg_date=line.reg_date, c.first_name = line.first_name, c.last_name = line.last_name, e.activity = line.activity_area;")
    database.run("using periodic commit 2000 load csv with headers from 'file:///ExportCSV"+i+".csv' as line FIELDTERMINATOR',' with line match (c:company {siren: tointeger(line.siren)}) match (p:person{email: (line.email_address)}) match (a:apes{code_ape:tointeger(line.code_ape)}) match (s:status {legal_status:line.legal_status}) merge (a)-[:aconnectedtoc]->(c) merge (s)-[:sconnectedtoc]->(c) merge (p)-[:pconnectedtoc]->(c)")
    for i in range(0,31):
        num = database.run("MATCH (n:person) return n")
        time = num.consume()
        xv.append(int(time.result_available_after))
        num = database.run("MATCH (n:company) where n.country='Italy' return n")
        time = num.consume()
        xv1.append(int(time.result_available_after))
        num = database.run("MATCH (s:status)-[r:sconnectedtoc]->(c:company) where s.legal_status = 'Accounting' return c.name, c.siren, c.post_code")
        time = num.consume()
        xv2.append(int(time.result_available_after))
        num = database.run("match (a:apes)-[:aconnectedtoc]->(c:company)<-[:pconnectedtoc]-(p:person) where a.code_ape>69999 and a.code_ape<80000 return a.code_ape, c.country, p.first_name, p.last_name, c.address")
        time = num.consume()
        xv3.append(int(time.result_available_after))
    v.append(xv)
    v1.append(xv1)
    v2.append(xv2)
    v3.append(xv3)
    database.run("match (n) call {with n optional MATCH (n)-[r]-() DELETE n,r} in transactions of 2000 rows")
    database.run("drop constraint on (c:apes) assert c.code_ape is unique")
    database.run("drop constraint on (c:person) assert c.email is unique")
    database.run("drop constraint on (c:company) assert c.siren is unique")
    database.run("drop constraint on (c:status) assert c.legal_status is unique")
database.close()

'''
connect = mysql.connector.connect(user='root',password='mypass',port=3306)
mariadb_cursor = connect.cursor()
num_4_sql = 0
xc = []
xc1 = []
xc2 = []
xc3 = []
for j in ('250','500','750','1000'):
    mariadb_cursor.execute('create database projdb')
    mariadb_cursor.execute('use projdb')
    dbread = pandas.read_csv('data/ExportCSV'+j+'.csv')
    dbread = dbread.itertuples()
    mariadb_cursor.execute("create table company (company_name varchar(100), siren int,address varchar(150), postal_code int,town varchar(50), country varchar(50), reg_date varchar(20),primary key(siren))")
    mariadb_cursor.execute("create table person (email_address varchar(150),first_name varchar(50), last_name varchar(50), siren int,primary key(email_address),foreign key(siren) references company(siren))")
    mariadb_cursor.execute("create table code_ape_table (siren int, code_ape int,primary key(siren),foreign key(siren) references company(siren))")
    mariadb_cursor.execute("create table legal_status_table (siren int, legal_status varchar(50), activity_area varchar(150),primary key(siren),foreign key(siren) references company(siren))")
    for rows in dbread:
        insert_query = ("insert into company (company_name,siren,address,postal_code,town,country,reg_date) values ('"+rows.company_name+"',"+str(rows.siren)+",'"+rows.address+"',"+str(rows.postal_code)+",'"+rows.town+"','"+rows.country+"','"+str(rows.reg_date)+"')")
        mariadb_cursor.execute(insert_query)
        insert_query2 = ("insert into person (email_address,first_name,last_name,siren) values ('"+rows.email_address+"','"+rows.first_name+"','"+rows.last_name+"',"+str(rows.siren)+")")
        mariadb_cursor.execute(insert_query2)
        insert_query3 = ("insert into code_ape_table (siren, code_ape) values ("+str(rows.siren)+","+str(rows.code_ape)+")")
        mariadb_cursor.execute(insert_query3)
        insert_query4 = ("insert into legal_status_table (siren, legal_status, activity_area) values ("+str(rows.siren)+",'"+rows.legal_status+"','"+rows.activity_area+"')")
        mariadb_cursor.execute(insert_query4)
    connect.commit()
    mariadb_cursor.execute("set profiling = 1;")
    for i in range(0,31):
        mariadb_cursor.execute("select * from person")
        mariadb_cursor.fetchall()
        mariadb_cursor.execute("show profiles;")
        cm = mariadb_cursor.fetchall()
        #print(cm)
        if num_4_sql < 14:
            xc.append(cm[i][1])
        elif num_4_sql == 14:
            xc.append(cm[14][1])
        else:
            xc.append(cm[14][1])
        num_4_sql += 1
    for i in range(0,31):
        mariadb_cursor.execute("select * from company where country = 'Italy'")
        mariadb_cursor.fetchall()
        mariadb_cursor.execute("show profiles;")
        cm = mariadb_cursor.fetchall()
        #print(cm)
        xc1.append(cm[14][1])
        num_4_sql += 1
    for i in range(0,31):
        mariadb_cursor.execute("select company.company_name, company.siren, company.postal_code from legal_status_table ,company where legal_status_table.siren = company.siren and legal_status_table.legal_status='Accounting'")
        mariadb_cursor.fetchall()
        mariadb_cursor.execute("show profiles;")
        cm = mariadb_cursor.fetchall()
        #print(cm)
        xc2.append(cm[14][1])
        num_4_sql += 1
    for i in range(0,31):
        mariadb_cursor.execute("select code_ape_table.code_ape, person.first_name, person.last_name, company.country, company.address from code_ape_table,company, person where code_ape_table.siren = company.siren and company.siren = person.siren and code_ape_table.code_ape>69999 and code_ape_table.code_ape<80000")
        mariadb_cursor.fetchall()
        mariadb_cursor.execute("show profiles;")
        cm = mariadb_cursor.fetchall()
        #print(cm)
        xc3.append(cm[14][1])
        num_4_sql += 1
    mariadb_cursor.execute("set profiling = 0")
    mariadb_cursor.execute('drop database projdb')
connect.close()
funnum = 0
for i in range(4):
    ac = []
    ac1 = []
    ac2 = []
    ac3 = []
    for j in range(31):
        ac.append(int(xc[funnum]*1000))
        ac1.append(int(xc1[funnum]*1000))
        ac2.append(int(xc2[funnum]*1000))
        ac3.append(int(xc3[funnum]*1000))
        funnum += 1
    c.append(ac)
    c1.append(ac1)
    c2.append(ac2)
    c3.append(ac3)
'''

print(k)
print(k1)
print(k2)
print(k3)
print(v)
print(v1)
print(v2)
print(v3)
print(c)
print(c1)
print(c2)
print(c3)

'''
with open('newdoc.csv','w',newline='') as file:
    mywrite = csv.writer(file, delimiter=',')
    mywrite.writerows(k)
    mywrite.writerows(k1)
    mywrite.writerows(k2)
    mywrite.writerows(k3)
    mywrite.writerows(v)
    mywrite.writerows(v1)
    mywrite.writerows(v2)
    mywrite.writerows(v3)
    mywrite.writerows(c)
    mywrite.writerows(c1)
    mywrite.writerows(c2)
    mywrite.writerows(c3)'''

