from astesriskDB import asteriskdb
from datetime import datetime

start = datetime.now()


def query1():
    select = asteriskdb.select("""
SELECT c.recid                                      AS id,
       DATE_FORMAT(c.calldate, '%d-%m-%Y %H:%i:%S') as CallDAte,
       RIGHT(c.src, 9)                              AS Mobile,
       c.uniqueid                                   AS Callid
FROM asterisk.cdr c
WHERE c.lastapp = 'BackGround'
  AND c.calldate BETWEEN (NOW() - INTERVAL 10 MINUTE) AND (NOW() - INTERVAL 2 MINUTE)
  AND LENGTH(c.src) > 8
  AND LEFT(RIGHT(c.src, 9), 1) = 5
  AND c.src NOT IN ('asterisk', 'Anonymous')
ORDER BY c.calldate DESC
    """)
    mob = []
    for item in select:
        mob.append(item[2])
    namlist = list(set(mob))
    return namlist


def query2(data):
    select1 = asteriskdb.select(f"""
    SELECT c.recid                                      AS id,
           DATE_FORMAT(c.calldate, '%d-%m-%Y %H:%i:%S') as CallDAte,
           RIGHT(c.src, 9)                              AS Mobile,
           c.uniqueid
    FROM asterisk.cdr c
    WHERE c.calldate BETWEEN NOW() - INTERVAL 120 MINUTE AND NOW() - INTERVAL 11 MINUTE
    AND LEFT(RIGHT(c.src, 9), 1) = 5
    ORDER BY c.calldate DESC
    """)

    mob1 = []
    for item in select1:
        mob1.append(item[2])
    namlist1 = list(set(mob1))

    mobiles = []
    for item in data:
        if item not in namlist1:
            mobiles.append(item)
    return mobiles


def query3(data):
    select2 = asteriskdb.select(f"""
    SELECT c.recid                                      AS id,
           DATE_FORMAT(c.calldate, '%d-%m-%Y %H:%i:%S') as CallDAte,
           RIGHT(c.dst, 9)                              AS Mobile,
           c.uniqueid
    FROM asterisk.cdr c
    WHERE c.calldate BETWEEN NOW() - INTERVAL 120 MINUTE AND NOW() - INTERVAL 11 MINUTE
    AND LEFT(RIGHT(c.dst, 9), 1) = 5
    ORDER BY c.calldate DESC
    """)

    mob2 = []
    for item in select2:
        mob2.append(item[2])
    namlist2 = list(set(mob2))

    mobiles = []
    for item in data:
        if item not in namlist2:
            mobiles.append(item)
    return mobiles


lastnumlist = query3(query2(query1())) + ['551515151', '552525252']


def numlist():
    select3 = asteriskdb.select(f"""
    SELECT oc.telnum
    FROM asterisk.outgoing_call oc
    WHERE DATE_FORMAT(oc.createdate, '%d-%m-%Y') = DATE_FORMAT(NOW(), '%d-%m-%Y')
    AND oc.telnum IN {tuple(lastnumlist)}
    """)

    mob3 = []
    for item in select3:
        mob3.append(item[0])
    namlist3 = list(set(mob3)) + ['551515151', '552525252']

    newnumlist = []
    for item in lastnumlist:
        if item not in namlist3:
            newnumlist.append(item)
    return newnumlist


newnumlist = numlist()

if len(newnumlist) == 0:
    print("New Records Not Found.")
else:
    nn = [tuple([int(n)] + ['ABANDON'] + ['ABANDON_CALL'] + ['user_waiting_operator'] + [str(datetime.now())[:-7]]) for n in newnumlist]

    insert_query = """INSERT INTO asterisk.outgoing_call (telnum, identifier, tasktypename, outgoincalltype, updatedate) VALUES (%s, %s, %s, %s, %s)"""
    records_to_insert = nn

    asteriskdb.insert(insert_query, records_to_insert)
print(datetime.now() - start)
