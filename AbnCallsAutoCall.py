from astesriskDB import asteriskdb
from datetime import datetime

select1 = asteriskdb.select("""
SELECT c.recid                                      AS id,
       DATE_FORMAT(c.calldate, '%d-%m-%Y %H:%i:%S') as CallDAte,
       RIGHT(c.src, 9)                              AS Mobile,
       c.uniqueid                                   AS Callid
FROM asterisk.cdr c
WHERE c.lastapp = 'BackGround'
  AND c.calldate BETWEEN (NOW() - INTERVAL 10 MINUTE) AND (NOW() - INTERVAL 2 MINUTE)
  AND LENGTH(c.src) > 8
  AND c.src NOT IN ('asterisk', 'Anonymous')
ORDER BY c.calldate DESC
""")
mob = []
for item in select1:
    mob.append(item[2])
namlist1 = list(set(mob)) + ['551515151', '552525252']


def numlist():
    select2 = asteriskdb.select(f"""
    SELECT oc.telnum
    FROM asterisk.outgoing_call oc
    WHERE DATE_FORMAT(oc.createdate, '%d-%m-%Y') = DATE_FORMAT(NOW(), '%d-%m-%Y')
    AND oc.telnum IN {tuple(namlist1)}
    """)

    mob2 = []
    for item in select2:
        mob2.append(item[0])
    namlist2 = list(set(mob2)) + ['551515151', '552525252']

    newnumlist = []
    for item in namlist1:
        if item not in namlist2:
            newnumlist.append(item)
    return newnumlist


# print(numlist())
# print(namlist1)

if len(namlist1) > 2:
    newnumlist = numlist()
    nn = [tuple([int(n)] + ['ABANDON'] + ['ABANDON_CALL'] + ['user_waiting_operator'] + [str(datetime.now())[:-7]]) for n in newnumlist]

    insert_query = """INSERT INTO asterisk.outgoing_call (telnum, identifier, tasktypename, outgoincalltype, updatedate) VALUES (%s, %s, %s, %s, %s)"""
    records_to_insert = nn

    asteriskdb.insert(insert_query, records_to_insert)
else:
    print("New Records Not Found.")
