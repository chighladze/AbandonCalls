from astesriskDB import asteriskdb
import os

onoff = asteriskdb.select("""
SELECT co.status AS Status
FROM asterisk.callout_onoff co
WHERE co.name = 'outgoing_call'
""")

if onoff[0][0] == 'off':
    os.system("python AbnCallsAutoCall.py")
