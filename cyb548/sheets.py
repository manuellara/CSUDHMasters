import gspread 
from oauth2client.service_account import ServiceAccountCredentials 
import sqlite3 


def writeDBHeaders():
    dbName = "data.db"
    tableName = "gunViolenceData"

    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    sql = f"select * from {tableName} where 1=0;"

    c.execute(sql)

    headers = [d[0] for d in c.description]

    # writes headers to first row 
    sheet.append_row( headers )

    conn.close()
    return


def populateSheets():
    dbName = "data.db"
    tableName = "gunViolenceData"

    conn = sqlite3.connect(dbName)
    c = conn.cursor()

    sql = f"select * from {tableName} ;"

    c.execute( sql )

    result = c.fetchall()

    for row in result:
        sheet.append_row( [ i for i in row ] )

    conn.close()
    return



scope = ['https://www.googleapis.com/auth/drive']

credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope) 

client = gspread.authorize(credentials)  

#connects to 'projectMgmt' sheet
sheet = client.open('projectMgmt').sheet1


# gets db headers 
writeDBHeaders()

populateSheets()