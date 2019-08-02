import gspread 
from oauth2client.service_account import ServiceAccountCredentials 
import sqlite3 



class db2sheets:
    def __init__(self, creds , dbName , tableName , sheetName):
        self.scope = ['https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(creds , self.scope) 
        self.client = gspread.authorize(self.credentials)
        self.dbName = dbName
        self.tableName = tableName 
        self.sheetName = sheetName

    def connectToDB(self):
        try:
            self.conn = sqlite3.connect(self.dbName)
            self.c = self.conn.cursor()    
            print(f"[ ✔ ] Connected to {self.sheetName}")
        except Exception as e:
            print(f"[ × ] Failed connecting to {self.dbName}: ", e)

    def connectToSheet(self):
        try:
            self.sheet = self.client.open('projectMgmt').sheet1
            print(f"[ ✔ ] Connected to {self.sheetName}")
        except Exception as e:
            print(f"[ × ] Failed connecting to {self.sheetName}: ", e)

    def writeDBHeaders(self):
        try:
            self.sql = f"select * from {self.tableName} where 1=0;"
            self.c.execute(self.sql)
            self.headers = [d[0] for d in self.c.description]
            self.sheet.append_row( self.headers )
            print(f"[ ✔ ] Headers successfully written to {self.sheetName}")
        except Exception as e:
            print(f"[ × ] Failed getting/writing headers: ", e)







# def writeDBHeaders():
#     dbName = "data.db"
#     tableName = "gunViolenceData"

#     conn = sqlite3.connect(dbName)
#     c = conn.cursor()

#     sql = f"select * from {tableName} where 1=0;"

#     c.execute(sql)

#     headers = [d[0] for d in c.description]

#     # writes headers to first row 
#     sheet.append_row( headers )

#     conn.close()
#     return


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



# scope = ['https://www.googleapis.com/auth/drive']

# credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope) 

# client = gspread.authorize(credentials)  

#connects to 'projectMgmt' sheet
# sheet = client.open('projectMgmt').sheet1


# gets db headers 
writeDBHeaders()

populateSheets()



creds = "creds.json"
dbName = "data.db" 
tableName = "gunViolenceData"
sheetName = "projectMgmt"


driver = db2sheets( creds , dbName , tableName , sheetName )