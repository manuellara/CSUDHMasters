import pandas as pd
import sqlite3
import os
import gspread 
from oauth2client.service_account import ServiceAccountCredentials 

class csv2db:
    def __init__(self, dbName, tableName, csv):
        self.dbName = dbName
        self.csv = csv
        self.tableName = tableName

    def getDataFrame(self):
        try:
            self.df = pd.read_csv(self.csv)
            print("[ ✔ ] CSV converted to dataframe")
        except Exception as e:
            print("[ × ] Failed getting data frame: ", e) 

    def getHeaders(self):
        try:
            self.headers = self.df.columns
            print("[ ✔ ] Headers saved")
        except Exception as e:
            print("[ × ] Failed getting headers: ", e)

    def createDatabase(self):
        try:
            if not os.path.exists(self.dbName):
                self.conn = sqlite3.connect(self.dbName)
                self.c = self.conn.cursor()
                print("[ ✔ ] Database created & connected")
            else:
                self.conn = sqlite3.connect(self.dbName)
                self.c = self.conn.cursor()
                print("[ ✔ ] Database connected")
        except Exception as e:
            print("[ × ] Failed creating databse: ", e)

    def insertHeaders(self):
        self.templateSQL = "CREATE TABLE ! ( ? )"
        self.templateString = "field text"
        self.placeholder = ''
        try:
            self.placeholder = ', '.join(
                str(self.templateString.replace("field", i)) for i in self.headers)
            self.templateSQL = self.templateSQL.replace("?", self.placeholder)
            self.templateSQL = self.templateSQL.replace("!", self.tableName)
        except Exception as e:
            print(f"[ × ] Failed inserting headers into {self.tableName}: ", e)
        else:
            self.c.execute(self.templateSQL)
            self.conn.commit()
            print(f"[ ✔ ] Headers inserted into the {self.tableName} table")

    def getIndexOfLastRow(self):
        self.indexOfLastRow = self.df.index[-1]

    def populateDB(self):
        print("[ ✔ ] Inserting rows in progress...")
        try:
            for i in range(0, self.indexOfLastRow + 1):
                self.templateSQL = "INSERT INTO ! VALUES ( ? )"
                self.templateString = "field"
                self.placeholder = ''
                self.placeholder = ', '.join('"' + str(self.templateString.replace(
                    "field", str(x))).replace('"', '`') + '"' for x in self.df.iloc[i])
                self.templateSQL = self.templateSQL.replace(
                    "?", self.placeholder)
                self.templateSQL = self.templateSQL.replace(
                    "!", self.tableName)
                self.c.execute(self.templateSQL)
            print(
                f"[ ✔ ] {self.indexOfLastRow + 1} rows inserted into the {self.tableName} table")
        except Exception as e:
            print("[ × ] Failed populating database: ", e)
        else:
            self.conn.commit()
            self.conn.close()
            print("[ ✔ ] Database connection closed")

    def execute(self):
        self.getDataFrame()
        self.getHeaders()
        self.createDatabase()
        self.insertHeaders()
        self.getIndexOfLastRow()
        self.populateDB()

class db2sheets:
    def __init__(self, creds , dbName , tableName , sheetName):
        self.scope = ['https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(creds , self.scope) 
        self.client = gspread.authorize(self.credentials)
        self.dbName = dbName
        self.tableName = tableName 
        self.sheetName = sheetName
        self.x = 0

    def connectToDB(self):
        try:
            self.conn = sqlite3.connect(self.dbName)
            self.c = self.conn.cursor()    
            print(f"[ ✔ ] Connected to {self.dbName}")
        except Exception as e:
            print(f"[ × ] Failed connecting to {self.dbName}: ", e)

    def connectToSheet(self):
        try:
            self.sheet = self.client.open(self.sheetName).sheet1
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

    def populateSheets(self):
        try:
            self.sql = f"select * from {self.tableName} ;"
            self.c.execute( self.sql )
            self.result = self.c.fetchall()
            print(f"[ ✔ ] Populating {self.sheetName}...")
            for row in self.result:
                self.sheet.append_row( [ i for i in row ] )
                self.x += 1
                if self.x == 50:
                    break
            self.conn.close()
        except Exception as e:
            print(f"[ × ] Failed populating Sheet: ", e)
        else:
            print(f"[ ✔ ] {self.x} rows inserted into {self.sheetName}")

    def execute(self):
        self.connectToDB()
        self.connectToSheet()
        self.writeDBHeaders()
        self.populateSheets()

print("CONVERTING CSV TO DB")
dbName = "data.db" 
tableName = "gunViolenceData"   #change if running multiple times
csv = 'gun-violence-data_01-2013_03-2018.csv'
data = csv2db(dbName, tableName, csv)
data.execute()

print("\nCONVERTING DB TO SHEETS")
creds = "creds.json"
sheetName = "projectMgmt"
driver = db2sheets( creds , dbName , tableName , sheetName )
driver.execute()

print(f"\n[ ✔ ] Done.")
