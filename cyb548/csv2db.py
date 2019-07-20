import pandas as pd
import sqlite3
import os

class csv2db:
    def __init__(self, dbName, csv):
        self.dbName = dbName
        self.csv = csv

    def getDataFrame(self):
        try:
            self.df = pd.read_csv(self.csv)
        except Exception as e:
            print("[ × ] Failed getting data frame: ", e)
        finally:
            print("[ ✔ ] CSV converted to dataframe")

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

    def getTableName(self):
        self.tableName = "gunViolenceData"

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
            print(f"[ ✔ ] Headers inserted into the {self.tableName} table")

    def getIndexOfLastRow(self):
        self.indexOfLastRow = self.df.tail(1).index.item()

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
            self.conn.close()
            print("[ ✔ ] Database connection closed")

    def execute(self):
        self.getDataFrame()
        self.getHeaders()
        self.createDatabase()
        self.getTableName()
        self.insertHeaders()
        self.getIndexOfLastRow()
        self.populateDB()

databaseName = 'data.db'
csv = 'gun-violence-data_01-2013_03-2018.csv'

data = csv2db(databaseName, csv)
data.execute()

os.system('rm data.db')