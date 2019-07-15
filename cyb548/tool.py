import pandas as pd 
import sqlite3


#returns CSV file we will use
def retireveCSV():
        #loads gun violence csv file
        df = pd.read_csv('gun-violence-data_01-2013_03-2018.csv')
        
        return df

#gets column headers => returns headers dict 
def getColumnHeaders( df ):
        #gets column headers 
        columnHeaders = df.columns 

        print("[ ✔ ] CSV coulmn headers saved")
        
        return columnHeaders

#builds SQL statemanet off column headers
def constructSQLstatement( columnHeaders ):
        templateSQL = "CREATE TABLE gunViolenceData ( ? )"
        templateString = "field text"
        placeholder = ''

        placeholder = ', '.join( str( templateString.replace( "field" , i ) ) for i in columnHeaders )
        templateSQL = templateSQL.replace( "?", placeholder )

        return templateSQL

#creates database, table, and fields 
def initializeDB( columnHeaders ):
        #DB name
        dbName = 'data.db'

        #initializes database OR connects to it if it already exits 
        conn = sqlite3.connect( dbName )

        print(f"[ ✔ ] Connected to {dbName}")

        #creates cursor to navigate database 
        c = conn.cursor()

        #try to create table => close DB when done
        try:
                sqlStatement = constructSQLstatement( columnHeaders )

                #create fields in table & fields 
                c.execute( sqlStatement ) 

                #commits data to database 
                conn.commit()

                print("[ ✔ ] Table has been created")
        except Exception as e:
                print(e)
        finally:
                #closes database connection
                conn.close()

                print("[ ✔ ] Database connection closed")
                
        return
             
#gets index of last row => returns index of last row
def getIndexOfLastRow( df ):
        indexOfLastRow = df.tail(1).index.item()
        
        return indexOfLastRow



##################################################################################################################

#gets CSV file we will use
df = retireveCSV()

#gets dict of column headers from CSV file
columnHeaders = getColumnHeaders( df )

#connects to DB, creates table & fields
initializeDB( columnHeaders ) 