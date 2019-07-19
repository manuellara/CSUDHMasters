import pandas as pd 
import sqlite3

#gets index of last row => returns index of last row
def getIndexOfLastRow( df ):
        indexOfLastRow = df.tail(1).index.item()
        
        return indexOfLastRow

#returns dataframe object from CSV
def retrieveCSV():
        #loads gun violence csv file
        df = pd.read_csv('gun-violence-data_01-2013_03-2018.csv')
        
        return df

#gets column headers => returns headers dict 
def getColumnHeaders( df ):
        #gets column headers 
        columnHeaders = df.columns 

        print("\n[ ✔ ] CSV coulmn headers saved")
        
        return columnHeaders

#builds SQL statemanet off column headers
def constructSQLstatement( columnHeaders ):
        tableName = "gunViolenceData"
        
        templateSQL = "CREATE TABLE ! ( ? )"
        templateString = "field text"
        placeholder = ''

        placeholder = ', '.join( str( templateString.replace( "field" , i ) ) for i in columnHeaders )

        templateSQL = templateSQL.replace( "?", placeholder )
        templateSQL = templateSQL.replace( "!", tableName )

        return templateSQL, tableName

#creates database, table, and fields 
def initializeDB( columnHeaders ):
        #DB name
        dbName = 'data.db'

        #initializes database OR connects to it if it already exits 
        conn = sqlite3.connect( dbName )

        print(f"\n[ ✔ ] Connected to {dbName}")

        #creates cursor to navigate database 
        c = conn.cursor()

        #try to create table => close DB when done
        try:
                sqlStatement, tableName = constructSQLstatement( columnHeaders )

                #create fields in table & fields 
                c.execute( sqlStatement ) 

                #commits data to database 
                conn.commit()

                print(f"[ ✔ ] Table '{tableName}' has been created")
        except Exception as e:
                print("[ × ]", e)
        finally:
                #closes database connection
                conn.close()

                print("[ ✔ ] Database connection closed")
                
        return dbName, tableName

#builds SQL insert stament => returns insert statement 
def constructSQLInsertStatement( df , index , tableName ):
        tableName = tableName

        templateSQL = "INSERT INTO ! VALUES ( ? )"
        templateString = "field"
        placeholder = ''

        placeholder = ', '.join( '"' + str( templateString.replace( "field" , str( x ) )  ).replace( '"', '`' ) + '"' for x in df.iloc[index] )

        templateSQL = templateSQL.replace( "?", placeholder )
        templateSQL = templateSQL.replace( "!", tableName )

        return templateSQL

#takes db name and 
def populateDB( dbName , df , tableName ):
        try:
                #initialize counter
                x = 0

                #initializes database OR connects to it if it already exits 
                conn = sqlite3.connect( dbName )

                #creates cursor to navigate database 
                c = conn.cursor()

                print(f"\n[ ✔ ] Connected to {dbName}")
                print( "[ ✔ ] Inserting rows in progress..." )

                for i in range( 0 , getIndexOfLastRow(df) + 1 ):
                        #calls insert statement function 
                        insertStatement = constructSQLInsertStatement( df , i , tableName )
                        
                        #create fields in table & fields 
                        c.execute( insertStatement ) 

                        #increment counter
                        x += 1
                
                #commits data to database 
                conn.commit()

                if x > 1:
                        row = "rows"
                else:
                        row = "row"

                print( f"[ ✔ ] Successfully inserted {x} {row} into the {tableName} table" )
        except Exception as e:
                print("[ × ]", e)
        finally:
                #closes database connection
                conn.close()

                print("[ ✔ ] Database connection closed")
        
        return



##################################################################################################################

print("[ ! ] START: importing CSV into DB")

#gets CSV file we will use => returns dataframe 
df = retrieveCSV()

#=> returns pandas column headers object
columnHeaders = getColumnHeaders( df )

#connects to DB, creates table & fields => returns DB name
dbName, tableName = initializeDB( columnHeaders ) 

#add CSV data to DB
populateDB( dbName , df , tableName )

print("\n[ ! ] END: CSV successfully imported into DB")