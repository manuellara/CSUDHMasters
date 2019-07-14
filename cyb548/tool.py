import pandas as pd 
import sqlite3


#connects to DB, creates table, populates fields
def initializeDB():
        #initializes database OR connects to it if it already exits 
        conn = sqlite3.connect('data.db')

        #creates cursor to navigate database 
        c = conn.cursor()

        #try to create table => close DB when done
        try:
                #create fields in table & fields 
                c.execute("""
                        CREATE TABLE gunViolenceData (
                                incident_id text ,
                                date text , 
                                state text ,
                                city_or_county text ,
                                address text ,
                                n_killed integer ,
                                n_injured integer ,
                                incident_url text ,
                                source_url text ,
                                incident_url_fields_missing text ,
                                congressional_district integer ,
                                gun_stolen text ,
                                gun_type text ,
                                incident_characteristics text ,
                                latitude real ,
                                location_description text ,
                                longitude real ,
                                n_guns_involved text ,
                                notes text ,
                                participant_age text ,
                                participant_age_group text ,
                                participant_gender text ,
                                participant_name text ,
                                participant_relationship text ,
                                participant_status text ,
                                participant_type text ,
                                sources text ,
                                state_house_district text ,
                                state_senate_district text
                        )
                """) 
        except:
                print("Table already exists.")
        finally:
                #commits data to database 
                conn.commit()
                #closes database connection
                conn.close()
                
        return

#gets index of last row => returns index of last row
def getIndexOfLastRow( df ):
        indexOfLastRow = df.tail(1).index.item()
        
        return indexOfLastRow

#gets column headers => returns headers dict 
def getColumnHeaders( df ):
        #declares header dictionary 
        header = {}

        #gets column headers 
        column = df.columns 

        #iterates through column's array
        # key = header :: value = ''
        for i in column:
                header[i] = ''

        return header

#takes "headers" dictionary and adds values to the keys( column headers )
def insertValuesIntoKeys( headers, df ):
        #iterate through last row
        #for i in range( 0 , getIndexOfLastRow( df ) + 1 ):
        for i in range( 0 , 1 ):
                #get row
                row = df.iloc[i]
                
                #writes values to keys
                for index, (key , value) in enumerate( headers.items() ):
                        headers[key] = row[index]

        return headers

#TODO: INSERT statement :: populate database  
def insertValuesIntoDB( data ):
        #iterate through dict => { (tuples), (tuples) }
        for i in data.items():
                print( i )
                


##################################################################################################################

#connects to DB, creates table, and fields
initializeDB() 


#loads gun violence csv file
df = pd.read_csv('gun-violence-data_01-2013_03-2018.csv')

#dict of column headers
headers = getColumnHeaders( df )

#returns populated dict 
data = insertValuesIntoKeys( headers, df )


insertValuesIntoDB( data )

#print( data ) 