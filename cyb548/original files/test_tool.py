#pytest -v --disable-warnings
from tool import csv2db
import os

def test_setup():
    #global data 
    #data = csv2db('data.db', 'gunViolenceData', 'gun-violence-data_01-2013_03-2018.csv')
    assert isinstance( data, csv2db )

def test_dbName():
    assert data.dbName == 'data.db'

def test_tableName():
    assert data.tableName == 'gunViolenceData'

def test_csvFile():
    assert data.csv == 'gun-violence-data_01-2013_03-2018.csv'

def test_dbCreated():
    assert os.path.exists(data.dbName)
    os.system('rm data.db')