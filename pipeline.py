import os
import connector.database as dbConnector
import extract as e
import transform as t
import load as l
from dotenv import load_dotenv

def fromSQLServerToCSV():

    load_dotenv()
    
    sqlServerConnection = dbConnector.SQLServerConnection(driver=os.getenv('DRIVER'),
                                                        server=os.getenv('DB_SERVER'),
                                                        database = os.getenv('DATABASE'),
                                                        user_id = os.getenv('UID'),
                                                        password = os.getenv('PASSWORD'))
    sqlServerConnection.connect()

    # Extract from sql server db
    extractor = e.Extract()
    fd = open('query/caregiver kode 3.sql', 'r')
    sqlFile = fd.read()
    fd.close()

    queryResult, columns = extractor.FromDb(sqlServerConnection, sqlFile)

    # Transform to dataframe
    transformer = t.Transform()
    transformedData = transformer.queryResultToDF(queryResult, columns)
    transformedData.head()

    # Load to csv
    loader = l.Load()
    loader.ToFSCSV(transformedData, 'hasil/caregiver kode 3.csv')

def extractFromGsheet():

    load_dotenv()

    extractor = e.Extract()
    spreadSheetId='1YgCZwN6e4m4xeRkNvxisT-28fgWAJQqBpuDt9DqUhOE'
    sheetName='matrix anomali 06-02-2023'
    api_key=os.getenv('GSHEET_API_KEY')
    for x in range (80):
        print(x)
        extractor.FromGsheet(spreadSheetId, sheetName, api_key)