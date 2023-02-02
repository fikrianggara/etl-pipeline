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
    fd = open('query/test.sql', 'r')
    sqlFile = fd.read()
    fd.close()

    queryResult, columns = extractor.FromDb(sqlServerConnection, sqlFile)

    # Transform to dataframe
    transformer = t.Transform()
    transformedData = transformer.queryResultToDF(queryResult, columns)
    transformedData.head()

    # Load to csv
    loader = l.Load()
    loader.ToFSCSV(transformedData, 'hasil/test.csv')

