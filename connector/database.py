import pyodbc
import pandas as pd

# method" yang harus diimplementasikan bagi 
# siapapun yang menggunakan interface Connection

class Connection():
    def connect():
        pass
    def disconnect():
        pass
    def read():
        pass

class SQLServerConnection(Connection):

    def __init__(self, driver, server, database, user_id, password):
        Connection.__init__(self)
        self.driver = driver
        self.server = server
        self.database = database
        self.user_id = user_id
        self.password = password
    
    def connect(self):
        driver = "{"+self.driver+"}"
        cnxn_str = (f"Driver={driver};"
            f"Server={self.server};"
            f"Database={self.database};"
            f"UID={self.user_id};"
            f"PWD={self.password};")
        
        try :
            # connect to db with config string
            cnxn = pyodbc.connect(cnxn_str)
            self.connection = cnxn
        except Exception as e:
            print(e)
    
    def disconnect(self):
        self.connection.close()

    def read(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        queryResult = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        return queryResult, columns



