import pandas as pd

class Extract :
    def FromDb(self, dbConnection , query):
        queryResult, columns = dbConnection.read(query)
        return queryResult, columns
    
    def FromCsv(self, path)->pd.DataFrame:
        return pd.read_csv(path)

