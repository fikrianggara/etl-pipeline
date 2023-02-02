import pandas as pd

class Transform :
    def queryResultToDF(self, queryResult, columns)->pd.DataFrame:
        return pd.DataFrame.from_records(queryResult, columns=columns)