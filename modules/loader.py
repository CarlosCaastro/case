from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, to_date, date_format

class DataLoaderIfood:
    def __init__(self, path: str):
        self.path = path

    def save_to_csv(self, df: DataFrame):
        df.coalesce(1).write.csv(self.path, header=True, mode="append")
