from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace, to_date, date_format

class DataTransformerIfood:
    @staticmethod
    def transform(df: DataFrame) -> DataFrame:
        df_transformed = df.withColumn("company", regexp_replace("company", "@", "")) \
                           .withColumn("created_at", date_format(to_date("created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'"), "dd/MM/yyyy"))
        return df_transformed
