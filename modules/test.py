from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, regexp_extract
from modules.extract import DataExtractorIfood


class DataTestIfood:

    def __init__(self, spark: SparkSession,path,user, token):
        self.spark = spark
        self.path = path
        self.user = user
        self.token = token
        self.headers = {"Authorization": f"token {token}"}

    def clean_company_check(self,df):
        
        if df.filter(col("company").like("%@%")).count() > 0:
            print("Existe pelo menos um '@' na coluna 'company'.")
        else:
            print("Não existe '@' na coluna 'company'.")
        
    def date_format_check(self,df):

        pattern = r'^\d{2}/\d{2}/\d{4}$'

        if df.filter(regexp_extract(col("created_at"), pattern, 0) != "").count() == df.count():
            print("A coluna 'created_at' está com as datas no formato dd/mm/yyyy.")
        else:
            print("A coluna 'created_at' não está com todas as datas no formato dd/mm/yyyy.")
    
    def volumetry_check(self,df):

        followers_count = DataExtractorIfood(spark=self.spark,user=self.user,token=self.token).get_count_followers()
        
        if df.count() == followers_count:
            return print("A quantidade de seguidores em conformidade a quantidade atual de seguidores no GitHub.")
        else:
            return print("A quantidade de seguidores não está igual")


    def execute_test(self):
        print("Resultado das verificações de teste no Dataframe.")
        df = DataExtractorIfood(spark=self.spark,user=self.user,token=self.token).extract_csv(path=self.path)
        self.clean_company_check(df=df)
        self.date_format_check(df=df)
        self.volumetry_check(df=df)
        