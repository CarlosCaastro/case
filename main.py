from pyspark.sql import SparkSession
from pyspark.sql.functions import  col, regexp_extract
from modules.extract import DataExtractorIfood
from modules.loader import DataLoaderIfood
from modules.transform import DataTransformerIfood
from modules.test import DataTestIfood
from credentials.credentials import user, token, path


spark = SparkSession.builder.appName("PySparkApp").getOrCreate()
spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
spark.conf.set("parquet.enable.summary-metadata", "false")
spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

extractor = DataExtractorIfood(spark=spark, user=user, token=token)
transformer = DataTransformerIfood()
loader = DataLoaderIfood(path)
test = DataTestIfood(spark=spark,path=path,user=user,token=token)

df_with_github_info = extractor.execute_extract_api()

df_transformed = transformer.transform(df_with_github_info)

loader.save_to_csv(df_transformed)

test.execute_test()

spark.stop()
