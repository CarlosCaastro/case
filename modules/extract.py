import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql.functions import udf

def get_github_user_info(login, token):
    url = f"https://api.github.com/users/{login}"
    headers = {"Authorization": f"token {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        user_info = response.json()
        return (
            user_info.get('name'),
            user_info.get('company'),
            user_info.get('blog'),
            user_info.get('email'),
            user_info.get('bio'),
            user_info.get('public_repos'),
            user_info.get('followers'),
            user_info.get('following'),
            user_info.get('created_at')
        )
    else:
        return (None, None, None, None, None, None, None, None, None)

class DataExtractorIfood:
    def __init__(self, spark: SparkSession, user: str, token: str):
        self.spark = spark
        self.user = user
        self.token = token

    def get_followers(self):
        followers = []
        page = 1
        while True:
            end_point_followers = f'https://api.github.com/users/{self.user}/followers?page={page}'
            response_followers = requests.get(end_point_followers, headers={"Authorization": f"token {self.token}"}).json()
            if not response_followers:
                break
            followers.extend(response_followers)
            page += 1
        users = [{'login': follower['login']} for follower in followers]
        followers_df = self.spark.createDataFrame(users)
        return followers_df

    def enrich_with_github_info(self, df):
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("company", StringType(), True),
            StructField("blog", StringType(), True),
            StructField("email", StringType(), True),
            StructField("bio", StringType(), True),
            StructField("public_repos", IntegerType(), True),
            StructField("followers", IntegerType(), True),
            StructField("following", IntegerType(), True),
            StructField("created_at", StringType(), True)
        ])

        token = self.token

        @udf(returnType=schema)
        def get_github_user_info_udf(login):
            return get_github_user_info(login, token)

        enriched_df = df.withColumn("github_info", get_github_user_info_udf(df["login"]))

        enriched_df = enriched_df.select(
            "github_info.name",
            "github_info.company",
            "github_info.blog",
            "github_info.email",
            "github_info.bio",
            "github_info.public_repos",
            "github_info.followers",
            "github_info.following",
            "github_info.created_at"
        )

        return enriched_df

    def execute_extract_api(self):
        df = self.get_followers()
        df_extract = self.enrich_with_github_info(df=df)
        return df_extract

    def read_csv(self, path):
        return self.spark.read.csv(path, header=True, inferSchema=True)

    def get_count_followers(self):
        url = f"https://api.github.com/users/{self.user}"
        response = requests.get(url, headers={"Authorization": f"token {self.token}"})
        data = response.json()
        followers_count = data.get('followers')
        return followers_count
