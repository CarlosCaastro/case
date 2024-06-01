import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, Row

class DataExtractorIfood:
    def __init__(self, spark: SparkSession, user: str, token: str):
        self.spark = spark
        self.user = user
        self.headers = {"Authorization": f"token {token}"}

    def get_followers(self):
        followers = []
        page = 1
        while True:
            end_point_followers = f'https://api.github.com/users/{self.user}/followers?page={page}'
            response_followers = requests.get(end_point_followers, headers=self.headers).json()
            if not response_followers:
                break
            followers.extend(response_followers)
            page += 1
        users = [{'login': follower['login']} for follower in followers]
        followers_df = self.spark.createDataFrame(users)
        return followers_df

    def get_github_user_info(self, login):
        url = f"https://api.github.com/users/{login}"
        response = requests.get(url, headers=self.headers)
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

    def enrich_with_github_info(self, df):
        users = df.collect()

        enriched_data = []
        for user in users:
            user_info = self.get_github_user_info(user['login'])
            enriched_data.append(Row(
                name=user_info[0],
                company=user_info[1],
                blog=user_info[2],
                email=user_info[3],
                bio=user_info[4],
                public_repos=user_info[5],
                followers=user_info[6],
                following=user_info[7],
                created_at=user_info[8]
            ))

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
        
        enriched_df = self.spark.createDataFrame(enriched_data, schema)
        return enriched_df

    def execute_extract_api(self):
        df = self.get_followers()
        df_extract = self.enrich_with_github_info(df=df)

        return df_extract

    def read_csv(self, path):
        return self.spark.read.csv(path, header=True, inferSchema=True)
    
    def get_count_followers(self):
        url = f"https://api.github.com/users/{self.user}"
        response = requests.get(url, headers=self.headers)
        data = response.json()
        followers_count = data.get('followers')

        return followers_count
