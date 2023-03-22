from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import IntegerType, FloatType

# Import the required functions
from pyspark.ml.recommendation import ALS, ALSModel

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RecommendationALS:
    def __init__(self):
        self.spark = SparkSession.builder.appName('Recommendations')\
            .master("local") \
            .getOrCreate()
        logger.info("loading data movies..")
        self.movies = self.spark.read.csv("data/movies.csv", header=True)
        logger.info("loading data ratings..")
        ratings = self.spark.read.csv("data/ratings.csv", header=True)

        # mengubah tipe data pada ratings
        self.ratings = ratings.withColumn("userId", ratings.userId.cast(IntegerType())) \
            .withColumn('movieId', ratings.movieId.cast(IntegerType())) \
            .withColumn('rating', ratings.rating.cast(FloatType()))\
            .drop('timestamp')

        # training model
        self.rank = 20
        self.maxIter = 8
        self.regParam = 0.14
        self.__train_model()

    def __train_model(self):
        logger.info("Training the ALS model...")

        als = ALS(
            rank=self.rank,
            maxIter=self.maxIter,
            regParam=self.regParam,
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            nonnegative=True,
            implicitPrefs=False,
            coldStartStrategy="drop"
        )

        self.model = als.fit(self.ratings)
        logger.info("Model build!")

    def add_ratings(self, ratings):
        # format userId, movieId, rating
        # new_ratings into dataframe base on ratings column
        new_user_ratings = self.spark.createDataFrame(
            ratings, self.ratings.columns)
        # new_user_ratings.show()
        self.new_ratings = new_user_ratings
        self.ratings = self.ratings.union(new_user_ratings).dropDuplicates()
        self.__train_model()

    def get_sparsity(self):
        # count total number ratings
        count_nonzero = self.ratings.select("rating").count()
        # count distinct userIds and distinct movieIds
        denominator = self.ratings.select("userId").distinct().count() \
            * self.ratings.select('movieId').distinct().count()
        # bagi numerator by denominator
        sparsity = (1.0 - (count_nonzero*1.0)/denominator)*100

        print(f"{sparsity} sparse")

    def get_joins_movies(self, recommendations):
        df = self.spark.createDataFrame(recommendations, ["movieId", "rating"])
        recommendations = df.join(self.movies, on='movieId')

        # return recommendations, movie rated
        return recommendations.toPandas().to_dict('records')

    def get_joins_new_ratings(self, user_id, new_ratings):
        # format userId, movieId, rating
        # new_ratings into dataframe base on ratings column
        new_user_ratings = self.spark.createDataFrame(
            new_ratings, self.ratings.columns)
        # new_user_ratings.show()
        self.new_ratings = new_user_ratings

        user_ratings = self.new_ratings.join(self.movies, on='movieId').filter(
            f'userId = {user_id}').sort('rating', ascending=False)

        return user_ratings.toPandas().to_dict('records')

    def get_recs_users(self, user_id):
        user_id = int(user_id)
        users = self.spark.createDataFrame(
            [user_id], IntegerType()).toDF('userId')

        userSubsetRecs = self.model.recommendForUserSubset(users, 8)

        recommendations = userSubsetRecs\
            .withColumn("rec_exp", explode("recommendations"))\
            .select('userId', col("rec_exp.movieId"), col("rec_exp.rating"))

        user_ratings = self.new_ratings.join(self.movies, on='movieId').filter(
            f'userId = {user_id}').sort('rating', ascending=False)

        recommendations = recommendations.join(self.movies, on='movieId').filter(
            f'userId = {user_id}')

        # return recommendation, movies
        return recommendations.toPandas().to_dict('records'), user_ratings.toPandas().to_dict('records')

    def get_recs_users_all(self, users):
        users = self.spark.createDataFrame(users, IntegerType()).toDF('userId')
        usersSubsetRecs = self.model.recommendForUserSubset(users, 8)

        return usersSubsetRecs.toPandas().to_dict('records')

    def samples(self, user_id, num_ratings):
        samples = self.ratings.sample(False, .001, seed=20).collect()
        # get list movieId
        sample_list = [i[1] for i in samples]
        new_ratings = []
        # get nre user rating
        for i in range(len(sample_list)):

            # print movie title by movie id in sample list
            print(self.movies.where(self.movies.movieId ==
                  sample_list[i]).take(1)[0]['title'])
            rating = input(
                'rate this movie 1-5, press n if you have not seen:\n')

            if rating == 'n':
                continue
            else:
                new_ratings.append(
                    (user_id, sample_list[i], float(rating)))
                num_ratings -= 1
                if num_ratings == 0:
                    break
        print(new_ratings)
        self.add_ratings(ratings=new_ratings)
        self.get_recs_users(user_id=user_id)


if __name__ == "__main__":

    recomendation = RecommendationALS()
    # recomendation.samples(user_id=2138, num_ratings=5)
    recomendation.get_recs_users_all([148, 4])
