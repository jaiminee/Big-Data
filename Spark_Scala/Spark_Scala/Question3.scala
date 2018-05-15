import org.apache.spark.SparkContext._
spark

sqlContext

sc

val moviesData = sqlContext.read.format("csv").option("header","true").load("FileStore/tables/movies.csv")
val ratingsData = sqlContext.read.format("csv").option("header","true").load("FileStore/tables/ratings.csv")
val tagsData = sqlContext.read.format("csv").option("header","true").load("FileStore/tables/tags.csv")

import org.apache.spark.sql.functions._
val moviesToRatingsJoined = moviesData.join(ratingsData, Seq("movieId"))
moviesToRatingsJoined.show

//Question 3.1.a
val avgDataOfMoviesRatings= moviesToRatingsJoined.groupBy("movieId").agg(avg($"rating"))
avgDataOfMoviesRatings.show
avgDataOfMoviesRatings.write.format("csv").option("header","true").save("FileStore/tables/q3Part1aOutPut.csv")


//Question 3.1.b
val lowestAvgMoviesRatings = moviesToRatingsJoined.select("movieId", "title","rating").groupBy("movieId","title").agg(avg($"rating")).orderBy(asc("avg(rating)")).limit(10)

lowestAvgMoviesRatings.show
lowestAvgMoviesRatings.write.format("csv").option("header","true").save("FileStore/tables/q3Part1bOutPut.csv")

val dataJoined = moviesToRatingsJoined.join(tagsData, Seq("movieId"))
dataJoined.show

//Question 3.2
val avgMoviesRatingsTagsAction = dataJoined.select("movieId", "title","rating","tag").groupBy("movieId","title","tag").agg(avg($"rating")).where("tag=='action'")

avgMoviesRatingsTagsAction.show
avgMoviesRatingsTagsAction.write.format("csv").option("header","true").save("FileStore/tables/q3Part2OutPut.csv")

//Question 3.3
val avgRatingsActionTagThrillerGenre = dataJoined.select("movieId", "title","rating","tag","genres").groupBy("movieId","title","tag","genres").agg(avg($"rating")).where("tag == 'action' and genres like('%Thriller%')")

avgRatingsActionTagThrillerGenre.show
avgRatingsActionTagThrillerGenre.write.format("csv").option("header","true").save("FileStore/tables/q3Part3OutPut.csv")


