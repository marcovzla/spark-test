package recommender

import java.io.File
import java.nio.charset.CodingErrorAction
import scala.math.sqrt
import scala.io.Codec
import scala.io.Source
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.log4j._

object Recommender extends App {

  implicit val codec = Codec("UTF-8")
  codec.onMalformedInput(CodingErrorAction.REPLACE)
  codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

  case class MovieRating(movie: Int, rating: Double)

  type User = Int
  type Movie = Int
  type Rating = Double
  type MoviePair = (Movie, Movie)
  type RatingPair = (Rating, Rating)
  type RatingPairs = Iterable[RatingPair]
  type Similarity = Double

  // we ignore movie pairs if we have less than this amount of reviews for them
  val minCount = 1000

  val movieOfInterest = 260//args(0).toInt

  val movieNames = readMovieNames("ml-1m/movies.dat") // used for displaying results only

  // only print error messages
  Logger.getLogger("org").setLevel(Level.ERROR)

  // use EMR configuration
  val conf = new SparkConf
  conf.setAppName("Recommender")
  val sc = new SparkContext(conf)

  // each line is of the form: UserID::MovieID::Rating::Timestamp
  val data: RDD[String] = sc.textFile("s3n://lum-ai-bucket/ml-1m/ratings.dat")

  // convert each line into an array of strings by splitting on the :: delimiter
  val fields: RDD[Array[String]] = data.map(_.split("::"))

  // convert the array of strings into a tuple (User, MovieRating)
  // so that we can use the users as keys and movie-ratings as values
  // of a pair-rdd
  val ratings: RDD[(User, MovieRating)] = fields.map(f => (f(0).toInt, MovieRating(f(1).toInt, f(2).toDouble)))

  // find pairs of movies reviewed by the same user,
  // but don't include pairs if they are the same movie!
  val joinedRatings: RDD[(User, (MovieRating, MovieRating))] = ratings.join(ratings).filter(isNotSameMovie)

  // reformat data as ((movie, movie), (rating, rating))
  // this will give us a pair-rdd where the key is a movie pair and the value a rating pair
  val moviePairs: RDD[(MoviePair, RatingPair)] = joinedRatings.map(mkPair)

  // cosine similarity: https://en.wikipedia.org/wiki/Cosine_similarity#Definition
  // ---
  // construct a pair-rdd with movie-pair as key and (cosine-similarity, count) as value.
  val moviePairsWithSimCounts: RDD[(MoviePair, (Similarity, Int))] = moviePairs
    .map { case (mp, (x, y)) => (mp, (x * x, y * y, x * y, 1)) } // value is (XX, YY, XY, count)  see wikipedia for formula
    .reduceByKey { case (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4) } // addition
    .map { case (pair, (xx, yy, xy, c)) => // return cos-sim for this movie pair   see wikipedia for formula
      val denom = sqrt(xx) * sqrt(yy)
      val score = if (denom != 0) xy / denom else 0.0
      (pair, (score, c))
    }
    .cache()

  // moviePairsWithSimCounts.saveAsTextFile("s3n://lum-ai-bucket/ml-1m/results.txt")

  // get movies most similar to the movie of interest
  // sorted by similarity in descending order
  val similarMovies: RDD[(Similarity, (Movie, Int))] = moviePairsWithSimCounts
    .filter { case ((m1, m2), (sim, count)) =>
      (m1 == movieOfInterest || m2 == movieOfInterest) && count >= minCount
    }
    .map { case ((m1, m2), (sim, count)) =>
      val otherMovie = if (m1 == movieOfInterest) m2 else m1
      (sim, (otherMovie, count))
    }
    .sortByKey(ascending = false)

  val results = similarMovies.take(30)

  println("movie: " + movieOfInterest)
  // let the user know what movies he/she should watch
  println(s"if you like '${movieNames(movieOfInterest)}' then you should watch:")
  for ((sim, (movie, count)) <- results) {
  // for (((m1, m2), (sim, count)) <- results) {
    val name = movieNames(movie)
    println(s"  $name\tsimilarity: $sim")
    // val n1 = movieNames(m1)
    // val n2 = movieNames(m2)
    // println(s"  $m1 - $m2    $sim     $count")
  }

  // FIN

  def hasMovieOfInterest(d: (MoviePair, (Similarity, Int))): Boolean = {
    val ((m1, m2), (sim, count)) = d
    m1 == movieOfInterest || m2 == movieOfInterest
  }

  def isNotSameMovie(userMoviePair: (User, (MovieRating, MovieRating))): Boolean = {
    val (user, (mr1, mr2)) = userMoviePair
    mr1.movie != mr2.movie
  }

  def mkPair(userMoviePair: (User, (MovieRating, MovieRating))): (MoviePair, RatingPair) = {
    val (user, (mr1, mr2)) = userMoviePair
    ((mr1.movie, mr2.movie), (mr1.rating, mr2.rating))
  }

  def readMovieNames(name: String): Map[Int, String] = readMovieNames(new File(name))

  def readMovieNames(file: File): Map[Int, String] = {
    val source = Source.fromFile(file)
    val namesById = source.getLines()
      .map(_.split("::"))
      .map(a => (a(0).toInt, a(1)))
      .toMap
    source.close()
    namesById
  }

}
