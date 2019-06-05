package it.unibo.bd1819

import utils.DFFactory._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col


object ScalaMain extends App {

  val sc =  new SparkContext()
  val sqlContext = SparkSession.builder().getOrCreate().sqlContext

  val titleBasicsDF = getTitleBasicsDF(sc, sqlContext)
  val titlePrinicipalsDF = getTitlePrincipalsDF(sc, sqlContext)
  val nameBasicsDF = getNameBasicsDF(sc, sqlContext)


  //Finding all directors inside the actors

  val directorMoviesDF = sqlContext.sql("select "+ nameID +" as DirectorID, " + titleID + " as MovieTitle " +
    "from " + TITLE_PRINCIPALS_TABLE_NAME +
    " where category = 'director' and "+titleID+" in ( select " + titleID + " from " + TITLE_BASICS_TABLE_NAME + ")")

  directorActorMovieCountDF.createOrReplaceTempView("DirectorMovieTable")

  val sortedDirectorMoviesCountDF = sqlContext.sql("select DirectorID, count(MovieTitle) as MoviesDirected " +
    "from DirectorMovieTable group by DirectorID")


  //Finding all directors inside the actors

  val actorMoviesDF = sqlContext.sql("select "+ nameID +" as ActorID, " + titleID + " as MovieTitle " +
    "from " + TITLE_PRINCIPALS_TABLE_NAME +
    " where category = 'actor' or category = 'actress' ")

  val joinedActorDirectorDF = directorMoviesDF.join(actorMoviesDF, Seq("MovieTitle"))
  joinedActorDirectorDF.createOrReplaceTempView("DirectorActorMoviesTable")

  val directorActorMovieCountDF = sqlContext.sql("select DirectorID, ActorID, count(distinct MovieTitle) as MoviesDirected " +
    " from DirectorActorMoviesTable group by DirectorID, ActorID ")




}
