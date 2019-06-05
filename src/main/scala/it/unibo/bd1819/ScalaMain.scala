package it.unibo.bd1819

import it.unibo.bd1819.spark.ThreeActorsDirectorBuilder
import utils.DFFactory._
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.functions.col


object ScalaMain extends App {

  val sc =  new SparkContext()
  val sqlContext = SparkSession.builder().getOrCreate().sqlContext

  val titleBasicsDF = getTitleBasicsDF(sc, sqlContext)
  val titlePrinicipalsDF = getTitlePrincipalsDF(sc, sqlContext)
  val nameBasicsDF = getNameBasicsDF(sc, sqlContext)


  //Finding all directors inside the title.principals table

  val directorMoviesDF = sqlContext.sql("select "+ nameID +" as DirectorID, " + titleID + " as MovieTitle " +
    "from " + TITLE_PRINCIPALS_TABLE_NAME +
    " where category = 'director' and "+titleID+" in ( select " + titleID + " from " + TITLE_BASICS_TABLE_NAME + ")")

  directorMoviesDF.createOrReplaceTempView("DirectorMovieTable")

  //Sort all the director by the number of movies
  val sortedDirectorMoviesCountDF = sqlContext.sql("select DirectorID, count(MovieTitle) as MoviesDirected " +
    "from DirectorMovieTable group by DirectorID")


  //Finding all directors inside the title.principals

  val actorMoviesDF = sqlContext.sql("select "+ nameID +" as ActorID, " + titleID + " as MovieTitle " +
    "from " + TITLE_PRINCIPALS_TABLE_NAME +
    " where category = 'actor' or category = 'actress' ")

  //Join actors and directors.
  val joinedActorDirectorDF = directorMoviesDF.join(actorMoviesDF, Seq("MovieTitle"))
  joinedActorDirectorDF.createOrReplaceTempView("DirectorActorMoviesTable")

  //Create a table with director, actor, collabNumber table
  val directorActorMovieCountDF = sqlContext.sql("select DirectorID, ActorID, count(distinct MovieTitle) as CollabMovies " +
    " from DirectorActorMoviesTable group by DirectorID, ActorID ")

  //directorActorMovieCountDF.createOrReplaceTempView("DirectorActorCollabTable")

  val threeActorsDirectorDF = ThreeActorsDirectorBuilder(sqlContext).buildThreeActorsDirectorsDataFrame(directorActorMovieCountDF)

  threeActorsDirectorDF.show(1000)

 /* val threeDirectorActorDF = sqlContext.sql("select DirectorID, ActorID from DirectorActorMoviesTable main where " +
    "ActorID in (select ActorID from DirectorActorCollabTable collab where main.DirectorID = " +
    "collab.DirectorID order by MoviesDirected desc " +
    "limit 3)").show()*/


}


