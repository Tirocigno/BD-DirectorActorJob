package it.unibo.bd1819

import it.unibo.bd1819.spark.ThreeActorsDirectorBuilder
import utils.DFFactory._
import org.apache.spark.{SparkContext, sql}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}


object ScalaMain extends App {

  var executors = 2
  var taskForExceutor = 4
  SparkSession.builder.config("spark.default.parallelism", (executors*taskForExceutor).toString)
  val sc =  new SparkContext()
  val sqlContext = SparkSession.builder.getOrCreate.sqlContext
  val titleBasicsDF = getTitleBasicsDF(sc, sqlContext)
  val titlePrinicipalsDF = getTitlePrincipalsDF(sc, sqlContext)
  val nameBasicsDF = getNameBasicsDF(sc, sqlContext)

  sqlContext.setConf("spark.sql.shuffle.partitions", (executors*taskForExceutor).toString)
  sqlContext.setConf("spark.default.parallelism", (executors*taskForExceutor).toString)



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

 //findinding for each director the three most frequent actors
  val threeActorsDirectorDF = ThreeActorsDirectorBuilder(sqlContext, executors * taskForExceutor)
    .buildThreeActorsDirectorsDataFrame(directorActorMovieCountDF)

  //Joining the previous result with the director count.
  val countMoviesActorsDirectorDF = threeActorsDirectorDF.join(sortedDirectorMoviesCountDF, Seq("DirectorID"))

  //Joining the previous table
  val namedDirectorCountMoviesActorsDirectorDF = countMoviesActorsDirectorDF.join(nameBasicsDF,
    nameBasicsDF(nameID) === countMoviesActorsDirectorDF("DirectorID"))

  namedDirectorCountMoviesActorsDirectorDF.createOrReplaceTempView("DIRECTOR_NAME_TEMP_TABLE")

  val filteredDirectorNameTable = sqlContext.sql("select primaryName as DirectorName, MoviesDirected,  ActorID," +
    " CollabMovies from DIRECTOR_NAME_TEMP_TABLE")

  val namedActorNamedDirectorCountMoviesActorsDirectorDF = filteredDirectorNameTable.join(nameBasicsDF,
    nameBasicsDF(nameID) === filteredDirectorNameTable("ActorID"))

  namedActorNamedDirectorCountMoviesActorsDirectorDF.createOrReplaceTempView("ACTOR_DIRECTOR_FINAL_TABLE")

  val resultDF = sqlContext.sql("select DirectorName, primaryName as ActorName from ACTOR_DIRECTOR_FINAL_TABLE order by MoviesDirected desc, " +
    "CollabMovies desc")

  resultDF.show()
  //resultDF.write.saveAsTable("fnaldini_director_actors_db.Actor_Director_Table_Second2")
}


