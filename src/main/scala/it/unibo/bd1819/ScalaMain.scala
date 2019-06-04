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

  sqlContext.sql("select tconst from " + TITLE_BASICS_TABLE_NAME).show()
  sqlContext.sql("select * from " + TITLE_PRINCIPALS_TABLE_NAME)
  sqlContext.sql("select * from " + NAME_BASICS_TABLE_NAME)

 /*val basicNameAndPrincipalJoinDF = titleBasicsDF.join(titlePrinicipalsDF,
    titleBasicsDF(titleID) === titlePrinicipalsDF(titleID))*/

  val directorsDF = sqlContext.sql("select nconst as DirectorID, count() as MoviesDirected" +
    "  from " +
  TITLE_PRINCIPALS_TABLE_NAME + " where category = \'director\' and tconst in (select tconst from "+
    TITLE_BASICS_TABLE_NAME +") group by nconst")
  //directorsDF.show()

  /*val namedDirectorsActorsJoinDF = basicNameAndPrincipalJoinDF.join(nameBasicsDF,
    Seq(nameID))

  namedDirectorsActorsJoinDF.createOrReplaceTempView("DirectorActorJoinTable")


  namedDirectorsActorsJoinDF.show(1000)*/

}
