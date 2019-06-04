package it.unibo.bd1819

import utils.DFFactory._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}


object ScalaMain extends App {

  val sc =  new SparkContext()
  val sqlContext = SparkSession.builder().getOrCreate().sqlContext

  val titleBasicsDF = getTitleBasicsDF(sc, sqlContext)
  val titlePrinicipalsDF = getTitlePrincipalsDF(sc, sqlContext)
  val nameBasicsDF = getNameBasicsDF(sc, sqlContext)

  sqlContext.sql("select * from " + TITLE_BASICS_TABLE_NAME).show()
  sqlContext.sql("select * from " + TITLE_PRINCIPALS_TABLE_NAME).show()
  sqlContext.sql("select * from " + NAME_BASICS_TABLE_NAME).show()

}
