package it.unibo.bd1819

import utils.DFFactory._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}


object ScalaMain extends App {

  val sc =  new SparkContext()
  val sqlContext = new SQLContext(sc)

  getTitleBasicsDF(sc, sqlContext)
  getTitlePrincipalsDF(sc, sqlContext)
  sqlContext.sql("select * from " + TITLE_BASICS_TABLE_NAME).show()
  sqlContext.sql("select * from " + TITLE_PRINCIPALS_TABLE_NAME).show()

}
