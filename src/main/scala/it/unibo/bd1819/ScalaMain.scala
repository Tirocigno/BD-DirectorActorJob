package it.unibo.bd1819

import it.unibo.bd1819.utils.{FilesParsing, Path}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ScalaMain extends App {

  val sc =  new SparkContext()
  val sqlContext = new SQLContext(sc)

  /*CSV FILE*/
  val titleBasicsTSV = sc.textFile(Path.TITLE_BASICS_PATH)
  val titleSchema = titleBasicsTSV.first()

  val titleSchemaType = FilesParsing.StringToSchema(titleSchema)

  val titleSchemaRDD = titleBasicsTSV.map(_.split(FilesParsing.FIELD_SEPARATOR))
    .map(e => Row(e(0), e(1), e(2), e(3), e(4), e(5), e(6), e(7), e(8)))

  val titleBasicsDF = sqlContext.createDataFrame(titleSchemaRDD, titleSchemaType)

  titleBasicsDF.registerTempTable("titlebasics")

  sqlContext.sql("select * from titlebasics").show()

}
