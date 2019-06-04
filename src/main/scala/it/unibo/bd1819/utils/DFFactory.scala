package it.unibo.bd1819.utils

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext}

/**
  * A Factory object to build all the needed dataframe.
  */
object DFFactory {

  val TITLE_BASICS_TABLE_NAME = "titlebasics"
  val TITLE_PRINCIPALS_TABLE_NAME = "titleprincipals"
  val NAME_BASICS_TABLE_NAME = "namebasics"

  /**
    * Build the Title Basics Dataframe and save the temp table.
    * @param sparkContext the specific spark context
    * @param sqlContext the sql contex to interrogate
    * @param tableName the name of the table to set.
    * @return a DF linked to the title.basics data
    */
  def getTitleBasicsDF(sparkContext: SparkContext, sqlContext: SQLContext, tableName:String = TITLE_BASICS_TABLE_NAME) = {
    val titleBasicsTSV = sparkContext.textFile(Path.TITLE_BASICS_PATH)
    val titleSchema = titleBasicsTSV.first()
    val titleSchemaType = FilesParsing.StringToSchema(titleSchema, FilesParsing.FIELD_SEPARATOR ,
      buildTitleBasicsFilterCriteria())
    val titleSchemaRDD = titleBasicsTSV.map(_.split(FilesParsing.FIELD_SEPARATOR))
      .filter(_(1).equals("movie"))
      .map(e => Row(e(0)))
    val titleBasicsDF = sqlContext.createDataFrame(titleSchemaRDD, titleSchemaType)
    titleBasicsDF.createOrReplaceTempView(tableName)
    titleBasicsDF
  }

  /**
  * Build the Title Principals Dataframe and save the temp table.
    * @param sparkContext the specific spark context
    * @param sqlContext the sql contex to interrogate
  * @param tableName the name of the table to set.
    * @return a DF linked to the title.principals data
    */
  def getTitlePrincipalsDF(sparkContext: SparkContext, sqlContext: SQLContext, tableName:String = TITLE_PRINCIPALS_TABLE_NAME) = {
    val titlePrincipalsTSV = sparkContext.textFile(Path.TITLE_PRINCIPALS_PATH)
    val titleSchema = titlePrincipalsTSV.first()
    val titleSchemaType = FilesParsing.StringToSchema(titleSchema, FilesParsing.FIELD_SEPARATOR ,
      buildTitlePrincipalsFilterCriteria())
    val titleSchemaRDD = titlePrincipalsTSV.map(_.split(FilesParsing.FIELD_SEPARATOR))
      .filter(e => titlePrincipalsFilterRowByCategory(e(3)))
      .map(e => Row(e(0), e(2)))
    val titlePrincipalsDF = sqlContext.createDataFrame(titleSchemaRDD, titleSchemaType)
    titlePrincipalsDF.createOrReplaceTempView(tableName)
    titlePrincipalsDF
  }

  /**
    * Build the Title Principals Dataframe and save the temp table.
    * @param sparkContext the specific spark context
    * @param sqlContext the sql contex to interrogate
    * @param tableName the name of the table to set.
    * @return a DF linked to the title.principals data
    */
  def getNameBasicsDF(sparkContext: SparkContext, sqlContext: SQLContext, tableName:String = NAME_BASICS_TABLE_NAME) = {
    val nameBasicsTSV = sparkContext.textFile(Path.NAME_BAISCS_PATH)
    val basicSchema = nameBasicsTSV.first()
    val basicSchemaType = FilesParsing.StringToSchema(basicSchema, FilesParsing.FIELD_SEPARATOR ,
      buildNameBasicsFilterCriteria())
    val basicsSchemaRDD = nameBasicsTSV.map(_.split(FilesParsing.FIELD_SEPARATOR))
      .map(e => Row(e(0), e(1)))
    val nameBasicsDF = sqlContext.createDataFrame(basicsSchemaRDD, basicSchemaType)
    nameBasicsDF.createOrReplaceTempView(tableName)
    nameBasicsDF
  }

  /**
    * Filter the fields of the title.basics table, in order to reduce the amount of data stored in the memory
    * @return a String- Boolean filter function
    */
  private def buildTitleBasicsFilterCriteria() = {
    val usefulFields = Set("tconst")
    val filterCriteria: String => Boolean = usefulFields(_)
    filterCriteria
  }

  /**
    * Filter the fields of the title.principals table, in order to reduce the amount of data stored in the memory
    * @return a String- Boolean filter function
    */
  private def buildTitlePrincipalsFilterCriteria() = {
    val usefulFields = Set("tconst", "nconst")
    val filterCriteria: String => Boolean = usefulFields(_)
    filterCriteria
  }

  private def titlePrincipalsFilterRowByCategory(category:String) =
    category.equals("director") || category.equals("actor") || category.equals("actress")

  /**
    * Filter the fields of the title.basics table, in order to reduce the amount of data stored in the memory
    * @return a String- Boolean filter function
    */
  private def buildNameBasicsFilterCriteria() = {
    val usefulFields = Set("nconst", "primaryName")
    val filterCriteria: String => Boolean = usefulFields(_)
    filterCriteria
  }

}
