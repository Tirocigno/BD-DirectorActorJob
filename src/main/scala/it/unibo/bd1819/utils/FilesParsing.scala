package it.unibo.bd1819.utils

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object FilesParsing {

  val FIELD_SEPARATOR = "\\t"
  /**
    * Map a string to a StructType schema
    * @param schemaString the string containing the schema
    * @param fieldSeparator the file separator used inside the schema string
    * @return a StructType containing the schemastring
    */
  def StringToSchema(schemaString: String, fieldSeparator: String = FIELD_SEPARATOR) =
    StructType(schemaString.split(fieldSeparator).map(fieldName => StructField( fieldName, StringType, true)))

}
