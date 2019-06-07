package it.unibo.bd1819.spark

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * This trai will implement a conversion and a filtering for a DirectorActorCollaboration count dataframe
  * to a new dataframe with the same schema, but only the specified number of actor
  */
trait ThreeActorsDirectorBuilder extends java.io.Serializable {

  /**
    * Build a new DataFrame considering only the top three actors for each director
    * @param initialDataFrame the initial dataframe to parse
    * @return a new dataframe built from the previous schema.
    */
  def buildThreeActorsDirectorsDataFrame(initialDataFrame: sql.DataFrame): sql.DataFrame
}

object ThreeActorsDirectorBuilder {

  def apply(sqlContext: SQLContext, partitionNumber:Int): ThreeActorsDirectorBuilder =
    new ThreeActorsDirectorBuilderImpl(sqlContext, partitionNumber)

  private class ThreeActorsDirectorBuilderImpl(sqlContext: SQLContext, partitionNumber:Int) extends ThreeActorsDirectorBuilder {

    override def buildThreeActorsDirectorsDataFrame(initialDataFrame: DataFrame): DataFrame = {

      val threePartitionRDD = initialDataFrame.rdd
        .coalesce(8)
        .map(convertToKeyValueTuple)
        .groupByKey(partitionNumber)
        .map {
          case (directorID, actorsCollabIterable) => (directorID,actorsCollabIterable.toList.sortBy(-_._2).take(3))
        }
        .flatMap {
          case (directorID, topThreeActorList) => topThreeActorList.map((directorID,_))
        }.map(keyvaluerow =>
        Row(keyvaluerow._1, keyvaluerow._2._1, keyvaluerow._2._2))
      sqlContext.createDataFrame(threePartitionRDD, initialDataFrame.schema)
    }

    private def convertToKeyValueTuple(row: Row) = {
      val directorID = row.getAs[String]("DirectorID")
      val actorID = row.getAs[String]("ActorID")
      val collabCount = row.getAs[Long]("CollabMovies")
      (directorID, (actorID, collabCount))
    }

  }

}
