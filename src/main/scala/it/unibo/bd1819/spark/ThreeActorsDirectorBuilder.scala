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

  def apply(sqlContext: SQLContext): ThreeActorsDirectorBuilder =
    new ThreeActorsDirectorBuilderImpl(sqlContext)

  private class ThreeActorsDirectorBuilderImpl(sqlContext: SQLContext) extends ThreeActorsDirectorBuilder {

    var directorActorCounterMap: scala.collection.mutable.Map[String, DirectorEntryValue] =
      scala.collection.mutable.Map()

    override def buildThreeActorsDirectorsDataFrame(initialDataFrame: DataFrame): DataFrame = {

      val threePartitionRDD = initialDataFrame.rdd.map(row => {
        val directorID = row.getAs[String]("DirectorID")
        val actorID = row.getAs[String]("ActorID")
        val collabCount = row.getAs[Long]("CollabMovies")
        (directorID, (actorID, collabCount))
      }).groupByKey
        .map {
          case (key, number) => key -> number.toList.sortBy(-_._2).take(3)
        }
        .flatMap {
          case (key, numbers) => numbers.map(key -> _)
        }.map(keyvaluerow =>
        Row(keyvaluerow._1, keyvaluerow._2._1, keyvaluerow._2._2))
      sqlContext.createDataFrame(threePartitionRDD, initialDataFrame.schema)
    }

    case class DirectorEntryValue(var topThree: (ActorCollabRecord, ActorCollabRecord, ActorCollabRecord) =
                                  (ActorCollabRecord(), ActorCollabRecord(), ActorCollabRecord())) {

      /**
        * Process a new record updating the topThree actors-collab records.
        *
        * @param record the record to process.
        */
      def processNewRecord(record: ActorCollabRecord): Unit = {
        if (record.actorCollab > topThree._1.actorCollab) {
          topThree = (record, topThree._1, topThree._2)
        } else {
          if (record.actorCollab > topThree._2.actorCollab) {
            topThree = (topThree._1, record, topThree._2)
          } else {
            if (record.actorCollab > topThree._3.actorCollab) {
              topThree = (topThree._1, topThree._2, record)
            }
          }
        }
      }

      /**
        * Check if the topthree records contains a specified record
        *
        * @param actorCollabRecord the recors to check
        * @return true if the record is present, false otherwise
        */
      def contains(actorCollabRecord: ActorCollabRecord): Boolean = {
        actorCollabRecord.equals(topThree._1) ||
          actorCollabRecord.equals(topThree._2) ||
          actorCollabRecord.equals(topThree._3)
      }


    }

    case class ActorCollabRecord(actorID: String = "", actorCollab: Long = 0)

    case class DirectorActorCountTuple(directorID: String, actorID: String, actorCollab: Long)

  }

}
