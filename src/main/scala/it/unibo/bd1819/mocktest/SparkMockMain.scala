package it.unibo.bd1819.mocktest

import org.apache.spark.SparkContext

object SparkMockMain extends App {

  val sc =  new SparkContext()
  val rddCapra = sc.textFile("hdfs:/bigdata/dataset/capra/capra.txt")
  val rddDC = sc.textFile("hdfs:/bigdata/dataset/divinacommedia")

  // Exercise 0

  rddCapra.count()
  rddCapra.collect()

  val rddCapraWords1 = rddCapra.map( x => x.split(" ") )
  rddCapraWords1.collect()
  val rddCapraWords2 = rddCapra.flatMap( x => x.split(" ") )
  rddCapraWords2.collect()

  val rddDcWords1 = rddDC.map( x => x.split(" ") )
  println("OUTPUT:" + rddDcWords1.collect())
  val rddDcWords2 = rddDC.flatMap( x => x.split(" ") )
  println("OUTPUT:" + rddDcWords2.collect())
}
