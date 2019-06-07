package it.unibo.bd1819.utils

object Path {

  val GENERIC_HDFS_PREFIX = "hdfs://"
  val ABSOLUTE_HDFS_PATH = "/user/fnaldini/bigdata/dataset/"
  val TITLE_BASICS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH +"titlebasics/title.basics.tsv"
  val TITLE_PRINCIPALS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH + "principals/title.principals.tsv"
  val NAME_BAISCS_PATH = GENERIC_HDFS_PREFIX + ABSOLUTE_HDFS_PATH + "namebasics/name.basics.tsv"
}
