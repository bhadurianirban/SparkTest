package org.dgrf.SparkTest

import org.apache.spark.sql.SparkSession

object TestMain {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("bheua").master("local").getOrCreate()
    val inputFile = "/home/bhaduri/MEGA/DGRFFractal/testdata/samplexysm.csv"
    val rp =  new ReadProcess(sparkSession)
    rp.readTestFile(inputFile)
  }

}
