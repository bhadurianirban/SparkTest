import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.functions.{collect_list, min}

class ReadProcess (sparkSession: SparkSession ){
  def readTestFile(XYTimeSeriesFile:String): Unit ={
    val schema = StructType(Seq(
      StructField("xval", DoubleType),
      StructField("yval", DoubleType)
    ))
    val sqlContext = sparkSession.sqlContext
    val inputTimeSeries = sqlContext.read.schema(schema).option("delimiter",",").csv(XYTimeSeriesFile)
    val filteredTimeSeries = inputTimeSeries.filter("yval>0.3")
    filteredTimeSeries.show()
  }
}
