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
    var minYval = inputTimeSeries.agg(min(inputTimeSeries("yval"))).first().getDouble(0)
    print(minYval)
  }
}
