import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}

object SpeedBandConsumer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SpeedBand Sink")
      .master("local")
      .getOrCreate()

    val inputDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "speedband")
      .load()

    val struct = new StructType()
      .add("LinkID", DataTypes.StringType)
      .add("RoadName", DataTypes.StringType)
      .add("RoadCategory", DataTypes.StringType)
      .add("SpeedBand", DataTypes.StringType)
      .add("MinimumSpeed", DataTypes.StringType)
      .add("MaximumSpeed", DataTypes.StringType)
      .add("Location", DataTypes.StringType)
      .add(name = "time", DataTypes.StringType)

    val jsonDf = inputDf.selectExpr("CAST(value AS STRING)")
    val nested = jsonDf.select(from_json(col("value"), struct).as("sp"))
        .selectExpr("sp.LinkID","sp.RoadName", "sp.RoadCategory",
          "cast(sp.SpeedBand as integer)", "cast(sp.MinimumSpeed as integer)",
          "cast(sp.MaximumSpeed as integer)", "sp.Location", "cast(sp.time as long)")
        .writeStream
        .outputMode("append")
        .format("console")
        .start()


    nested.awaitTermination()

  }
}
