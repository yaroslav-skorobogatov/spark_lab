import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object agg {
  def main(args: Array[String]): Unit = {

    def spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafka_params = Map(
      "kafka.bootstrap.servers"->"",
      "subscribe"-> "yaroslav_skorobogatov",
      "startingOffsets"-> "earliest"//,
      //"maxOffsetsPerTrigger"-> "5",
      //"minPartitions"-> "10"
    )

    val topic = spark
      .readStream
      .format("kafka")
      .options(kafka_params)
      .load()

    val schema = new StructType()
      .add("event_type",StringType)
      .add("category",StringType)
      .add("item_id",StringType)
      .add("item_price",LongType)
      .add("uid",StringType)
      .add("timestamp",LongType)

    val parse_topic = topic.select(from_json(col("value").cast("string"), schema).as("data")).select("data.*")

    val result = parse_topic
      .withColumn("datetime",(col("timestamp")/1000).cast(TimestampType))
      .withColumn("visitors", when(col("uid").isNotNull, 1).otherwise(0))
      .withColumn("revenue", when(col("event_type") === "buy", col("item_price")).otherwise(0))
      .withColumn("purchases", when(col("event_type") === "buy", 1).otherwise(0))
      .groupBy(window(col("datetime"), "60 minutes"))
      .agg(
        sum(col("visitors")).alias("visitors"),
        sum(col("revenue")).alias("revenue"),
        sum(col("purchases")).alias("purchases"),
        (ceil(unix_timestamp(last(col("datetime")))/lit(3600))*lit(3600)).alias("end_ts"),
        unix_timestamp(first(col("datetime"))).alias("start_ts"))
      .select(col("start_ts"),
        col("end_ts"),
        col("revenue"),
        col("visitors"),
        col("purchases"),
        (col("revenue")/col("purchases")).alias("aov"))

    result
      .selectExpr("'key' AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "/user/yaroslav.skorobogatov/tmp")
      .option("kafka.bootstrap.servers", "")
      .option("topic", "yaroslav_skorobogatov_lab04b_out")
      .outputMode("update")
      .start()
      .awaitTermination()

  }
}