import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object filter {

  def main(args: Array[String]): Unit = {

    def spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.sparkContext.setLogLevel("WARN")

    val topic = spark.conf.get("spark.filter.topic_name").toString
    val offset = spark.conf.get("spark.filter.offset").toString
    val output_dir = spark.conf.get("spark.filter.output_dir_prefix").toString

    val StartingOffSets = if (offset == "earliest" || offset == "latest"){
      offset
    } else {
      s"""{"$topic":{"0":$offset}}"""
    }

    val df = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "")
      .option("subscribe", topic)
      .option("startingOffsets", StartingOffSets)
      .load()

    val df2 = df.select(col("value").cast("string"))

    val schema = new StructType()
      .add("event_type",StringType)
      .add("category",StringType)
      .add("item_id",StringType)
      .add("item_price",LongType)
      .add("uid",StringType)
      .add("timestamp",LongType)

    val df3 = df2.select(from_json(col("value"), schema).as("data")).select("data.*")

    val df4 = df3.withColumn("date", date_format(to_date(from_unixtime(col("timestamp")/1000)), "yyyyMMdd"))
      .withColumn("date_p", col("date"))

    val dfBye = df4.filter(col("event_type") === "buy")

    dfBye
      .write
      .mode("overwrite")
      .partitionBy("date_p")
      .format("json")
      .save(s"$output_dir/buy")

    val dfView = df4.filter(col("event_type") === "view")

    dfView
      .write
      .mode("overwrite")
      .partitionBy("date_p")
      .format("json")
      .save(s"$output_dir/view")

    spark.stop()
  }
}