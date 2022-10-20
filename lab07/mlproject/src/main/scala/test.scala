import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructType}

object test {

  def main(args: Array[String]): Unit = {

    def spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    val input_topic = spark.conf.get("spark.test.input_topic").toString
    val output_topic = spark.conf.get("spark.test.output_topic").toString
    val input_model_path = spark.conf.get("spark.test.input_model_path").toString

    val kafka_params = Map(
      "kafka.bootstrap.servers" -> "",
      "subscribe" -> s"$input_topic",
      "startingOffsets" -> "earliest" //,
      //"maxOffsetsPerTrigger"-> "5",
      //"minPartitions"-> "10"
    )

    val topic = spark
      .readStream
      .format("kafka")
      .options(kafka_params)
      .load()

    val schema = new StructType()
      .add("uid", StringType)
      .add("visits", ArrayType(new StructType()
        .add("url", StringType)
        .add("timestamp", LongType)
      ))

    val parse_topic = topic.select(from_json(col("value").cast("string"), schema).as("data")).select("data.*")

    val result = parse_topic
      .withColumn("visits_explode", explode(col("visits")))
      .withColumn("host", lower(callUDF("parse_url", col("visits_explode.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"),
        col("domain"),
        col("visits_explode.timestamp").alias("timestamp"))

    val test = result.groupBy("uid").agg(collect_list("domain").alias("domains"))

    val model = PipelineModel.load(s"$input_model_path")

    val df_final = model.transform(test).select(col("uid"), col("gender_age"))

    df_final
      .selectExpr("'key' AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .trigger(Trigger.ProcessingTime("25 seconds"))
      .option("checkpointLocation", "/user/yaroslav.skorobogatov/model/tmp")
      .option("kafka.bootstrap.servers", "")
      .option("topic", s"$output_topic")
      .outputMode("update")
      .start()
      .awaitTermination()
  }
}
