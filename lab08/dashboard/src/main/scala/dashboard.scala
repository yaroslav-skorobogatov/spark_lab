import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.PipelineModel

object dashboard {

  def main(args: Array[String]): Unit = {

    def spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    val df = spark.read.format(source = "json")
      .load("/labs/laba08")

    val result = df
      .withColumn("visits_explode", explode(col("visits")))
      .withColumn("host", lower(callUDF("parse_url", col("visits_explode.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("date"),
        col("uid"),
        col("domain"),
        col("visits_explode.timestamp").alias("timestamp"))

    val test = result.groupBy(col("date"),col("uid")).agg(collect_list("domain").alias("domains"))

    val model = PipelineModel.load(s"/user/yaroslav.skorobogatov/gender_age_model")

    val df_final = model.transform(test).select(col("date"),col("uid"), col("gender_age"))

    val Options = Map("es.nodes" -> "",
      "es.port" -> "",
      "es.nodes.wan.only"->"true",
      "es.net.http.auth.user" -> "yaroslav.skorobogatov",
      "es.net.http.auth.pass" -> "",
      "es.resource"->"/yaroslav_skorobogatov_lab08/_doc")

    df_final
      .write
      .format("org.elasticsearch.spark.sql")
      .options(Options)
      .save

    spark.close()

  }

}