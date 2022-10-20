import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{CountVectorizer, StringIndexer, IndexToString}
import org.apache.spark.ml.{Pipeline}

object train {

  def spark = SparkSession
    .builder()
    .appName("SPARK_YAR")
    .getOrCreate()

  val input_path = spark.conf.get("spark.train.input_path").toString
  val output_model_path = spark.conf.get("spark.train.output_model_path").toString

  def main(args: Array[String]): Unit = {
    val weblogs = spark
      .read
      .format(source = "json")
      .load(s"$input_path")
      .withColumn("visits_explode", explode(col("visits")))

    val parse_weblogs = weblogs
      .withColumn("host", lower(callUDF("parse_url", col("visits_explode.url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .select(col("uid"),
        col("gender_age").alias("gender_age_original"),
        col("domain"),
        col("visits_explode.timestamp").alias("timestamp"))

    val training = parse_weblogs.groupBy("gender_age_original","uid").agg(collect_list("domain").alias("domains"))

    val cv = new CountVectorizer()
      .setInputCol("domains")
      .setOutputCol("features")

    val indexer = new StringIndexer()
      .setInputCol("gender_age_original")
      .setOutputCol("label")
      .fit(training) //

    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)

    val converter =new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("gender_age")
      .setLabels(indexer.labels)

    val pipeline = new Pipeline()
      .setStages(Array(cv, indexer, lr, converter))

    val model = pipeline.fit(training)

    model.write.overwrite().save(s"$output_model_path")
  }
}