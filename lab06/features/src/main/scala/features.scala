import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object features {

  def main(args: Array[String]): Unit = {

    def spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    val weblogs = spark
      .read
      .format(source = "json")
      .load("hdfs:///labs/laba03/weblogs.json")
      .withColumn("visits_explode", explode(col("visits")))


    val weblogs_df = weblogs
      .select(col("uid"),
        col("visits_explode.timestamp").alias("timestamp"),
        col("visits_explode.url").alias("url")
      )

    val weblogs_df_parse_url = weblogs_df.withColumn("host", lower(callUDF("parse_url", col("url"), lit("HOST"))))
      .withColumn("domain", regexp_replace(col("host"), "www.", ""))
      .withColumn("datetime",(col("timestamp")/1000).cast(TimestampType))
      .select(col("uid"),
        col("datetime"),
          col("domain")
        //regexp_replace(col("domain"), "\\.","_").alias("domain")
        //substring_index(col("parse2"),"/",1).alias("domain")
      ).cache()


    val top_visits = weblogs_df_parse_url.withColumn("lit_visits", lit(1))
      .groupBy(col("domain"))
      .agg(sum("lit_visits").alias("visits"))
      .orderBy(col("visits") desc)
      .limit(1000).cache()

    val cols = top_visits.select(col("domain")).collect().map("`"+_.getString(0)+"`").toSeq.sorted

    val top_domain = top_visits.join(weblogs_df_parse_url, Seq("domain"), "left")
      .withColumn("visits", lit(1))
      .orderBy("domain")
      .groupBy(col("uid"))
      .pivot("domain")
      .sum("visits")

    top_visits.unpersist()

    val addAttr = weblogs_df_parse_url
      .withColumn("week_day_full", concat(lit("web_day_"),lower(substring(date_format(col("datetime"), "EEEE").cast(StringType),1,3))))
      .groupBy(col("uid"))
      .pivot("week_day_full")
      .agg(count(col("datetime")))

    val addAttr2 = weblogs_df_parse_url
      .withColumn("hours", concat(lit("web_hour_"),regexp_replace(date_format(col("datetime"), "HH"), "^0","")))
      .groupBy(col("uid"))
      .pivot("hours")
      .agg(count(col("datetime")))

    val addAttr3 = weblogs_df_parse_url.withColumn("hours", date_format(col("datetime"), "HH"))
      .withColumn("web_fraction_work_hours", when(col("hours").between(9, 17), 1).otherwise(0))
      .withColumn("web_fraction_evening_hours", when(col("hours").between(18, 24), 1).otherwise(0))
      .groupBy(col("uid"))
      .agg(( sum(col("web_fraction_work_hours")) / count(col("hours"))).alias("web_fraction_work_hours"),
        (sum(col("web_fraction_evening_hours")) / count(col("hours"))).alias("web_fraction_evening_hours"))

    weblogs_df_parse_url.unpersist()

    val joinDf = addAttr.join(addAttr2, Seq("uid"), "full")
      .join(addAttr3, Seq("uid"), "full")
      .join(top_domain, Seq("uid"), "full")

    val user_item = spark.read.format("parquet").load(s"/user/yaroslav.skorobogatov/users-items/20200429/*")

    val dffinal = user_item.join(joinDf, Seq("uid"), "full")
      .na.fill(0)
      .withColumn("domain_features", array(cols.map(m=>col(m)):_*))
      .drop(cols:_*)

    dffinal
      .write
      .mode("overwrite")
      .format("parquet")
      .save("/user/yaroslav.skorobogatov/features")

  }
}