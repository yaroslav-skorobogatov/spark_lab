object Main {

  import org.apache.spark.sql.SparkSession

  import scala.util.{Try, Success, Failure}
  import java.net.URLDecoder
  //import org.apache.spark.sql.functions.udf
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.DataFrame
  import org.apache.spark.sql.types.DataTypes._
  import org.apache.spark.sql.Column

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    val Options = Map(
      "interSchema" -> "true",
      "delimiter" -> "\\t",
      "encoding" -> "UTF-8"
    )

    val df = spark.read.format(source = "csv")
      .options(Options)
      .load("hdfs:///labs/laba02/logs")
      .select(
        col("_c0").alias("UID"),
        col("_c1").alias("timestamp"),
        col("_c2").alias("URL")
      ).na.drop(Seq("URL","UID"))

    val decode_udf = udf { (url: String) =>
      Try(URLDecoder.decode(url, "UTF-8")) match {
        case Success(url: String) => url
        case Failure(exc) => ""
      }
    }

    val temp = df.withColumn("newcol", decode_udf(col("URL")))
      .filter(substring(col("newcol"),1,4) === "http")

    val parse = temp.withColumn("HOST", regexp_replace(col("newcol"), "^https?://", ""))
      .select(col("UID"),
        substring_index(col("HOST"),"/",1).alias("HOST"))

    val df_parse =  parse.select(
      col("UID"),
      when(substring(col("HOST"),1,4) === "www.", substring(col("HOST"),5, 50)).otherwise(col("HOST")).alias("HOST")
    )


    val autousers = spark.read.format(source = "json")
      .load("hdfs:///labs/laba02/autousers.json")
      .withColumn("autousers", explode(col("autousers")).as("autousers"))

    val join_users = df_parse.join(autousers, col("UID")===col("autousers"), "left")
      .select(
        col("HOST"),
        col("UID"),
        when(col("autousers").isNotNull, 1).otherwise(0).alias("autousers")
      )//.na.drop(Seq("UID","HOST"))

    val count_UID = join_users.agg(sum(col("autousers"))).first.getLong(0).toDouble //313523.0
    val count_all = join_users.agg(count("*")).first.getLong(0).toDouble //6571018.0

    println(s"count_UID = $count_UID , count_all= $count_all")

    val df_agg = join_users
      .groupBy(col("HOST")).agg(
      (
        ((sum(col("autousers"))/count_all)*(sum(col("autousers"))/count_all))
          /
          ((count(col("HOST"))/count_all)*(count_UID/count_all))
        ).cast("decimal(15,15)").alias("relevance")
    )
      .orderBy(col("relevance").desc, col("HOST").asc).limit(200)

    df_agg
      .write
      .format("csv")
      .mode("overwrite")
      .option("sep", "\t")
      .option("encoding", "UTF-8")
      .option("path","laba020_domains.txt")
      .save

    spark.close()

  }
}