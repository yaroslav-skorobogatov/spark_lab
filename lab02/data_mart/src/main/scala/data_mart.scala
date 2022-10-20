object data_mart {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark

  import org.apache.spark.sql._
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.DataFrame

  import org.apache.spark.sql.cassandra._
  import com.datastax.spark.connector._
  import com.datastax.spark.connector.cql.CassandraConnector

  import org.elasticsearch.spark.sql._
  import org.elasticsearch.spark._

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    import spark.implicits._

    spark.conf.set("spark.cassandra.connection.host", "")
    spark.conf.set("spark.cassandra.connection.port", "")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    val Options_for_clients_df = Map("table" -> "clients",
      "keyspace" -> "labdata")

    val clients_df = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Options_for_clients_df)
      .load

    val clients_df_sort_age = clients_df.withColumn("age_cat", when(col("age").between(18, 24), "18-24")
      .when(col("age").between(25, 34), "25-34")
      .when(col("age").between(35, 44), "35-44")
      .when(col("age").between(45, 54), "45-54")
      .otherwise(">=55")
    )
      .select('uid,
        'gender,
        'age_cat
      )

    val weblogs = spark
      .read
      .format(source = "json")
      .load("hdfs:///labs/laba03/weblogs.json")
      .withColumn("visits_explode", explode(col("visits")))

    val weblogs_df = weblogs
      .select(col("uid"),
        col("visits_explode.timestamp").alias("timestamp"),
        col("visits_explode.url").alias("url")
      ).distinct() //были дубли

    val weblogs_df_parse_url = weblogs_df.withColumn("parse1", regexp_replace('url,"^https?://", ""))
      .withColumn("parse2", when(substring(col("parse1"),1,4) === "www.", substring(col("parse1"),5, 50)).otherwise(col("parse1")))
      .select('uid,
        'timestamp,
        substring_index(col("parse2"),"/",1).alias("domain")
      )

    val Options_for_visits_df = Map("es.nodes" -> "",
      "es.port" -> "",
      "es.nodes.wan.only"->"true",
      "es.net.http.auth.user" -> "yaroslav.skorobogatov",
      "es.net.http.auth.pass" -> "",
      "es.resource"->"/visits")

    val visits_df = spark
      .read
      .format("org.elasticsearch.spark.sql")
      .options(Options_for_visits_df)
      .load

    val filtered_visits_df = visits_df
      .filter('uid.isNotNull)
      .select(concat(lit("shop_"),lower(regexp_replace(col("category"),  "[\\-,\\ ]", "_"))).alias("shop_cat"),
        'uid)
      .withColumn("visits", lit(1))
      .groupBy(col("uid"))
      .pivot("shop_cat")
      .sum("visits")

    val category_web_df = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://")
      .option("dbtable", "domain_cats")
      .option("user", "yaroslav_skorobogatov")
      .option("password", "")
      .option("driver", "org.postgresql.Driver")
      .load

    val filtered_category_web_df = category_web_df.select(concat(lit("web_"),lower(regexp_replace(col("category"),  "[\\-,\\ ]", "_"))).alias("web_cat"),
      'domain)

    val web_visits = weblogs_df_parse_url.join(broadcast(filtered_category_web_df), Seq("domain"), "left")
      .withColumn("visits", lit(1))
      .groupBy(col("uid"))
      .pivot("web_cat")
      .sum("visits")
      .select('uid,
        'web_science,
        'web_gambling,
        'web_shopping,
        'web_books_and_literature,
        'web_home_and_garden,
        'web_games,
        'web_computer_and_electronics,
        'web_reference,
        'web_beauty_and_fitness,
        'web_law_and_government,
        'web_autos_and_vehicles,
        'web_pets_and_animals,
        'web_travel,
        'web_recreation_and_hobbies,
        'web_food_and_drink,
        'web_career_and_education,
        'web_finance,
        'web_internet_and_telecom,
        'web_arts_and_entertainment,
        'web_health,
        'web_news_and_media,
        'web_business_and_industry,
        'web_sports
      )

    val all_visits = web_visits.join(filtered_visits_df, Seq("uid"), "full")

    val final_df = all_visits.join(clients_df_sort_age, Seq("uid"), "full")

    final_df.write
      .format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:postgresql://")
      .option("dbtable", "clients")
      .option("user", "yaroslav_skorobogatov")
      .option("password", "")
      .option("driver", "org.postgresql.Driver")
      .save()

  spark.close()
  }
}
