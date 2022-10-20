import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}

object users_items {

  def main(args: Array[String]): Unit = {

    def spark = SparkSession
      .builder()
      .appName("SPARK_YAR")
      .getOrCreate()

    val input_dir = spark.conf.get("spark.users_items.input_dir").toString
    val output_dir = spark.conf.get("spark.users_items.output_dir").toString
    val modeType = spark.sparkContext.getConf.getOption("spark.users_items.update").getOrElse(1)

    val dfView = spark.read.format("json")
      .load(s"$input_dir/view/*")

    val dfPreparationView = dfView.filter(col("uid").isNotNull)
      .select(concat(lit("view_"), lower(regexp_replace(col("item_id"), "[\\ ,\\-]", "_"))).alias("item_id"),
        lit(1).alias("browsing"),
        col("uid")
      )
      .groupBy(col("uid"))
      .pivot(col("item_id"))
      .agg(sum(col("browsing")))

    val dfBuy = spark.read.format("json")
      .load(s"$input_dir/buy/*")

    val dfPreparationBuy = dfBuy.filter(col("uid").isNotNull)
      .select(concat(lit("buy_"), lower(regexp_replace(col("item_id"), "[\\ ,\\-]", "_"))).alias("item_id"),
        lit(1).alias("browsing"),
        col("uid")
      )
      .groupBy(col("uid"))
      .pivot(col("item_id"))
      .agg(sum(col("browsing")))

    val ViewAndBuy = dfPreparationBuy.join(dfPreparationView, Seq("uid"), "full")

    val maxdate = dfView.select(col("date")).union(dfBuy.select(col("date"))).agg(max(col("date"))).first().getString(0)

    println(s"maxdate = $maxdate")

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val lsDir = fs.listStatus(new Path("/user/yaroslav.skorobogatov/users-items")).filter(_.isDir).map(_.getPath)
    val allMatrix = lsDir.map(x => x.toString.takeRight(8))
    val lastMatrix = if (allMatrix.length == 0) { maxdate } else { allMatrix.sorted.head }

    println(s"lastMatrix = $lastMatrix")

    val dffinal = if(modeType==1 && lastMatrix != maxdate) {
      val dfOld = spark.read.format("parquet").load(s"$output_dir/$lastMatrix/*")
      dfOld.union(ViewAndBuy.join(dfOld, Seq("uid"), "leftanti"))
    } else if(modeType==1 && allMatrix.length != 0) {
      val dfOld = spark.read.format("parquet").load(s"$output_dir/$lastMatrix/*")
      ViewAndBuy.join(dfOld, Seq("uid"), "leftanti")
    }else {
      ViewAndBuy
    }

    dffinal.na.fill(0)
      .write
      .mode(if(modeType==1 && lastMatrix != maxdate) "overwrite" else if (modeType==1 && allMatrix.length != 0) "append" else "overwrite")
      .format("parquet")
      .save(if(modeType==1 && lastMatrix != maxdate) s"$output_dir/$maxdate" else if (modeType==1 && allMatrix.length != 0) s"$output_dir/$lastMatrix" else s"$output_dir/$maxdate")


    spark.stop()

  }
}