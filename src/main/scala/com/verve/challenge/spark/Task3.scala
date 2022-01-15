package com.verve.task1_2.spark


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, expr, struct}


object Task3 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Start ... ")
    System.setProperty("hadoop.home.dir", "C:\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\winutils-master\\hadoop-2.7.1\\bin")

    val spark = SparkSession.builder().appName("Verve").master("local[*]").getOrCreate()


    val impressionsDF = spark.read
      //      .schema(schema)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiline", "true")
      .json("C:\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\impressions.json")


    val clicksDF = spark.read
      //      .schema(schema)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiline", "true")
      .json("C:\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\clicks.json")


    impressionsDF.createOrReplaceTempView("impressionDF")
    clicksDF.createOrReplaceTempView("clicksDF")


    println("Starting pipeline  ...")


    val joinedDF =
      impressionsDF.join(clicksDF, impressionsDF("id") === clicksDF("impression_id"), "left").distinct()
        .select("advertiser_id", "app_id", "country_code", "id", "revenue")

    joinedDF.createOrReplaceTempView("joinedDF")


    val df_sum_revenue = spark.sql("select joinedDF.advertiser_id,joinedDF.app_id ,joinedDF.country_code ,joinedDF.id, sum(joinedDF.revenue) as revenue " +
      "from joinedDF " +
      "group by joinedDF.advertiser_id,joinedDF.app_id ,joinedDF.country_code,joinedDF.id ").distinct()
    //    df_sum_revenue.show(100000,false)

    //    val cols1 = List("revenue")
    //    val sums1 = cols1.map(colName => functions.sum(colName).cast("double").as("sum_" + colName))
    //    df_sum_revenue.groupBy().agg(sums1.head, sums1.tail: _*).show()


    df_sum_revenue.createOrReplaceTempView("dfWithRevenue")

    val df_count_id = spark.sql("select impressionDF.advertiser_id , impressionDF.app_id, impressionDF.country_code, impressionDF.id ,count(impressionDF.id) as impressions " +
      "from impressionDF " +
      "group by impressionDF.advertiser_id, impressionDF.country_code,impressionDF.app_id, impressionDF.id").distinct()

    df_count_id.createOrReplaceTempView("dfWithImpression")


    val mergedDF = spark.sql("select dfWithRevenue.advertiser_id, dfWithRevenue.app_id, dfWithRevenue.country_code, " +
      " dfWithRevenue.revenue / dfWithImpression.impressions as revenuePerImpression " +
      "from dfWithRevenue inner join dfWithImpression on dfWithRevenue.id = dfWithImpression.id").distinct()


    mergedDF.createOrReplaceTempView("mergedDF")


    val df = spark.sql("select *" +
      //      " listagg(x.advertiser_id,',') WITHIN GROUP (order by x.revenuePerImpression) " +
      " from (select * from (select mergedDF.advertiser_id,mergedDF.app_id, mergedDF.country_code,mergedDF.revenuePerImpression,row_number() over (partition by mergedDF.app_id, mergedDF.country_code order by nvl(mergedDF.revenuePerImpression,0) desc) num_ from mergedDF) where num_ <= 5 order by num_ desc) ")


    val ds = df.withColumn("advertiser_ids", struct(col("advertiser_id"), col("app_id"), col("country_code"),
      col("revenuePerImpression").cast("double"), col("num_").cast("Int")))
      .groupBy("app_id", "country_code")
      .agg(concat_ws("-", expr("sort_array(collect_list(advertiser_ids)).advertiser_id")).alias("recommended_advertiser_ids"))

    ds.show(100, false)

    println("End ...")
  }
}
