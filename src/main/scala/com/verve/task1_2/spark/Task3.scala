package com.verve.task1_2.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Task3 {

  def main(args: Array[String]): Unit = {

    println("Start ... ")
    System.setProperty("hadoop.home.dir", "C:\\Users\\m.basij\\Documents\\winutils-master\\hadoop-2.7.1\\bin")

    val spark = SparkSession.builder().appName("Verve").master("local[*]").getOrCreate()

    //    val schema = new StructType()
    //      .add("app_id", StringType, true)
    //      .add("advertiser_id", StringType, true)
    //      .add("country_code", StringType, true)
    //      .add("id", StringType, true)

    val impressionsDF = spark.read
      //      .schema(schema)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiline", "true")
      .json("C:\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\impressions.json")

//    println("Schema impressionsDF")
//    impressionsDF.printSchema()
//
//
//    println("Show impressionsDF")
//
//    impressionsDF.show(false)


    val clicksDF = spark.read
      //      .schema(schema)
      .option("inferSchema", "true")
      .option("header", "true")
      .option("multiline", "true")
      .json("C:\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\clicks.json")

    println("Schema clicksDF")
//    clicksDF.printSchema()
    println("Show clicksDF")
//    clicksDF.show(false)

    impressionsDF.createOrReplaceTempView("impressionDF")
    clicksDF.createOrReplaceTempView("clicksDF")

    /*val joinedDF =*/ impressionsDF.join(clicksDF,impressionsDF("id") ===  clicksDF("impression_id"),"fullouter")
      .select("app_id","impression_id","advertiser_id","country_code","revenue")
//      .groupBy("app_id","country_code","advertiser_id")
//      .count()
//      .sort()
      .show(100000,false)

//    spark.sql("select i.")

    println("Joined DFs ...")

    println("End ...")
  }
}
