package com.verve.task1_2

import scala.Console.println
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.parsing.json.JSON

object Impressions {
  def main(args: Array[String]): Unit = {

    val impressionsAddress = os.pwd / "\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\impressions.json"
    val clickAddress = os.pwd / "\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\clicks.json"
    val reader = new Reader
    val impressionJson = reader.readImpression(impressionsAddress)
    val clickJson = reader.readClick(clickAddress)


    var app_ids = new ListBuffer[Int]()
    var country_codes = new ListBuffer[String]()
    var impression_ids = new ListBuffer[String]()
    var countOfImpressions: Integer = 0

    for (click <- clickJson) {
      val impression_id = click.obj.values.iterator(0).strOpt.getOrElse().toString
      impression_ids += impression_id
    }

    println("impression_ids.size : " + impression_ids.size)
    impression_ids = impression_ids.distinct
    println("impression_ids.distinct.size : " + impression_ids.size)
    println("impression_ids.distinct : " + impression_ids)


//    val impressions = ujson.read(impressionJson).value.asInstanceOf[ArrayBuffer[ujson.Obj]]
    for (impression <- impressionJson) {
      val appid = impression.obj.get("app_id")
      if (appid.get.numOpt.eq(None)) {
        app_ids += appid.get.toString().replace("\"", "").toInt
        app_ids = app_ids.distinct
      }
      else {
        app_ids += appid.get.toString.toInt
        app_ids = app_ids.distinct

      }
      val countryCode = impression.obj.get("country_code")
      country_codes += countryCode.get.toString
      country_codes = country_codes.distinct
      countOfImpressions += 1
    }
    println("size of country_codes distinct{} " + app_ids.size)
    println("size of country_codes {} " + country_codes)


    for (appidDistincted <- app_ids) {
      for (countryCodeDistincted <- country_codes) {
        var Impression_Count: Int = 0
        for (impression <- impressionJson) {
          val countryCode = impression.obj.get("country_code").get.toString
          val appid = impression.obj.get("app_id")
          val applicationId = appid.get.toString().replace("\"", "").toInt
          if (appid.get.numOpt.eq(None)) {
            if (applicationId.equals(appidDistincted) && countryCode.equalsIgnoreCase(countryCodeDistincted)) {
              Impression_Count += 1
            }
            else {
            }
          }
          else {
            if (appid.get.toString.toInt.equals(appidDistincted) && countryCode.equalsIgnoreCase(countryCodeDistincted)) {
              Impression_Count += 1
            }
            else {
            }
          }

        }
        println("[" + "{app_id: " + appidDistincted + ", " +
          " country_code: " + countryCodeDistincted + ", " +
          " impressions: " + Impression_Count + ", " +
          //          " clicks: " + count_click + ", " +
          //          " revenue: " + revenue +
          "}" + "]")
      }
    }


  }

}
