//package com.verve.task1_2
//
//import java.io.{FileNotFoundException, IOException}
//
//import ujson.Num
//
//import scala.Console.{out, println}
//import scala.collection.mutable.ArrayBuffer
//import scala.collection.mutable.ListBuffer
//
//object main {
//
//  def main(args: Array[String]): Unit = {
//
//    val impressionsAddress = os.pwd / "\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\impressions.json"
//    val clickAddress = os.pwd / "\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\clicks.json"
//
//    val reader = new Reader
//    val impressionJson = reader.readImpression(impressionsAddress)
//    val clickJson = reader.readClick(clickAddress)
//
//
//    var app_id: Int = 0
//    var country_code: String = ""
//    var impressions: Int = 0
//    var clicks: Int = 0
//    var revenue: Double = 0.0
//
//    var app_id1: Int = 0
//    var country_code1: String = ""
//
//    var impressionId: String = ""
//    var id: String = ""
//    //    var revenueClick: Double = 0.0
//println("Start :!")
//    for (impression1 <- impressionJson) {
//      val outPutElements = OutPutElements(0, "", 0, 0, 0.0)
//      app_id = impression1.obj.get("app_id").getOrElse().toString.replace("\"", "").toInt
//      country_code = impression1.obj.get("country_code").getOrElse().toString
//      for (impression2 <- impressionJson){
//
//      }
//      println(outPutElements)
//    }
//  }
//}
