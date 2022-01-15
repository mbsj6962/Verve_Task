package com.verve.challenge

import java.io.File

import com.verve.challenge.model.Result

import scala.collection.mutable.ArrayBuffer


object main {
  val resultAddress = "src\\test\\resources\\resultSet\\results.txt"

  val impressionsFile = os.pwd / "\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\impressions.json"
  val clicksFile = os.pwd / "\\Users\\m.basij\\IdeaProjects\\Verve_Task\\src\\test\\resources\\clicks.json"

  val reader = new Reader();
  val impressions = reader.readImpression(impressionsFile)
  val clicks = reader.readClick(clicksFile)


  val country_codes = impressions.map(_.value).map(x => Map(x.get("id").getOrElse().toString -> x.get("country_code").getOrElse().toString)).groupBy(l => l).map(t => t._1)
  val app_ids = impressions.map(_.value).map(x => Map(x.get("id").getOrElse().toString -> x.get("app_id").getOrElse().toString)).groupBy(l => l).map(t => t._1)
  val mapClick = clicks.map(_.value).map(x => Map(x.get("impression_id").getOrElse().toString -> x.get("revenue").getOrElse().toString.toDouble))

  def main(args: Array[String]): Unit = {

    val clk = clicks.map(_.value.map(_._2)).map(_.headOption.orNull)
    val impr = impressions.map(_.value).map(_.get("id").getOrElse())
    val clkDist = clicks.map(_.value.map(_._2)).map(_.headOption.orNull).distinct
    val imprDist = impressions.map(_.value).map(_.get("id").getOrElse()).distinct
    val uncommon = (clkDist ++ imprDist).groupBy(l => l).map(t => (t._1, t._2.length)).filter(t => t._2 == 1).map(_._1)

    val revenueHashMap = mergeMap(mapClick)((v1, v2) => v1 + v2)
    val clicksHashMap = clk.groupBy(l => l).map(t => (t._1, t._2.length))
    val impressionHashMap = impr.groupBy(l => l).map(t => (t._1, t._2.length))

    var ccode: String = ""
    var aid: Int = 0
    var revenue: Double = 0.0
    var count_click: Int = 0
    var count_impression: Int = 0
    val listResult: ArrayBuffer[Result] = new ArrayBuffer[Result]()


    // Impressions which made users click to produce revenue for company
    for (id <- imprDist) {
      for (imp_id <- clkDist) {
        val res = Result(1, "", 1.1, 1, 1);
        if (id.equals(imp_id)) {
          for (ai <- app_ids) {
            if (ai.get(id.toString).getOrElse().getClass.equals("".getClass)) {
              aid = ai.get(id.toString).getOrElse().toString.replace("\"", "").toInt
            }
          }
          country_codes
            .foreach(cc => if (!cc.get(id.toString).getOrElse().getClass.equals("".getClass)) {
            } else {
              ccode = cc.get(id.toString).getOrElse().toString
            })
          count_click = clicksHashMap.get(id.toString.replace("\"", "")).get.toString.toInt
          count_impression = impressionHashMap.get(id).get.toString.toInt
          revenue = revenueHashMap.get(id.toString).getOrElse().toString.toDouble
          res.clicks = count_click
          res.impressions = count_impression
          res.country_code = ccode
          res.app_id = aid
          res.revenue = revenue
          listResult.append(res)
        }

      }
    }

    //    impressions which couldn't make user click and create revenue for company
    for (id <- uncommon) {
      val res = Result(1, "", 1.1, 1, 1);

      app_ids
        .foreach(ai => if (ai.get(id.toString).getOrElse().getClass.equals("".getClass)) {
          aid = ai.get(id.toString).getOrElse().toString.replace("\"", "").toInt
        })
      country_codes
        .foreach(cc => if (!cc.get(id.toString).getOrElse().getClass.equals("".getClass)) {
        } else {
          ccode = cc.get(id.toString).getOrElse().toString
        })
      count_click = 0
      count_impression = impressionHashMap.get(id).get.toString.toInt
      revenue = 0.0
      res.clicks = count_click
      res.impressions = count_impression
      res.country_code = ccode
      res.app_id = aid
      res.revenue = revenue
      listResult.append(res)

    }
    val expectedResult: ArrayBuffer[String] = listResult.map(x => toJson(x))

    printToFile(new File(resultAddress)) { p =>
      expectedResult.foreach(p.println)
    }
  }

  def mergeMap[A, B](ms: ArrayBuffer[Map[A, B]])(f: (B, B) => B): Map[A, B] =
    (Map[A, B]() /: (for (m <- ms; kv <- m) yield kv)) { (a, kv) =>
      a + (if (a.contains(kv._1)) kv._1 -> f(a(kv._1), kv._2) else kv)
    }

  def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try {
      op(p)
    } finally {
      p.close()
    }
  }
  def toJson(res: Result): String = {
    "{" + "\n" +
      "app_id: " + res.app_id + "\n" +
      "country_code: " + res.country_code + "\n" +
      "impressions: " + res.impressions + "\n" +
      "clicks: " + res.clicks + "\n" +
      "revenue: " + res.revenue + "\n" +
      "}" + ","
  }
}
