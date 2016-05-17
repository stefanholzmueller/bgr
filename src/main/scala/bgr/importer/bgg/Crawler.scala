package bgr.importer.bgg

import scalaj.http.Http
import scalikejdbc._

object Crawler {
  val ID_BATCHES = 2000
  val ID_BATCH_SIZE = 100
  val ID_START = 1

//  Class.forName("com.mysql.jdbc.Driver")
//  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg20160516", "root", "root")
  implicit val session = AutoSession

  def crawlItems = { // took 7 hours
    for (batch <- 0 until ID_BATCHES) {
      val start = batch * ID_BATCH_SIZE + ID_START
      val thingIds = start until start + ID_BATCH_SIZE
      download(thingIds, 1, true)
    }
  }

  def crawlRatings = { // took 5 hours or so
    for ((page, batchOfIds) <- optimizePaging; ids <- batchOfIds) {
      download(ids, page, false)
    }
  }

  def optimizePaging = {
    type ItemId = Int
    val list = sql"SELECT itemId, ratingsCount FROM item WHERE type = 'boardgame' AND ratingsCount > 100".map { wrs =>
      (wrs.get("ratingsCount"): Int, wrs.get("itemId"): ItemId)
    }.list.apply()
    val pageCountToIds = list.map {
      case (ratingsCount, itemId) =>
        (ratingsCount / 100 + 1, itemId)
    }
    val pagesToIds = pageCountToIds.flatMap {
      case (pageCount, itemId) =>
        val pages = (2 to pageCount).toList
        pages.map { page => (page, itemId) }
    }
    val mapOfPagesToIds = pagesToIds.groupBy(_._1).mapValues(list => list.map(_._2))
    val mapOfPagesToBatchesOfIds = mapOfPagesToIds.mapValues { listOfItemIds => listOfItemIds.grouped(ID_BATCH_SIZE).toList }
    mapOfPagesToBatchesOfIds.toIndexedSeq.sortBy { case (page, itemIds) => page }.reverse
  }

  def download(ids: Seq[Int], page: Int, includeStats: Boolean) = {
    Thread.sleep(500)

    val map: Map[String, String] = Map(
      "id" -> ids.map { i => Integer.toString(i) }.mkString(","),
      "ratingcomments" -> "1",
      "stats" -> (if (includeStats) "1" else "0"),
      // TODO "type" -> "boardgame" ... then filter in Parser should be unnecessary
      // but is is also okay for downloading ratings?
      "pagesize" -> "100",
      "page" -> Integer.toString(page))
    val url = "http://boardgamegeek.com/xmlapi2/thing?" + map.map { case (k, v) => k + "=" + v }.mkString("&")
    println(url)
    try {
      val response = Http(url).timeout(connTimeoutMs = 10000, readTimeoutMs = 50000).execute()
      if (response.code == 200) {
        sql"insert into raw (url, status, body) values (${url}, ${response.code}, ${response.body})".update.apply()
      } else {
        sql"insert into raw (url, status) values (${url}, ${response.code})".update.apply()
      }
    } catch {
      case e: Exception => sql"insert into raw (url, status) values (${url}, ${e.toString})".update.apply()
      e.printStackTrace()
    }
  }

}