package bgr.importer.bgg

import scalaj.http.Http
import scalikejdbc._

object Crawler {
  val ID_BATCHES = 100000
  val ID_BATCH_SIZE = 2
  val ID_START = 1

  implicit val session = AutoSession

  def crawlItems = {
    for (batch <- 0 until ID_BATCHES) {
      val start = batch * ID_BATCH_SIZE + ID_START
      val thingIds = start until start + ID_BATCH_SIZE
      download(thingIds, 1, true)
    }
  }

  def crawlRatings = {
    for ((page, batchOfIds) <- optimizePaging; ids <- batchOfIds) {
      download(ids, page, false)
    }
  }

  def optimizePaging = {
    type ItemId = Int
    val list = sql"SELECT itemId, ratingsCount FROM item WHERE ratingsCount > 100".map { wrs =>
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

  def recrawlFailures = {
    val idsAndUrls = sql"SELECT id, url FROM raw WHERE status != 200 OR body = ''".map { wrs =>
      (wrs.get("id"): Int, wrs.get("url"): String)
    }.list.apply().toSet[(Int, String)] // randomize
    idsAndUrls.foreach {
      case (id, url) =>
        downloadUrl(url)
        sql"delete from raw where id = ${id}".update.apply()
    }
  }

  def download(ids: Seq[Int], page: Int, includeStats: Boolean) = {
    Thread.sleep(500)

    val map: Map[String, String] = Map(
      "id" -> ids.map { i => Integer.toString(i) }.mkString(","),
      "ratingcomments" -> "1",
      "stats" -> (if (includeStats) "1" else "0"),
      "type" -> "boardgame",
      "pagesize" -> "100",
      "page" -> Integer.toString(page))
    val url = "http://boardgamegeek.com/xmlapi2/thing?" + map.map { case (k, v) => k + "=" + v }.mkString("&")
    downloadUrl(url)
  }

  def downloadUrl(url: String) = {
    println(url)
    try {
      val response = Http(url).timeout(connTimeoutMs = 10000, readTimeoutMs = 300000).execute()
      if (response.code == 200) {
        sql"insert into raw (url, status, body) values (${url}, ${response.code}, ${response.body})".update.apply()
      } else {
        sql"insert into raw (url, status) values (${url}, ${response.code})".update.apply()
        println("HTTP " + response.code)
      }
    } catch {
      case e: Exception =>
        sql"insert into raw (url, status) values (${url}, ${e.toString})".update.apply()
        e.printStackTrace()
    }
  }

}