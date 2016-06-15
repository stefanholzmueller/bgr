package bgr.importer.bgg

import scalaj.http.Http
import scalikejdbc._

object Importer {

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg20160518", "root", "root")
  implicit val session = AutoSession

    val pioUrl = "http://192.168.178.38:7070"
    val pioAccessKey = "RihaFc2gCm3Fe2pa_2xNPvo4paiO-xzkRbMEutByjr_x1hPRdpvXsTGUyLzD2JD3"

  def main(args: Array[String]) {
    //    createTables
    //    Crawler.crawlItems  // 16:55 - 21:10 (batchsize 20)
    //    Crawler.recrawlFailures
    //    Parser.parseItems  // 1 minute
    //    Crawler.crawlRatings  // 22:15 - 7:00 (pages 24+, batchsize 2), 7:05 - 10:15 (pages 23-, batchsize 8)
    //    Crawler.recrawlFailures
    //    Parser.parseItemRatings  // 47 min
    //    filterForPopularity(50, 10) // 1 min
    uploadToPIO // ~3h (local vm)
    println(new java.util.Date())
  }

  def uploadToPIO() = {
    var list: List[String] = List()
    var count: Int = 0

    val itemratings = sql"SELECT itemratingid, userName, itemId, rating FROM itemrating_50_10"
    val ratings = itemratings.foreach { row =>
      val userName = row.string("userName")
      val itemId = row.string("itemId")
      val rating = row.float("rating")

      val event: String = s"""{"event":"rate","entityType":"user","entityId":"$userName","targetEntityType":"item","targetEntityId":"$itemId","properties":{"rating":$rating}}"""
      list = event :: list
      if (list.size == 50) {
        val json = list.mkString("[", ",", "]")
        list = List()

        val url = pioUrl + "/batch/events.json?accessKey=" + pioAccessKey
        val response = Http(url).method("POST").header("Content-Type", "application/json").postData(json).execute()
        if (response.code == 200) {
        	count += 50
          println(s"uploaded $count events: $json")
        } else {
          println("failed upload: url=" + url + ", payload=" + json)
        }
      }
    }
  }

  def filterForPopularity(ratingsCount: Int, timesVoted: Int) = {
    val tableName = s"itemrating_${ratingsCount}_${timesVoted}"
    val tableNameSQL = SQLSyntax.createUnsafely(tableName)
    createTableForRatings(tableName)
    sql"""INSERT INTO ${tableNameSQL}
SELECT *
FROM itemrating
WHERE itemid IN (SELECT itemid FROM itemrating GROUP BY itemid HAVING COUNT(itemid) >= ${ratingsCount})
  AND userid IN (SELECT userid FROM itemrating GROUP BY userid HAVING COUNT(userid) >= ${timesVoted})""".execute.apply() // TODO usersRated instead of ratingsCount
  }

  def createTables = {
    println("create table: raw")
    sql"""CREATE TABLE `raw` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `url` varchar(1000) NOT NULL,
  `status` varchar(255) NOT NULL,
  `body` longtext,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".execute.apply()

    println("create table: item")
    sql"""CREATE TABLE `item` (
  `itemid` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(100) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `yearpublished` varchar(100) DEFAULT NULL,
  `image` varchar(100) DEFAULT NULL,
  `thumbnail` varchar(100) DEFAULT NULL,
  `ratingscount` mediumint(9) DEFAULT NULL,
  `usersrated` mediumint(9) DEFAULT NULL,
  PRIMARY KEY (`itemid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".execute.apply()

    createTableForRatings("itemrating")
  }

  def createTableForRatings(tableName: String) = {
    println("create table: " + tableName)
    val tableNameSQL = SQLSyntax.createUnsafely(tableName)
    sql"""CREATE TABLE `${tableNameSQL}` (
  `itemratingid` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(100) DEFAULT NULL,
  `userid` int(11) DEFAULT NULL,
  `itemid` int(11) DEFAULT NULL,
  `rating` float DEFAULT NULL,
  `comment` text,
  PRIMARY KEY (`itemratingid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".execute.apply()
  }

}