package bgr.importer.bgg

import scalikejdbc._

object Importer {

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg20160518", "root", "root")
  implicit val session = AutoSession

  def main(args: Array[String]) {
    //    createTables
    //    Crawler.crawlItems  // 16:55 - 21:10 (batchsize 20)
    //    Crawler.recrawlFailures
    //    Parser.parseItems  // 1 minute
    //    Crawler.crawlRatings  // 22:15 - 7:00 (pages 24+, batchsize 2), 7:05 - 10:15 (pages 23-, batchsize 8)
    //    Crawler.recrawlFailures
    //    Parser.parseItemRatings  // 47 min
    //    println(new java.util.Date())
    filterForPopularity(100, 10) // 6 min
  }

  def filterForPopularity(ratingsCount: Int, timesVoted: Int) = {
    val tableName = s"itemrating_${ratingsCount}_${timesVoted}"
    val tableNameSQL = SQLSyntax.createUnsafely(tableName)
    createTableForRatings(tableName)
    sql"""INSERT INTO ${tableNameSQL}
SELECT itemrating.*
FROM itemrating INNER JOIN item ON itemrating.itemid = item.itemid
WHERE item.ratingscount >= ${ratingsCount} AND itemrating.userId IN
 (SELECT userId FROM itemrating GROUP BY userId HAVING COUNT(userId) >= ${timesVoted})
GROUP BY itemrating.userid, itemrating.itemid, itemrating.rating;""".execute.apply() // TODO usersRated instead of ratingsCount
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