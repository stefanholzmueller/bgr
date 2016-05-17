package bgr.importer.bgg

import scalikejdbc._

object Importer {

  Class.forName("com.mysql.jdbc.Driver")
  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg20160518", "root", "root")
  implicit val session = AutoSession

  def main(args: Array[String]) {
    createTables
    Crawler.crawlItems  // took 7 hours
    Crawler.recrawlFailures
//    Parser.parseItems  // a few minutes
//    Crawler.crawlRatings  // 6 hours, with many failures
//    Crawler.recrawlFailures
//    Parser.parseItemRatings  // 1 hour?
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
  PRIMARY KEY (`itemid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".execute.apply()

    println("create table: itemrating")
    sql"""CREATE TABLE `itemrating` (
  `itemratingid` int(11) NOT NULL AUTO_INCREMENT,
  `username` varchar(100) DEFAULT NULL,
  `userId` int(11) DEFAULT NULL,
  `itemid` int(11) DEFAULT NULL,
  `rating` float DEFAULT NULL,
  `comment` text,
  PRIMARY KEY (`itemratingid`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8""".execute.apply()
  }

}