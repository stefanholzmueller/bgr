package bgr.importer.bgg

import scalikejdbc._

case class Item(itemId: Int, itemType: String, itemName: String, yearpublished: String, image: String, thumbnail: String, ratingsCount: Int)
case class ItemRating(userName: String, itemId: Int, rating: Double)

object Parser {

//  Class.forName("com.mysql.jdbc.Driver")
//  ConnectionPool.singleton("jdbc:mysql://localhost:3306/bgg20160516", "root", "root")
  implicit val session = AutoSession

  def parseItemRatings { // took 1 hour
    sql"delete from itemrating".execute.apply()

    val userNameToUserId = scala.collection.mutable.Map[String, Int]()
    var runningUserId = 0

    val ids = sql"SELECT id FROM raw".map(_.get("id"):Int).list.apply
    ids.foreach { id =>
    val xmls = sql"select body from raw where status = '200' AND id = $id".fetchSize(1).foreach { row =>
      println(s"id=$id  runningUserId=$runningUserId")
      val xml = row.string("body")
      val xmlElem = scala.xml.XML.loadString(fixXml(xml))
      val items = (xmlElem \ "item").filter { item =>
        (item \ "@type").text == "boardgame" 
      }  // necessary?
      items.foreach { item =>
        val itemId = (item \ "@id").text.toInt
        val itemRatings = (item \ "comments" \ "comment").map { commentNode =>
          val userName = (commentNode \ "@username").text
          val rating = (commentNode \ "@rating").text.toDouble
          val comment = (commentNode \ "@value").text

          val userId = userNameToUserId.getOrElseUpdate(userName, {runningUserId = runningUserId + 1; runningUserId})

          sql"""INSERT INTO itemrating (userName, userId, itemId, rating, comment)
                VALUES ($userName, $userId, $itemId, $rating, $comment)""".update.apply
        }
      }
    }
    }
  }

  def parseItemRatingXml(itemId: Int, xml: String) = {
    val xmlElem = scala.xml.XML.loadString(xml)
    val itemRatings = (xmlElem \ "item" \ "comments" \ "comment").map { commentNode =>
      val userName = (commentNode \ "@username").toString
      val rating = (commentNode \ "@rating").toString.toDouble
      ItemRating(userName, itemId, rating)
    }
  }

  def parseItems {
    sql"delete from item".execute.apply()

    val xmls = sql"select body from raw where status = '200'".foreach { row =>
      val items = parseItemXml(row.string("body"))
      items foreach {
        case Item(itemId, itemType, itemName, yearpublished, image, thumbnail, ratingsCount) =>
          sql"""insert into item (itemId, type, name, yearpublished, image, thumbnail, ratingsCount) 
                values (${itemId}, ${itemType}, ${itemName}, ${yearpublished}, ${image}, ${thumbnail}, ${ratingsCount})""".update.apply()
      }
    }
  }

  def parseItemXml(xml: String): Seq[Item] = {
    try {
      val xmlElem = scala.xml.XML.loadString(fixXml(xml))
      val items = (xmlElem \ "item").filterNot { item =>
        val itemType = (item \ "@type").text
        itemType == "thing" /* item 50968 */ || itemType == "videogamehardware" /* item 65196 */
      }.map { item =>
        val itemId = (item \ "@id").text.toInt
        val itemType = (item \ "@type").text
        val itemName = ((item \ "name").head \ "@value").text // just pick the first
        val yearpublished = (item \ "yearpublished" \ "@value").text
        val image = (item \ "image").text
        val thumbnail = (item \ "thumbnail").text
        val totalitems = (item \ "comments" \ "@totalitems").text
        val ratingsCount = if (totalitems.isEmpty) 0 else totalitems.toInt
        Item(itemId, itemType, itemName, yearpublished, image, thumbnail, ratingsCount)
      }
      items
    } catch {
      case e: Exception => throw new RuntimeException(xml, e)
    }
  }

  def fixXml(xml: String): String = removeNonPrintableCharacters(removeStrangeLeadingDots(xml))

  def removeNonPrintableCharacters(string: String) = string.replaceAll("\\p{C}", "")

  def removeStrangeLeadingDots(string: String) = string.replaceFirst("^\\.+", "")

}