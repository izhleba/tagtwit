import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths, StandardOpenOption}

import twitter4j._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object TwitterTagCollector {

  val acceptTagCountBorder = 3

  val limit = 20000

  val SPLIT_CHAR = ';'

  val SOURCES_SET = Set(
    //      hubs
    "kaggle", "deeplearningldn", "odsc", "ds_ldn", "DataScienceDojo", "data_wizard", "datascience_ru",
    "big_data_flow", "Deep_Hub", "kdnuggets"
    //      persons
//    "hmason", "johnmyleswhite", "peteskomoroch", "DataJunkie", "dpatil", "bigdata", "ogrisel", "revodavid"
  )

  def main(args: Array[String]) {
    if(args.length < 2){
      sys.error("Not enough arguments.")
    }
    TwitterAuth.auth(args(0))
    val tagFilePath = Paths.get(args(1))

    val twitter: Twitter = new TwitterFactory().getInstance()
    val twitterUtil = new TwitterTagCollector(twitter)
    val sources = SOURCES_SET.toSeq
    val totalSize = sources.size
    val map = sources.zipWithIndex.foldLeft(new mutable.HashMap[String, Int]) { case (accMap, (userName, index)) =>
      println(s"(${index+1}\\$totalSize)\t$userName")
      val tags = tagsFromUser(twitterUtil)(userName)
      tags.foreach { tag =>
        accMap.get(tag) match {
          case Some(count) => accMap.update(tag, count + 1)
          case None => accMap.put(tag, 1)
        }
      }
      accMap
    }

    val writer = Files.newBufferedWriter(tagFilePath, StandardCharsets.UTF_8,
      StandardOpenOption.WRITE, StandardOpenOption.CREATE)
    val tuples = map.toBuffer
    try {
      tuples.filter(_._2 >= acceptTagCountBorder).sortBy(x => x._2).foreach { case (tag, count) =>
        writer.write(s"$tag$SPLIT_CHAR$count\n")
      }
    } finally {
      writer.close()
    }
  }

  def extractTag(status: Status): Iterable[String] = {
    status.getHashtagEntities.map(_.getText)
  }

  def tagsFromUser(twitterUtil: TwitterTagCollector)(user: String): Iterable[String] = {
    twitterUtil.applyToTimeline(user, extractTag, limit) match {
      case Failure(exception) =>
        exception.printStackTrace()
        println("Failed to get timeline: " + exception.getMessage())
        Iterable.empty
      case Success(hashTags) => hashTags
    }
  }

}

class TwitterTagCollector(twitter: Twitter) {

  def applyToTimeline[T](user: String, fn: Status => Iterable[T], limit: Int = Int.MaxValue): Try[Iterable[T]] = {
    Try {
      val statusesBuilder = ArrayBuffer.newBuilder[T]
      var pageNum = 1
      var currentSize = 0
      var total = 0
      do {
        val page = new Paging(pageNum, 100)
        pageNum = pageNum + 1
        val statusesPage = twitter.getUserTimeline(user, page)
        currentSize = statusesPage.size()
        total += currentSize
        statusesBuilder ++= statusesPage.asScala.flatMap(fn)
      } while (currentSize > 0 && total <= limit)
      statusesBuilder.result()
    }
  }

}

