import java.nio.file.{Files, Paths}

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils

import scala.util.{Failure, Success, Try}

object DataScienceTwitterTagStreamCounter extends StreamTaskUtil {

  val TAG_SPLIT_CHAR = ';'
  val OUT_SPLIT_CHAR = ';'

  def taskName = "data_science_twitter_tag_stream_counter"


  def main(args: Array[String]) {
    val flags = DataScienceTwitterTagFlags(args)
    // Настройки для авторизации в twitter
    TwitterAuth.auth(flags.authFile)
    // Контексты
    val sc = createContext(flags)
    val ssc = createStreamContext(sc, flags)

    // Читаем тэги по которым будем фильтровать
    val appropriateTags: RDD[(String, Int)] = sc.textFile(flags.tagFile).zipWithIndex().map { case (line, lineNumber) =>
      parseLine(line, lineNumber)
    }

    // Доставляем множество для фильтрации каждому воркеру
    val tagSetBC = sc.broadcast(appropriateTags.map(_._1).collect().toSet)

    // Стриминг постов из twitter
    val stream = TwitterUtils.createStream(ssc, None)

    // Извлекаем тэги сообщений, берем только те что относятся к нашей теме
    val mlTagStream: DStream[String] = stream.flatMap { status =>
      val currentTags = status.getHashtagEntities.map(_.getText)
      currentTags.filter(tagSetBC.value.contains)
    }

    // Подсчитываем количество тэгов
    val tagCountStream: DStream[(String, Long)] = mlTagStream.countByValue()

    // Аккумулируем количество тэгов
    val tagCountState: DStream[(String, Long)] = tagCountStream.updateStateByKey(sumAmountOfKeys)
    tagCountState.checkpoint(Minutes(1))

    // Сохраняем в директорию с результатами
    val dataSaver = saveDataToDir(flags.outDir) _
    tagCountState.foreachRDD(dataSaver)

//    TODO DEBUG ONLY
    println(s"Filter size: ${appropriateTags.collect().length}")
    stream.foreachRDD{rdd =>
      val status= rdd.first()
      println(status.getText)
    }

//

    ssc.start()
    ssc.awaitTermination()
  }

  private def sumAmountOfKeys(newValues: Seq[Long], currentCount: Option[Long]): Option[Long] = {
    Some(currentCount.getOrElse(0l) + newValues.sum)
  }

  private def parseLine(line: String, lineNumber: Long): (String, Int) = {
    val strValues = line.split(s"$TAG_SPLIT_CHAR")
    if (strValues.length < 2) {
      throw new RuntimeException(s"""Not enough values after split in line($lineNumber):"$line" """)
    } else {
      val name = strValues(0)
      Try(strValues(1).toInt) match {
        case Failure(ex) =>
          throw new RuntimeException(s"""Cant parse second integer value in line($lineNumber):"$line" """, ex)
        case Success(value) => (name, value)
      }
    }
  }

  private def saveDataToDir(dirPath: String)(pairs: RDD[(String, Long)], time: Time): Unit = {
    val finalFilePath = Paths.get(dirPath).resolve(s"$taskName-${time.milliseconds}")

    //TODO DEBUG ONLY REMOVE IT!
    val words = pairs.collect().foldLeft(0l){case (acc,(_,count)) => count + acc}
    println(s"Saved words sum $words. ${time.milliseconds}")
    //

    pairs.saveAsTextFile(finalFilePath.toString)
  }

}