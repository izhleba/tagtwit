import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

trait StreamTaskUtil {

  def taskName: String

  protected def createContext(flags: BaseFlags): SparkContext = {
    val config = makeConf(flags.masterOpt)
    // Fault tolerant part.
    //    flags.checkpointDirOpt.foreach { _ =>
    //      config.set("spark.streaming.receiver.writeAheadLog.enable", "true")
    //    }
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    sc
  }

  protected def makeConf(masterOpt: Option[String]): SparkConf = {
    val base = new SparkConf().setAppName(taskName)
    masterOpt match {
      case Some(masterName) => base.setMaster(masterName)
      case None => base
    }
  }

  protected def createStreamContext(sc: SparkContext, flags: BaseFlags)(): StreamingContext = {
    def create(): StreamingContext = {
      val ssc = new StreamingContext(sc, flags.frequency)
      flags.checkpointDirOpt.foreach { checkpointDir =>
        ssc.checkpoint(checkpointDir)
      }
      ssc
    }

    flags.checkpointDirOpt match {
      case Some(checkpointDir) => StreamingContext.getOrCreate(checkpointDir, create)
      case None => create()
    }
  }

}

trait BaseFlags {

  def masterOpt: Option[String]

  def frequency: Duration

  def checkpointDirOpt: Option[String]

}