import org.apache.spark.streaming.{Duration, Seconds}

case class DataScienceTwitterTagFlags(authFile: String,
                                      tagFile: String,
                                      outDir: String,
                                      masterOpt: Option[String],
                                      frequency: Duration,
                                      checkpointDirOpt: Option[String]) extends BaseFlags


object DataScienceTwitterTagFlags {
  val DEFAULT_SEC = 1l

  private val AUTH_PREFIX = "--auth:"
  private val MASTER_PREFIX = "--master:"
  private val TAG_FILE_PREFIX = "--tag_file:"
  private val OUT_DIR_PREFIX = "--out_dir:"
  private val FREQUENCY_SECOND_PREFIX = "--freq_sec:"
  private val CHECKPOINT_DIRECTORY_PREFIX = "--checkpoint_dir:"

  private val MANUAL =
    s"""
       |DataScienceTwitterTagStreamCounter
       |Arguments:
       |  Required:
       |  --auth:<path> Path to file with authentication properties for twitter.
       |  --tag_file:<path> Path to file with tags.
       |  --out_dir:<path> Output directory.
       |  Optional:
       |  --master:<master> In local mode, <master> should be 'local[n]' with n > 1.
       |  --freq_sec:<second> Frequency of streaming in seconds. Default $DEFAULT_SEC sec.
       |  --checkpoint_dir:<path> Fault tolerant enabled if specified path for checkpoints. (Experimental!)
       |  """.stripMargin

  def apply(args: Array[String]): DataScienceTwitterTagFlags = {
    try {
      DataScienceTwitterTagFlags(
        authFile = findFlag(args, AUTH_PREFIX).get,
        tagFile = findFlag(args, TAG_FILE_PREFIX).get,
        outDir = findFlag(args, OUT_DIR_PREFIX).get,
        masterOpt = findFlag(args, MASTER_PREFIX),
        frequency = Seconds(findFlag(args, FREQUENCY_SECOND_PREFIX).map(_.toLong).getOrElse(DEFAULT_SEC)),
        checkpointDirOpt = findFlag(args, CHECKPOINT_DIRECTORY_PREFIX)
      )
    } catch {
      case _: Throwable =>
        System.err.println("Your arguments were " + args.mkString("[", ", ", "]"))
        System.err.println(MANUAL)
        sys.exit(1)
    }
  }

  protected def findFlag(args: Array[String], prefix: String): Option[String] = {
    args.find(_.startsWith(prefix)).map(s => s.substring(prefix.size))
  }
}
