import org.apache.spark._
import org.apache.log4j._

object Solution03 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Solution03")

  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val loginfo = lines.map(x => x.split(",")(0))
  val warnInfocount = loginfo.filter(x => x == "WARN").count()
  println(warnInfocount)

}
