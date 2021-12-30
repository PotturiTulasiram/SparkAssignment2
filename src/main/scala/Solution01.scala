
import org.apache.spark._
import org.apache.log4j._

object Solution01 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Solution01")
  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val count = lines.count()
  println(count)

}
