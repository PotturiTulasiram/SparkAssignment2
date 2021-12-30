import org.apache.spark._
import org.apache.log4j._


object Solution07 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Soluiton04")

  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val timeField =  lines.flatMap(x => x.split(","))
    .filter(x => x.contains("+00"))
    .map(x => (x.substring(0, 11), (x.substring(12,14), 1)))
    //.reduceByKey((x, y) => (x._1, x._2 + y._2))
    .foreach(println)
}
