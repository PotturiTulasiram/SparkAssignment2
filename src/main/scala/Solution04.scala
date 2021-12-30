import org.apache.spark._
import org.apache.log4j._

object Solution04 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Soluiton04")

  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val reqData = lines.flatMap(x => x.split(" "))
    .filter(x => x == "api_client.rb:")
    .map(x => (x, 1))
    .reduceByKey((x, y) => x + y).foreach(println)


}
