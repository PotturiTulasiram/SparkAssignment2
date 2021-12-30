import org.apache.spark._
import org.apache.log4j._

object Solution05 extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Solution05")

  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val reqfield = lines.flatMap(x => x.split(","))

  val filteredRDD = reqfield.filter(x => x.contains("http"))

  val rdd = filteredRDD.map(x => x.slice(1,13))

  val mapRDD = rdd.map(x => (x,1)).reduceByKey((x,y) => x+y).sortBy(x => x._2,false).take(5).foreach(println)

}
