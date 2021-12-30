import org.apache.spark._
import org.apache.log4j._


object Solution06 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Soluiton04")

  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val reqfield = lines.flatMap(x => x.split(","))

  val filteredRDD = reqfield.filter(x => x.contains("Failed request"))

  val rdd = filteredRDD.map(x => x.slice(1,13))

  val mapRDD = rdd.map(x => (x,1)).reduceByKey((x,y) => x+y).sortBy(x => x._2,false).take(10).foreach(println)
}
