import org.apache.spark._
import org.apache.log4j._


object Solution08 extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]","Solution05")

  val lines = sc.textFile("src/main/resources/ghtorrent-logs.txt")

  val reqfield = lines.flatMap(x => x.split(","))
    .filter(x => x.contains("ghtorrent.rb: Repo") && x.contains("exists"))
    .map(x => (x.substring(x.indexOf("Repo")+5,x.indexOf("exists")-1),1))
    .reduceByKey((x,y) => x+y).sortBy(x => x._2,false).take(10)
    .foreach(println)

}
