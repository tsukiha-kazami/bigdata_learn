import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object Demo1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("json").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val rdd1: RDD[String] = sc.textFile(this.getClass().getClassLoader.getResource("word.json").getPath)
    val rdd2: RDD[Option[Any]] = rdd1.map(JSON.parseFull(_))
    rdd2.foreach(println)
  }
}