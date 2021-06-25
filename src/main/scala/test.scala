
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}

/**
 * Created by tsun on 19/3/23.
 */
object test {

  def main(args: Array[String]) {
//    Logger.getLogger("org").setLevel(Level.ERROR)

    val sparkConf = new SparkConf().setAppName("spark api").setMaster("local")
    sparkConf.set("spark.testing.memory", "2147480000")
    val sparkContext = new SparkContext(sparkConf)

    val data = Array(1, 2, 3, 4, 5)
    val inputRdd = sparkContext.parallelize(data)

    inputRdd.collect().foreach(println(_))
    println(" the spark!!!!!!!!")


  }
}
