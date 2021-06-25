import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    //获取集群入口
    val conf: SparkConf = new SparkConf()
    conf.setAppName("WordCount")
    conf.setMaster("local")
    val sc = new SparkContext(conf)
    //从 HDFS 中读取文件
    val lineRDD: RDD[String] = sc.textFile("/Users/wuziyue/Desktop/1.txt")
    //做数据处理
    val wordRDD: RDD[String] = lineRDD.flatMap(line=>line.split("\\s+"))
    val wordAndCountRDD: RDD[(String, Int)] = wordRDD.map(word=>(word,1))
    //将结果写入到 HDFS 中
    wordAndCountRDD.reduceByKey(_+_).saveAsTextFile("/Users/wuziyue/Desktop/output")
    //关闭编程入口
    sc.stop()
  }
}
