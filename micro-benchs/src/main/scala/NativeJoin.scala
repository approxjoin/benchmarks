import breeze.util.BloomFilter
import org.apache.spark.{SparkConf, SparkContext}

object NativeJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Native Join")
    //conf.set("spark.eventLog.enabled","true")
    val sc = new SparkContext(conf)

    if (args.length < 4) {
      System.err.println("Usage: HashJoin <master> <input1> <input2>")
      System.exit(1)
    }

    /* Getting input parameters */
    val Array(master, input1, input2) = args

    /* Loading input data */
    val textFile1 = sc.textFile("hdfs://" + master + ":9000/" + input1)
    val textFile2 = sc.textFile("hdfs://" + master + ":9000/" + input2)
    val input1 = textFile1.flatMap(line => line.split("\n")).map(line => (line.split(",")(0), line.split(",")(1)))
    val input2 = textFile2.flatMap(line => line.split("\n")).map(line => (line.split(",")(0), line.split(",")(1)))

    val joinResult = input1.join(input2)
    println("#items in join:" + joinResult.count())
  }
}

