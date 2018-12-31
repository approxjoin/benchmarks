import breeze.util.BloomFilter
import org.apache.spark.{SparkConf, SparkContext}

object HashJoin {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Hash Join")
    val sc = new SparkContext(conf)

    if (args.length < 4) {
      System.err.println("Usage: HashJoin <master> <input1> <input2>" +
        "<number of cores>")
      System.exit(1)
    }

    /* Getting input parameters */
    val Array(master, input1, input2, cores) = args
    val num_cores: Int = cores.toInt
    val num_partition = num_cores*2

    /* Loading input data */
    val textFile1 = sc.textFile("hdfs://" + master + ":9000/" + input1)
    val textFile2 = sc.textFile("hdfs://" + master + ":9000/" + input2) 
    val input1 = textFile1.flatMap(line => line.split("\n")).map(line => (line.split(",")(0), line.split(",")(1))).repartition(num_partition)
    val input2 = textFile2.flatMap(line => line.split("\n")).map(line => (line.split(",")(0), line.split(",")(1))).repartition(num_partition)

    val joinResult = input1.join(input2) 
    println("#items in join:" + joinResult.count())
  }
}

