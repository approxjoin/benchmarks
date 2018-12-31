import breeze.util.BloomFilter
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ApproxJoinFiltering {

  def bloomFilterBuild[K, V](input: RDD[(K, V)], size: Int, false_rate: Double): BloomFilter[K] = input.mapPartitions { iter =>
    val bf = BloomFilter.optimallySized[K](size, false_rate)
    iter.foreach(i => bf += i._1)
    Iterator(bf)
  }.reduce(_ | _)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Approximate Join w/ filtering stage")
    //conf.set("spark.eventLog.enabled","true")
    val sc = new SparkContext(conf)
    if (args.length < 4) {
      System.err.println("Usage: ApproxJoinFiltering <master> <input1> <input2>" +
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

    /* Building bloom filters */
    val bf1: BloomFilter[String] = bloomFilterBuild(input1, 100000, 0.01)
    val bf2: BloomFilter[String] = bloomFilterBuild(input2, 100000, 0.01)
    val mergebf = bf1 & bf2

    /* Join performance */
    println("Join result: ")
    val join1 = input1.filter(i => mergebf.contains(i._1))
    val join2 = input2.filter(i => mergebf.contains(i._1))
    val joinResult = join1.join(join2)
    println("#items in bloom filter join:" + joinResult.count())

  }
}
