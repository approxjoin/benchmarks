import breeze.util.BloomFilter
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection

object ApproxJoinSampling {

  def randomList(sizex: Long, sizey: Long, fraction: Double): BloomFilter[(Long, Long)] = {
    val rnd = new scala.util.Random
    val total: Long = sizex*sizey
    val size: Long = (fraction*total).toLong
    val bf = BloomFilter.optimallySized[(Long, Long)](size, 0.01)
    //val bf2 = BloomFilter.optimallySized[Long](size, 0.01)
    var i = 0
    while (i < size) {
      val a = (rnd.nextDouble()*total).toLong
      val x = a%sizey
      val y = a/sizey
      //bf1 += x
      //bf2 += y
      bf += (x, y)
      i += 1
    }
    //(bf1, bf2)
    bf
  }

  def randomList(spark: SparkContext, numPartitions: Int, sizex: Long, sizey: Long, fraction: Double): (BloomFilter[Long], BloomFilter[Long]) = {
    val rnd = new scala.util.Random
    val total: Long = sizex*sizey
    val size: Long = (fraction*total).toLong
    val recordsPerPartition = size/numPartitions
    val distData = spark.parallelize(Seq[Long](), numPartitions).mapPartitions { _ => {
      (1 to recordsPerPartition.toInt).map{_ => (rnd.nextDouble()*total).toLong}.iterator
    }}

    val bf1: BloomFilter[Long] = distData.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[Long](size, 0.01)
      iter.foreach(i => bf += (i%sizey).toLong)
      Iterator(bf)
    }.reduce(_ | _) 
  
    val bf2: BloomFilter[Long] = distData.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[Long](size, 0.01)
      iter.foreach(i => bf += (i/sizey).toLong)
      Iterator(bf)
    }.reduce(_ | _)

    (bf1, bf2)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Approx Sampling Join")
    val sc = new SparkContext(conf)

    if (args.length < 3) {
      System.err.println("Usage: ApproxJoinSampling <input1> <input2> " +
        "<sampling fraction> ")
      System.exit(1)
    }
   
    /* Get input parameters */
    val Array(input1, input2, sampling_fraction) = args
    val fraction: Double = sampling_fraction.toDouble
    val fractions = scala.collection.mutable.HashMap[String, Double]()

    /* Hardcoded fractions for micro-benchmark */
    for (i <- 0 until 72) {
      val key = "A9" + i.toString
      fractions(key) = fraction
    }
    val num_partition = 144

    /* Loading input data */
    val textFile1 = sc.textFile("hdfs://stream10:9000/" + input1)
    val textFile2 = sc.textFile("hdfs://stream10:9000/" + input2)
    val input1 = textFile1.flatMap(line => line.split("\n")).map(line => (line.split(",")(0), line.split(",")(1))).repartition(num_partition)
    val input2 = textFile2.flatMap(line => line.split("\n")).map(line => (line.split(",")(0), line.split(",")(1))).repartition(num_partition)

    /* Building bloom filters */
    val bf1: BloomFilter[String] = input1.mapPartitions { iter =>
      val bf = BloomFilter.optimallySized[String](100000, 0.01)
      iter.foreach(i => bf += i._1)
      Iterator(bf)
    }.reduce(_ | _)

    val bf2: BloomFilter[String] = input2.mapPartitions { iter =>
      val bf1 = BloomFilter.optimallySized[String](100000, 0.01)
      iter.foreach(i => bf1 += i._1)
      Iterator(bf1)
    }.reduce(_ | _)

    val mergebf = bf1 & bf2

    /* Filtering joined items */
    val join1 = input1.filter(x => mergebf.contains(x._1))
    val join2 = input2.filter(x => mergebf.contains(x._1))

    /** Sampling after filtering **/
    /* Taking a sample of a list */
    val sizex = (join1.count()/num_partition)*2
    val sizey = (join2.count()/num_partition)*2
    //println("Size X, Size Y: " ,sizex, sizey) 
    val randList = randomList(sizex, sizey, fraction)
    //val randList = randomList(sc, num_partition, sizex, sizey, fraction) 

    /* Building index */
    val join1Index = join1.groupByKey().flatMap(x => {
      //val randList = randomList(sizex, sizey, fraction)
      for (value <- x._2.zipWithIndex) //; if (randList._1.contains(value._2.toLong)))
        yield (x._1, value)
    })

    val join2Index = join2.groupByKey().flatMap(x => {
      //val randList = randomList(sizex, sizey, fraction)
      for (value <- x._2.zipWithIndex) // ; if (randList._2.contains(value._2.toLong)))
        yield (x._1, value)
    })

    /* Performing join */
    val joinResult = join1Index.cogroup(join2Index).flatMapValues(pair => {
      val randList = randomList(sizex, sizey, fraction)
      for (v <- pair._1.iterator; w <- pair._2.iterator; if (randList.contains((v._2, w._2))))
          yield (v._1, w._1)
    })

    println("#items in bloom filter and sampling join:" + joinResult.count())
  }
}

