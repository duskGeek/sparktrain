package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object FlumePushWordCount {


  def main(args: Array[String]): Unit = {
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("FlumePushWordCount")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    import org.apache.spark.streaming.flume._
    FlumeUtils.createStream(ssc,"yqdata000",41414)

    ssc.start()
    ssc.awaitTermination()
  }
}
