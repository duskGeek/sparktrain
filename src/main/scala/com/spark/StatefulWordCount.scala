package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StatefulWordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setAppName("").setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val lines=ssc.socketTextStream("yqdata000",6789)

    ssc.checkpoint("./checkpointData")

    val result=lines.flatMap(_.split(" ")).map((_,1))
    val state=result.updateStateByKey[Int](updateFuction _)

    state.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def updateFuction(currentValues:Seq[Int],runningCount:Option[Int]):Option[Int] ={
    val current=currentValues.sum
    val pre=runningCount.getOrElse(0)

    Some(current+pre)
  }
}
