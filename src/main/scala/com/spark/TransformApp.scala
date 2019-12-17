package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object TransformApp {


  def main(args: Array[String]): Unit = {
    val blackList=List("zs","ls")

    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val blackRdd=ssc.sparkContext.parallelize(blackList).map((_,true))

    blackRdd.foreachPartition({
      x=> x.foreach( {
        y=> print("black print.........")
          println("y._1:"+y._1)
          println("y._2:"+y._2)
      } )
    })

    val lines=ssc.socketTextStream("yqdata000",6789)

    val clicklog=lines.map(x =>(x.split(",")(1),x)).transform(rdd=>
      rdd.leftOuterJoin(blackRdd).filter(x=>x._2._2.getOrElse(false)!=true)
      .map(x=>x._2._1)
    )

    clicklog.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
