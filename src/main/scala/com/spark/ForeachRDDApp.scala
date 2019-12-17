package com.spark

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDDApp {

  def main(args: Array[String]): Unit = {

    val sparkConf=new SparkConf().setAppName("").setMaster("local[2]")

    val ssc=new StreamingContext(sparkConf,Seconds(5))

    val lines=ssc.socketTextStream("yqdata000",6789)

    ssc.checkpoint("./checkpointData")

    val result=lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)


    result.print()
    result.foreachRDD({rdd =>
      rdd.foreachPartition({partitionOfRecords => {
            val conn=createConnection()
              partitionOfRecords.foreach({ record =>
              println("*****************************insert*****************************"+record._1+","+record._2)
              val selectSql="select count(1) from wordcount where word='"+record._1+"'";
              val rs=conn.createStatement().executeQuery(selectSql)
              rs.next()
              val num=rs.getInt(1)
               var updateSql=""
              if(num > 0){
                updateSql="update wordcount set wordcount="+record._2 +" where word='"+record._1+"'"
              }else{
                updateSql="insert into wordcount(word,wordcount) values('"+record._1+"',"+record._2+")"
              }
              conn.createStatement().executeUpdate(updateSql)
            })
            conn.close()
        }
      })
    })


    ssc.start()
    ssc.awaitTermination()
  }

  def updateFuction(currentValues:Seq[Int],runningCount:Option[Int]):Option[Int] ={
    val current=currentValues.sum
    val pre=runningCount.getOrElse(0)

    Some(current+pre)
  }

  def createConnection()={
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/srping","root","123456")
  }
}
