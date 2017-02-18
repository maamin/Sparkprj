package org.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql
import org.apache.spark.sql.SQLContext


object WordCount {
  def main(args:Array[String])={
    val conf=new SparkConf()
    .setAppName("WordCount")
   /*.setMaster("local")*/
    val sc = new SparkContext(conf)
    
    case class call(phone_number: String, amount: String)
    val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
   val df= sqlContext.sql("SELECT phone_number, amount FROM xademo.call_detail_records").toDF()
   
        df.rdd.saveAsTextFile("/tmp/" + args(1))
        val test=sc.textFile("/tmp/word.txt")
    test.flatMap  { line =>
      line.split(" ")
      }
    .map { word =>
      (word,1) 
      }
    
    .reduceByKey (_ + _)
    .saveAsTextFile("/tmp/"+args(0))     
   /* 
  sqlContext.sql("SELECT * FROM xademo.tesst2")
  .toDF()
  .map{p=>(p(0),p.getDouble(1))}
  .reduceByKey( _ + _ )
  .saveAsTextFile("/tmp/"+args(2))
  */
    
  }
}