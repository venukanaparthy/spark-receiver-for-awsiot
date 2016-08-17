package com.esri.realtime.transport.inbound.continuous.aws

import com.typesafe.config.ConfigFactory

import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds

object AWSEventHubClient {
  
  def main(args: Array[String]) {
    
    /*if (args.length < 3){      
      println("Usage AWSEventHubClient")
      System.exit(1)
      return
    }*/    
    
    val execMode = "local[2]" 
    val batchSize = 5 //args(1).toLong
    val storageLevel = StorageLevel.MEMORY_ONLY
    
    //config
    val config = ConfigFactory.load()
    
    //aws params
    val clientEndPoint = config.getString("awsiot.endpoint")
    val certificateFile = config.getString("awsiot.certificate")
    val privateKeyFile = config.getString("awsiot.privateKey") 
    val topicName = config.getString("awsiot.topicName")
   
    // key-store, password pair
    //val ksp = SslUtil.generateFromFilePath(certificateFile, privateKeyFile)    
    //println(ksp.keyPass)
    
    val sparkConf = new SparkConf().setAppName("AwsEventHub Streaming Client")
                                     .setMaster(execMode)
                                     .set("spark.executor.memory", "2g")  
                                     .set("spark.driver.memory", "2g")  
    val ssc = new StreamingContext(sparkConf, Seconds(batchSize))
    
    val awsIoTStream = AwsEventHubUtils.createStream(ssc, clientEndPoint, certificateFile, privateKeyFile, topicName, storageLevel)
    
    awsIoTStream.print()
         
    ssc.start()     
    ssc.awaitTermination()
                                             
  }  
}