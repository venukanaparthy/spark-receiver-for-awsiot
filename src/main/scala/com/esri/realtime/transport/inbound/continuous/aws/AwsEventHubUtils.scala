package com.esri.realtime.transport.inbound.continuous.aws

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import scala.reflect.ClassTag
/*
 * 
 * AwsEventHubUtils
 */
object AwsEventHubUtils {
  /**
   * Create an input stream that receives messages published by Aws Event Hub.
   * @param ssc           StreamingContext object
   * @param clientEndPoint     Aws Endpoint
   * @param certificateFile
   * @param privateKeyFile
   * @param topicName
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStream(
                    ssc: StreamingContext,
                    clientEndPoint: String,
                    certificateFile: String,
                    privateKeyFile: String,
                    topicName: String,                     
                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
                    ): ReceiverInputDStream[String] = {
    new AwsEventHubInputDStream(ssc, clientEndPoint, certificateFile, privateKeyFile, topicName, storageLevel)
  }
  
  /**
   * Create an input stream that receives messages pushed by a Aws Event Hub.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param clientEndPoint     Aws Endpoint
   * @param certificateFile
   * @param privateKeyFile
   * @param topicName
   */
  def createStream(
                    jssc: JavaStreamingContext,
                    clientEndPoint: String,
                    certificateFile: String,
                    privateKeyFile: String,
                    topicName: String                               
                    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, clientEndPoint, certificateFile, privateKeyFile, topicName)
  }
  
  /**
   * Create an input stream that receives messages pushed by a Aws Event Hub.
   * @param jssc      JavaStreamingContext object
	 * @param clientEndPoint     Aws Endpoint
   * @param certificateFile
   * @param privateKeyFile
   * @param topicName  
   * @param storageLevel  RDD storage level.
   */
  def createStream(
                    jssc: JavaStreamingContext,
                    clientEndPoint: String,
                    certificateFile: String,
                    privateKeyFile: String,
                    topicName: String,              
                    storageLevel: StorageLevel
                    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, clientEndPoint, certificateFile, privateKeyFile, topicName, storageLevel)
  }
}