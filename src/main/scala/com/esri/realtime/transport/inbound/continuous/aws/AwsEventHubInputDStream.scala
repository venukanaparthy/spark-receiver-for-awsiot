package com.esri.realtime.transport.inbound.continuous.aws

/*
 *   AwsEventHubInputDStream
 * 
 */

import scala.annotation.meta.param
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver

class AwsEventHubInputDStream(
                           @(transient @param) ssc_ : StreamingContext,
                           clientEndPoint: String,
                           certificateFile: String,
                           privateKeyFile: String,
                           topicName: String, 
                           storageLevel: StorageLevel) extends ReceiverInputDStream[String] (ssc_){
  override def getReceiver(): Receiver[String] = {
    new AwsEventHubReceiver(clientEndPoint, certificateFile, privateKeyFile, topicName, storageLevel)
  }   
}

class AwsEventHubReceiver (clientEndPoint: String,
                           certificateFile: String,
                           privateKeyFile: String,
                           topicName: String, 
                           storageLevel: StorageLevel) extends 
        Receiver [String](storageLevel)
                                         {
  override def onStart(): Unit = ???
  override def onStop(): Unit = ???  
  def cleanUp(): Unit = ???
  
}
