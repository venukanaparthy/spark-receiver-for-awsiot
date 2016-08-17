package com.esri.realtime.transport.inbound.continuous.aws

/*
 *   AwsEventHubInputDStream
 * 
 */
import java.math.BigInteger
import scala.annotation.meta.param
import java.security.SecureRandom
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import com.amazonaws.services.iot.client.AWSIotMqttClient
import com.amazonaws.services.iot.client.AWSIotMessage;
import com.amazonaws.services.iot.client.AWSIotQos;
import com.amazonaws.services.iot.client.AWSIotTopic;


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
  private var awsIotClient: AWSIotMqttClient = _
    
  override def onStart(): Unit = {
     setUp()
  }
  override def onStop(): Unit = {
    cleanUp()
  }
  
  def setUp(): Unit = {
   
     try {
          // key-store, password pair
         val ksp = SslUtil.generateFromFilePath(certificateFile, privateKeyFile)
         
         //create aws iot client connection
         val awsClientId = "clientId-%s".format(new BigInteger(128, new SecureRandom()).toString(32))
         awsIotClient = new AWSIotMqttClient(clientEndPoint, awsClientId, ksp.keyStore, ksp.keyPass)  
         
         //connect to Aws IoT Hub
         awsIotClient.connect()
         println("=?Connected to AwsIoT Hub")
         
         // subscribe to a topic, register listener, call store on receiving the message
         awsIotClient.subscribe(new AWSIotTopic(topicName, AWSIotQos.QOS0){
             override def onMessage(message: AWSIotMessage): Unit = {
                 store(message.getStringPayload())
             }      
           }, true);         
         println("=>Subscribed to topic: %s".format(topicName))

         
       }catch{
         case e: Exception =>
           restart("Error occured while connecting. Restaring", e)
       }
   }
   
  def cleanUp(): Unit = {
    if (awsIotClient !=null) {
      awsIotClient.disconnect()
      println("Disconnected from client")
      awsIotClient = null      
    }
  }
  
}
