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
        Receiver [String](storageLevel)  {
  
  // AwsIoT client connection                
  private var awsIotClient: AWSIotMqttClient = _
    
  override def onStart(): Unit = {
      new Thread("Aws IoT Receiver") {
        override def run() {
          receiveAwsIoT()
        }
      }.start()     
  }
  
  override def onStop(): Unit = {
    cleanUp()
  }
  
  /*
   *  Creates connection to Aws IoT Hub and 
   *  receives messages from a topic   
   */
  private def receiveAwsIoT(): Unit = {
   
     try {
         // key-store, password pair
         val ksp = SslUtil.generateFromFilePath(certificateFile, privateKeyFile)
         
         //create aws iot client connection
         val awsClientId = "clientId-%s".format(new BigInteger(128, new SecureRandom()).toString(32))
         awsIotClient = new AWSIotMqttClient(clientEndPoint, awsClientId, ksp.keyStore, ksp.keyPass)  
         
         //connect to Aws IoT Hub
         awsIotClient.connect()
         println("=>Connected to AwsIoT Hub")
         
         // subscribe to a topic, register listener, call store on receiving the message
         awsIotClient.subscribe(new AWSIotTopic(topicName, AWSIotQos.QOS0){
             override def onMessage(message: AWSIotMessage): Unit = {
                 store(message.getStringPayload())
             }      
           }, true);         
         println("=>Subscribed to topic: %s".format(topicName))

         
       }catch{
         case t: Throwable  =>
           restart("Error receiving messages from Aws IoT Hub", t)
       }
   }
   
  /*
   *  Closes connection to Aws IoT Hub
   */
  private def cleanUp(): Unit = {
    try{
      if (awsIotClient !=null) {
        println("=>Disconnecting from Aws IoT Hub")
        awsIotClient.disconnect()
        println("=>Disconnected from Aws IoT Hub")            
      }
    }catch{
      case t: Throwable =>
        println("Error cleaning up : " + t.getMessage)
    }finally {
        awsIotClient = null
    }
  }
  
}
