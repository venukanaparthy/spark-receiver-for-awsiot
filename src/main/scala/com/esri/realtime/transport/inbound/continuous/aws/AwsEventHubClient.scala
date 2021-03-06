package com.esri.realtime.transport.inbound.continuous.aws

import java.nio.charset.StandardCharsets
import java.math.BigInteger
import java.security.SecureRandom
import com.typesafe.config.ConfigFactory
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.Time
import com.amazonaws.services.iot.client.AWSIotMqttClient
import com.amazonaws.services.iot.client.AWSIotMessage
import com.amazonaws.services.iot.client.AWSIotQos
import java.util.Scanner
import org.apache.spark.streaming.Duration
import org.apache.spark.streaming.Duration

object AwsEventHubClient{
  
  private var batchSize:Long = 5
  private var eventsPerSecond = 10
  private var intervalInMillis = 1000
  private var bReceiver: Boolean = false  
  private val execMode = "local[2]"     
  private val storageLevel = StorageLevel.MEMORY_ONLY
  private var clientEndPoint:String = null
  private var topicName:String = null
  private var certificateFile:String = null
  private var privateKeyFile:String = null
  private var pair:KeyStorePasswordPair = null
  private var awsClient:AWSIotMqttClient = null
  private var scratchDir:String = null
  
  def main(args: Array[String]) {
    
    if (args.length < 2){      
      println("Usage: AWSEventHubClient:")
      println("Receiver  mode (receives Aws IoT messages)    : AWSEventHubClient <receiver> <batchSize>")
      println("Simulator mode (publishes messagesto Aws IoT) : AWSEventHubClient <simulator>  <eventsPerSec> <intervalInMillis>")
      
      System.exit(1)      
    }    
     
    if (args(0) == "receiver"){
      val Array(_ , batchSizeStr) = args
      batchSize = batchSizeStr.toLong
      bReceiver = true
    }
    else{
      val Array(_, eventsPerSecStr , intervalInMillisStr) = args
      eventsPerSecond = eventsPerSecStr.toInt
      intervalInMillis = intervalInMillisStr.toInt
    }
     
    try {
      
      //config
      val config = ConfigFactory.load()    
      //aws params
      clientEndPoint = config.getString("awsiot.endpoint")
      certificateFile = config.getString("awsiot.certificate")
      privateKeyFile = config.getString("awsiot.privateKey") 
      topicName = config.getString("awsiot.topicName")
      scratchDir  = config.getString("awsiot.scratchDir")
      
      if(!bReceiver) {     
        simulateEvents()       
        return
      }
      
      //spark conf
      val sparkConf = new SparkConf().setAppName("AwsEventHub Streaming Client")
                                       .setMaster(execMode)
                                       .set("spark.executor.memory", "4g")  
                                       .set("spark.driver.memory", "4g")
                                       //.set("spark.streaming.receiver.writeAheadLog.enable", "true") // reliable receiver on driver failure?
                                                                            
      val ssc = new StreamingContext(sparkConf, Seconds(batchSize))
      
      val awsIoTStream = AwsEventHubUtils.createStream(ssc, clientEndPoint, certificateFile, privateKeyFile, topicName, storageLevel)   
     
      //awsIoTStream.print()
      
      val idCountRDD = awsIoTStream.map{x => x.split(",")}.map{ y => (y(0), 1)}.reduceByKeyAndWindow((a:Int, b:Int) => (a + b) , Seconds(30), Seconds(5))
      idCountRDD.print()
      
      //val idCountRDD = awsIoTStream.map{x => x.split(",")}.map{ y => (y(0), 1)}.reduceByKey(_+_)
      //idCountRDD.saveAsTextFiles(scratchDir, "txt")
       
      ssc.checkpoint(scratchDir)
      ssc.start()     
      ssc.awaitTermination()
    
     }catch {
      case t:Throwable =>
        println("Error occured send events" + t.getMessage)
          
     }finally {
        if (awsClient !=null){
          println("Aws client disconnecting..")
          awsClient.disconnect()
          awsClient = null
        }
     }                                                   
  } 
  
  /*
   * Send events to Aws Iot Hub
   */
  def simulateEvents(): Unit = {
   
    //Connect to Aws Event Hub       
    pair = SslUtil.generateFromFilePath(certificateFile, privateKeyFile);
    val awsClientId = "clientId-%s".format(new BigInteger(128, new SecureRandom()).toString(32))
    println("Connecting to Aws IoT Hub")   
    awsClient = new AWSIotMqttClient(clientEndPoint, awsClientId , pair.keyStore, pair.keyPass)
    awsClient.connect()  
    println("Connected to Aws IoT Hub")   
    var ix = 0
    println("Writing " + eventsPerSecond + " e/s every " + intervalInMillis + " millis ...to topic: " + topicName)
    val doLoop = true    
    while(doLoop) {
        
      if (ix + eventsPerSecond > tracks.length)
        ix = 0
      for (jx <- ix to ix+eventsPerSecond-1) {
        val eventString = tracks(jx).replace("TIME", System.currentTimeMillis().toString)
        ix += 1
        //println(jx + ": " + eventString)
        val bytes = eventString.getBytes(StandardCharsets.UTF_8); 
        awsClient.publish(new AWSIotMessage(topicName, AWSIotQos.QOS0, bytes){
            override def onSuccess() : Unit = {
                println("%d : published success %s".format(System.currentTimeMillis(), getStringPayload))
            }
          
            override def onFailure() : Unit = {
                println("%d : publish failed  %s".format(System.currentTimeMillis(), getStringPayload))
            }
           
            override def onTimeout() : Unit = {
                 println("%d : publish timedout >>> %s".format(System.currentTimeMillis(), getStringPayload))
           }
	      })	            
      }
      println()
      Thread.sleep(intervalInMillis)
    }
  }
  
  //SUSPECT,TRACK_DATE,SENSOR,BATTERY_LEVEL,LATITUDE,LONGITUDE,DISTANCE_FT,DURATION_MIN,SPEED_MPH,COURSE_DEGREE
  val tracks = Array[String](
    "J7890,TIME,2,High,32.97903,-115.550378,78.63,0.87,1.03,123",
    "U89765,TIME,7,High,33.773321,-117.251617,116.91,0.82,1.63,99.6",
    "K90123,TIME,3,Low,33.564687,-117.137729,2632.43,1.03,28.95,194.7",
    "A12345,TIME,1,High,33.687425,-117.162813,75.44,1.08,0.79,190.2",
    "J7890,TIME,2,High,32.978986,-115.550098,87.35,1,0.99,98.9",
    "U89765,TIME,7,High,33.773397,-117.251692,35.84,1.02,0.4,315.4",
    "K90123,TIME,3,Low,33.553706,-117.140083,4059.74,1.15,40.12,192.1",
    "A12345,TIME,1,High,33.687407,-117.162832,8.74,1.55,0.06,226.5",
    "J7890,TIME,2,High,32.978951,-115.550327,71.37,1.05,0.77,261.3",
    "U89765,TIME,7,High,33.773359,-117.251649,19.03,1.18,0.18,131.5",
    "K90123,TIME,3,Low,33.547474,-117.142262,2362.96,1.32,20.39,199.3",
    "A12345,TIME,1,High,33.68739,-117.162778,17.55,0.47,0.43,107.5",
    "J7890,TIME,2,High,32.978976,-115.550315,9.81,1,0.11,25.6",
    "U89765,TIME,7,High,33.773347,-117.251684,11.5,0.97,0.14,251.1",
    "K90123,TIME,3,Low,33.542181,-117.143787,1981.34,0.75,30.02,196.1",
    "A12345,TIME,1,High,33.6873,-117.16281,34.17,1.02,0.38,199.6",
    "J7890,TIME,2,High,32.978944,-115.550502,58.52,1.02,0.65,260.3",
    "U89765,TIME,7,High,33.773456,-117.251926,83.56,1.02,0.93,294.2",
    "K90123,TIME,3,Low,33.541376,-117.144013,300.92,1.02,3.36,195.7",
    "A12345,TIME,1,High,33.6877,-117.162717,148.28,1.03,1.63,13.1",
    "J7890,TIME,2,High,32.97917,-115.550708,103.7,1.05,1.12,317.7",
    "U89765,TIME,7,High,33.773361,-117.251595,106.37,1.05,1.15,106",
    "K90123,TIME,3,Low,33.539838,-117.144424,573.51,1,6.52,195",
    "A12345,TIME,1,High,33.687665,-117.162751,16.41,1.02,0.18,224.2",
    "J7890,TIME,2,High,32.979187,-115.550589,37.01,1.15,0.37,81.9",
    "U89765,TIME,7,High,33.773308,-117.251747,50.06,1,0.57,250.8",
    "K90123,TIME,3,Low,33.539838,-117.144486,18.89,1.02,0.21,270",
    "A12345,TIME,1,High,33.687551,-117.162779,42.35,1.17,0.41,193.8",
    "J7890,TIME,2,High,32.97898,-115.550886,118.19,0.83,1.61,235.1",
    "U89765,TIME,7,High,33.773295,-117.2517,15.05,0.88,0.19,105.5",
    "K90123,TIME,3,Low,33.534181,-117.147999,2320.25,1.03,25.52,211.8",
    "A12345,TIME,1,High,33.687255,-117.163804,329.89,1.02,3.69,253.9",
    "J7890,TIME,2,High,32.979293,-115.548969,598.83,1.03,6.59,80.7",
    "U89765,TIME,7,High,33.773302,-117.251778,23.84,1.05,0.26,275.1",
    "K90123,TIME,3,Low,33.528969,-117.154261,2690.52,1.02,30.07,230.2",
    "A12345,TIME,1,High,33.687558,-117.162745,340.5,1.05,3.69,74",
    "J7890,TIME,2,High,32.978584,-115.548211,347.26,1.13,3.48,133.1",
    "U89765,TIME,7,High,33.773449,-117.251719,56.42,0.97,0.66,21.9",
    "K90123,TIME,3,Low,33.527032,-117.156959,1083.02,1,12.31,234.3",
    "A12345,TIME,1,High,33.687486,-117.162722,27.12,0.88,0.35,162.3",
    "J7890,TIME,2,High,32.978596,-115.544128,1252.18,0.87,16.42,89.8",
    "U89765,TIME,7,High,33.773311,-117.251724,50.24,1.02,0.56,182.1",
    "K90123,TIME,3,Low,33.525952,-117.157001,393.21,1.03,4.32,182.2",
    "A12345,TIME,1,High,33.687678,-117.162715,69.9,1.02,0.78,2.1",
    "J7890,TIME,2,High,32.978614,-115.54173,735.44,1.15,7.27,89.6",
    "U89765,TIME,7,High,33.773371,-117.251663,19.62,1.03,0.22,96.2",
    "K90123,TIME,3,Low,33.522118,-117.164024,1623.21,0.92,20.12,235.9",
    "A12345,TIME,1,High,33.687477,-117.16288,178.93,1.05,1.94,39.9",
    "J7890,TIME,2,High,32.830996,-115.569898,4560.62,0.98,52.7,183.9",
    "U89765,TIME,7,High,33.773297,-117.251692,28.33,1,0.32,201.4",
    "K90123,TIME,3,Low,33.510271,-117.155911,4969.9,1.2,47.06,145.6",
    "A12345,TIME,1,High,33.687912,-117.162607,178.76,0.92,2.22,32.1",
    "J7890,TIME,2,High,32.826066,-115.569941,1793.82,1.17,17.47,180.5",
    "U89765,TIME,7,High,33.773269,-117.251652,15.86,1.03,0.17,125",
    "K90123,TIME,3,Low,33.497456,-117.149322,5077.44,0.85,67.88,152.8",
    "A12345,TIME,1,High,33.687912,-117.162607,178.76,0.92,2.22,32.1",
    "J7890,TIME,2,High,32.822461,-115.570044,1312.06,0.85,17.54,181.6",
    "U89765,TIME,7,High,33.773266,-117.251669,5.28,1,0.06,260",
    "K90123,TIME,3,Low,33.482675,-117.141522,5880.94,1,66.83,152.2",
    "A12345,TIME,1,High,33.687415,-117.162873,198.13,1,2.25,208.2",
    "J7890,TIME,2,High,32.814976,-115.569918,2723.68,1.03,29.95,179",
    "U89765,TIME,7,High,33.773239,-117.251727,20.18,1.15,0.2,245",
    "K90123,TIME,3,Low,33.46474,-117.13659,6697.38,1.02,74.86,164.6",
    "A12345,TIME,1,High,33.692348,-117.124362,5082.82,1.08,53.32,88.2",
    "J7890,TIME,2,High,32.693358,-115.483001,19.49,0.85,0.26,100.1",
    "U89765,TIME,7,Medium,33.572116,-117.181043,7790.8,1.22,72.77,186.7",
    "K90123,TIME,3,High,33.626508,-117.949214,3385.14,1.02,37.84,288.9",
    "A12345,TIME,1,High,33.692544,-117.101358,6997.88,1.13,70.17,89.5",
    "J7890,TIME,2,High,32.693469,-115.483014,40.58,1.02,0.45,353.3",
    "U89765,TIME,7,Medium,33.572116,-117.181043,7790.8,1.22,72.77,186.7",
    "K90123,TIME,3,High,33.633458,-117.960844,3291.81,1.02,36.79,301.1",
    "A12345,TIME,1,High,33.696222,-117.085686,4951.44,0.87,64.92,76.8",
    "J7890,TIME,2,High,32.693365,-115.482967,40.51,1.07,0.43,155.7",
    "U89765,TIME,7,Medium,33.555244,-117.18105,6139.59,1,69.77,180",
    "K90123,TIME,3,Medium,33.639585,-117.972818,4272.57,1.02,47.76,297.1",
    "A12345,TIME,1,High,33.697284,-117.065391,6185.25,1.05,66.94,87",
    "J7890,TIME,2,High,32.693336,-115.482969,10.57,0.97,0.12,183.9",
    "U89765,TIME,7,High,33.537768,-117.173472,6765.58,1.02,75.62,156.6",
    "K90123,TIME,3,High,33.645934,-117.983971,4106.19,1.02,45.9,299.7",
    "A12345,TIME,1,High,33.698557,-117.043089,6799.35,1.15,67.19,86.7",
    "J7890,TIME,2,High,32.693287,-115.482925,22.39,1.03,0.25,138.1",
    "U89765,TIME,7,High,33.523319,-117.163811,6026.08,1.07,64.2,146.2",
    "K90123,TIME,3,High,33.650926,-117.991772,2989.42,1.03,32.87,302.6",
    "A12345,TIME,1,High,33.70549,-117.032298,4139.74,0.9,52.27,57.3",
    "J7890,TIME,2,High,32.693344,-115.482988,28.39,1.02,0.32,312.1",
    "U89765,TIME,7,High,33.504208,-117.153297,7657.22,0.98,88.49,151.2",
    "K90123,TIME,3,High,33.655257,-117.99927,2773.2,1.02,31,300",
    "A12345,TIME,1,High,33.710144,-117.016671,5045.42,0.97,59.31,73.4",
    "J7890,TIME,2,High,32.693332,-115.482728,80.11,0.98,0.93,92.6",
    "U89765,TIME,7,High,33.501085,-117.150728,1380.15,0.97,16.22,140.6",
    "K90123,TIME,3,Medium,33.66079,-118.006831,3057.45,1.18,29.36,306.2",
    "A12345,TIME,1,High,33.718612,-117.006634,4337.35,1.22,40.51,49.8",
    "J7890,TIME,2,High,32.693388,-115.482732,20.41,1.03,0.22,355.9",
    "U89765,TIME,7,High,33.502423,-117.148976,722.72,1.03,7.95,52.6",
    "K90123,TIME,3,High,33.667118,-118.015467,3494.01,0.88,44.95,306.2",
    "A12345,TIME,1,High,33.726835,-117.006632,2992.36,0.85,40,0",
    "J7890,TIME,2,Medium,32.693988,-115.48592,1004.82,1.15,9.93,280.7",
    "U89765,TIME,7,Medium,33.513857,-117.154503,4488.89,1.68,30.3,334.2",
    "K90123,TIME,3,High,33.674846,-118.026013,4266.69,1,48.49,306.2",
    "A12345,TIME,1,High,33.729318,-116.999824,2258.66,1.05,24.44,70",
    "J7890,TIME,2,High,32.693976,-115.494,2485.88,1.03,27.34,269.9",
    "U89765,TIME,7,High,33.514636,-117.155085,334.4,0.57,6.71,323.2",
    "K90123,TIME,3,High,33.684112,-118.036348,4610.41,1.02,51.53,311.9",
    "A12345,TIME,1,High,33.729377,-116.998138,513.09,1,5.83,88",
    "J7890,TIME,2,High,32.693967,-115.499007,1540.45,0.88,19.82,269.9",
    "U89765,TIME,7,High,33.516607,-117.155583,733.11,0.78,10.64,345.8",
    "K90123,TIME,3,Medium,33.695028,-118.046532,5037.52,1.02,56.31,317",
    "A12345,TIME,1,High,33.729377,-116.998138,513.09,1,5.83,88"
  )
}