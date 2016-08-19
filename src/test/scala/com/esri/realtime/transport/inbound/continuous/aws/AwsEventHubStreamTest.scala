package com.esri.realtime.transport.inbound.continuous.aws

import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.scalatest.{BeforeAndAfter, FlatSpec, FunSuite}
import org.scalatest.concurrent.Eventually
import com.typesafe.config.ConfigFactory

class AwsEventHubStreamTest extends FlatSpec with Eventually with BeforeAndAfter {
  
  private val _master = "local[2]"    
  private val _appName = "Spark Receiver Stream"
  
  private var _keyStorePass:KeyStorePasswordPair = _  
  private var _ssc: StreamingContext = _
  private var _sc : SparkContext = _
  private val _batchDuration = Seconds(1)
  
  private var _certificateFile: String = _
  private var _privateKeyFile: String = _
  private var _topicName: String = _
  private var _clientEndPoint: String = _
  
  before {
    
    //config
    val config = ConfigFactory.load()
    //aws params
    _clientEndPoint = config.getString("awsiot.endpoint")
    _certificateFile = config.getString("awsiot.certificate")
    _privateKeyFile = config.getString("awsiot.privateKey") 
    _topicName = config.getString("awsiot.topicName")
    
    //spark conf
    /*val conf = new SparkConf()
      .setMaster(_master).setAppName(_appName)
      .set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    //streaming context
    ssc = new StreamingContext(conf, _batchDuration)*/
   
  }
  
  after {
    if (_ssc != null){
    _ssc.stop()
    }
  }
  
  "KeyStore Password " should "no be null " in { 
    _keyStorePass = SslUtil.generateFromFilePath(_certificateFile, _privateKeyFile)    
    assert(_keyStorePass.keyPass != null)
  }
  
  
  

}