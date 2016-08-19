# spark-receiver-for-awsiot
0. git clone https://github.com/venukanaparthy/spark-receiver-for-awsiot
   cd spark-receiver-for-awsiot

1. mvn clean package

2. Start receiver on 5 second batch size 
  SPARK_HOME\bin\spark-submit --class com.esri.realtime.transport.inbound.continuous.aws.AwsEventHubClient target\spark-receiver-awsiot-0.1.jar receiver 5
      
3. Start similator to send messages to Aws IoT Hub (send 3 messages every 1000ms)
	SPARK_HOME\bin\spark-submit --class com.esri.realtime.transport.inbound.continuous.aws.AwsEventHubClient target\spark-receiver-awsiot-0.1.jar simulator 3 1000
	