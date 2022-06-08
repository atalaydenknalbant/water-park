# Waterpark Project

iotsimulator2.py produces fixed guid and destination values along with current timestap also payload data are from 2 iot devices.
payload data combines 2 waterpark iot devices in a single device, it collects:

1. temperature and humidity from the thermostat
2. total people and revenue from cash registers.


Sent Data =
{
{
"guid": "0-ZZZ123456788Q", 
"destination": 
"0-AAA12345678", 
"timestamp": "2022-06-08T00:33:58.824437", 
"payload": {"format": "urn:example:sensor:waterpark", 
	"data": {"temperature": 60.3, "humidity": 98.2, "totalpeople": 607047, "revenue": 1423746.9}}}

}



In our kafka-direct-iotmsg.py file we process data and determine:

1.For a given temperature and humidity data if we need to run Air conditioner at max level.
2.For a given totalpeople and revenue get information about Low Revenue.


If we want spesific date payload data we can send output of iotsimulator2.py to Kafka queue with command:
KAFKA_HEAP_OPTS="-Xmx20M" \
  ./iotsimulator2.py 1 | python -m json.tool \
  | kafka_*/bin/kafka-console-producer.sh   \
  --broker-list {***IPV4 ADRESS***} --topic iotmsgs


We can process Kafka Queue using  Spark Streaming with this command:
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.3.3 kafka-direct-iotmsg.py {***IPV4 ADRESS***} iotmsgs
