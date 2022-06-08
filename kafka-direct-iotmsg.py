
"""
 Processes direct stream from kafka, '\n' delimited text directly received
   every 2 seconds.
 Usage: kafka-direct-iotmsg.py <broker_list> <topic>

 To run this on your local machine, you need to setup Kafka and create a
   producer first, see:
 http://kafka.apache.org/documentation.html#quickstart

 and then run the example
    `$ bin/spark-submit --jars \
      external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
      kafka-direct-iotmsg.py \
      localhost:9092 iotmsgs`
"""
from __future__ import print_function

import sys
import re
import json
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import OffsetRange

from operator import add


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: kafka-direct-iotmsg.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)
    sqlContext = SQLContext(sc)

    sc.setLogLevel("WARN")

    ###############
    # Globals
    ###############
    temp = 0.0
    humidity = 0.0
    people = 0
    revenue = 0.0
    today_date = datetime.datetime.today()

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})

    # Read in the Kafka Direct Stream into a TransformedDStream
    lines = kvs.map(lambda x: x[1])
    jsonLines = lines.map(lambda x: re.sub(r"\s+", "", x, flags=re.UNICODE))

    ############
    # Processing
    ############
    # foreach function to iterate over each RDD of a DStream
    def process_temp(time, rdd):
        global temp
        global today_date
        print("========= %s =========" % str(today_date))
        today_date += datetime.timedelta(days=1)
        tempList = rdd.collect()
        for tempFloat in tempList:
            temp = float(tempFloat)
            print("Temperature = " + str(temp))


    def process_humidity(time, rdd):
        global temp
        global humidity
        humidity_list = rdd.collect()
        for humFloat in humidity_list:
            humidity = float(humFloat)
            print("Humidity = " + str(humidity))
            if int(humidity) > 80 or int(temp) > 78:
                print("Run Air Conditioner At Max Level")
            else:
                pass

    def process_revenue(time, rdd):
        global revenue
        global people
        rev_list = rdd.collect()
        for revFloat in rev_list:
            revenue = float(revFloat)
            print("Revenue = " + str(revenue))
            if int(people) > 1000 and int(revenue) > 1000:
                if int(people) * 2 >= int(revenue):
                    print("Low Income")
            else:
                pass

    def process_totalpeople(time, rdd):
        global people
        people_list = rdd.collect()
        for peopleint in people_list:
            people = int(peopleint)
            print("Total People = " + str(people))


    # Search for specific IoT data values (assumes jsonLines are split(','))
    tempValues = jsonLines.filter(lambda x: re.findall(r"temperature.*", x, 0))

    # Parse out just the value without the JSON key
    parsedTempValues = tempValues.map(lambda x: re.sub(r"\"temperature\":", "", x).split(',')[0])

    # Search for specific IoT data values (assumes jsonLines are split(','))
    totalpeopleValues = jsonLines.filter(lambda x: re.findall(r"totalpeople.*", x, 0))

    # Parse out just the value without the JSON key
    parsedTotalPeopleValues = totalpeopleValues.map(lambda x: re.sub(r"\"totalpeople\":", "", x).split(',')[0])

    # Search for specific IoT data values (assumes jsonLines are split(','))
    revenueValues = jsonLines.filter(lambda x: re.findall(r"revenue.*", x, 0))

    # Parse out just the value without the JSON key
    parsedRevenueValues = revenueValues.map(lambda x: re.sub(r"\"revenue\":", "", x))

    # Search for specific IoT data values (assumes jsonLines are split(','))
    humidityValues = jsonLines.filter(lambda x: re.findall(r"humidity.*", x, 0))

    # Parse out just the value without the JSON key
    parsedHumidityValues = humidityValues.map(lambda x: re.sub(r"\"humidity\":", "", x).split(',')[0])

    # Iterate on each RDD in  DStream to use w/global variables
    parsedTempValues.foreachRDD(process_temp)
    parsedHumidityValues.foreachRDD(process_humidity)
    parsedTotalPeopleValues.foreachRDD(process_totalpeople)
    parsedRevenueValues.foreachRDD(process_revenue)
    ssc.start()
    ssc.awaitTermination()
