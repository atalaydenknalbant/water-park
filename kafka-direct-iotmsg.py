
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
    tempTotal = 0.0
    tempCount = 0
    tempAvg = 0.0
    humTotal = 0.0
    humCount = 0
    humAvg = 0.0
    peopleTotal = 0.0
    peopleCount = 0
    peopleAvg = 0.0
    revTotal = 0.0
    revCount = 0
    revAvg = 0.0
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
    # Match local function variables to global variables
        global tempTotal
        global tempCount
        global tempAvg
        global today_date
        print("========= %s =========" % str(today_date))
        today_date += datetime.timedelta(days=1)
        tempList = rdd.collect()
        for tempFloat in tempList:
            tempTotal += float(tempFloat)
            tempCount += 1
            tempAvg = tempTotal / tempCount
            print("Avg Temperature = " + str(tempAvg))


    def process_humidity(time, rdd):
        global humTotal
        global humCount
        global humAvg
        global tempAvg
        humudity_list = rdd.collect()
        for humFloat in humudity_list:
            humTotal += float(humFloat)
            humCount += 1
            humAvg = humTotal / humCount
            print("Avg Humidity = " + str(humAvg))
        if int(humAvg) > 80 or int(tempAvg) > 78:
            print("Run Air Conditioner At Max Level")
            humAvg = 0
            tempAvg = 0
        else:
            pass

    def process_revenue(time, rdd):
        global revTotal
        global revCount
        global revAvg
        global peopleAvg
        rev_list = rdd.collect()
        for revFloat in rev_list:
            revTotal += float(revFloat)
            revCount += 1
            revAvg = revTotal / revCount
            print("Avg Revenue = " + str(revAvg))
        if int(peopleAvg) > 1000 and int(revAvg) > 1000:
            if int(peopleAvg) * 2 <= int(revAvg):
                print("Low Income")
                peopleAvg = 0
                revAvg = 0

        else:
            pass

    def process_totalpeople(time, rdd):
        global peopleTotal
        global peopleCount
        global peopleAvg
        people_list = rdd.collect()
        for peopleFloat in people_list:
            peopleTotal += float(peopleFloat)
            peopleCount += 1
            peopleAvg = peopleTotal / peopleCount
            print("Avg Total People = " + str(peopleAvg))


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
