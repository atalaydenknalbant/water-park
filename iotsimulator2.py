#!/bin/env python3

from __future__ import print_function
import json
import sys
import datetime
import random
import string
import secrets

guid = {}
destination = {}
timestamp = {}
messagebody = {}
format = {}
data = {}
payload = {}

# Set number of simulated messages to generate
if len(sys.argv) > 1:
    numMsgs = int(sys.argv[1])
else:
    numMsgs = 1

# Fixed values
guidStr = "0-ZZZ12345678"
destinationStr = "0-AAA12345678"

formatStr = "urn:example:sensor:waterpark"

# Choice for random letter
letters = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

rng = secrets.SystemRandom()
# Generate JSON output:
print("[ ")
for counter in range(0, numMsgs):
    randInt = rng.randrange(0, 9)
    randLetter = rng.choice(letters)
    messagebody['guid'] = guidStr+str(randInt)+randLetter

    messagebody['destination'] = destinationStr

    today = datetime.datetime.today()
    dateStr = today.isoformat()
    messagebody['timestamp'] = dateStr

    payload['format'] = formatStr

    # Generate random floating point numbers
    randTemp = round(rng.uniform(60.0, 100.0), 1)
    randHumidity = round(rng.uniform(35.0, 100.0), 1)
    randTotalPeople = rng.randrange(100000, 1000001)
    randRevenue = round(rng.uniform(randTotalPeople,randTotalPeople*5),1)

    data['temperature'] = randTemp
    data['humidity'] = randHumidity

    data['totalpeople'] = randTotalPeople
    data['revenue'] = randRevenue
    payload['data'] = (data)
    messagebody['payload'] = (payload)

    if counter != 0:
        print(", ")

    message = json.dumps(messagebody)
    print(message, end='')

print()
print(" ]")

