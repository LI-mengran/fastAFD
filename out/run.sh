#!/bin/bash
java -jar -Xms2g -Xmx52g FastAFD.jar -r -1 -f ./dataset/restaurant.csv -t 0 -s threshold/restaurant.txt -e ""
java -jar -Xms2g -Xmx52g FastAFD.jar -r -1 -f ./dataset/pcm.csv -t 0 -s threshold/pcm.txt -e ""
java -jar -Xms2g -Xmx52g FastAFD.jar -r -1 -f ./dataset/struct.csv -t 0 -s threshold/struct.txt -e ""


