#!/usr/bin/bash

for ((t=1903; t<=2016; t++))
do
        echo "Processing year $t ...."
        hadoop fs -copyToLocal /user/hannesm/lsde/noaa/"$t".zip "$t".zip
        unzip "$t".zip
        hadoop fs -mkdir /user/lsde02/data/"$t"
        hadoop fs -moveFromLocal "$t"/*.gz /user/lsde02/data/"$t"
        rm "$t".zip
        rm -rf "$t"/
        echo "Finished processing year $t."
done
