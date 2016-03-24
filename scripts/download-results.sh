#!/usr/bin/bash

mkdir $1
hadoop fs -copyToLocal /user/lsde02/results/$1-sky-condition $1/sky-condition
hadoop fs -copyToLocal /user/lsde02/results/$1-temp $1/temp
hadoop fs -copyToLocal /user/lsde02/results/$1-wind-speed $1/wind-speed
hadoop fs -copyToLocal /user/lsde02/results/$1-visibility $1/visibility
hadoop fs -copyToLocal /user/lsde02/results/$1-wind-direction $1/wind-direction
