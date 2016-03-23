import sys
import time
import math
import utils
from pyspark.context import SparkContext

if (len(sys.argv) > 1):
	hdfs_file_path = "/user/lsde02/data/%s/*.gz" % sys.argv[1]
else:
	hdfs_file_path = "/user/lsde02/data/1901/*.gz"
hdfs_results_path = "/user/lsde02/results/"
start_time = time.strftime("%Y-%m-%d-%H-%M-%S")

sc = SparkContext()
context = sc.textFile(hdfs_file_path)
stations = context.flatMap(lambda x: [utils.extract(record) for record in x.splitlines()])
stations = stations.filter(lambda x: 'longitude' in x[1] and 'latitude' in x[1])

# Do computations on month level
month_data = stations.map(lambda x:((x[0][0], x[0][1], x[0][3]), (get_attribute(x[1], 'temp'), get_attribute(x[1], 'windspeed'), \
			get_attribute(x[1], 'sky-condition'), get_attribute(x[1], 'visibility'), get_attribute(x[1], 'wind-direction'))))
month_data = month_data.combineByKey(lambda value: (x[0] if x[0] != None else 0, 1 if x[0] != None else 0,\
					x[1] if x[1] != None else 0, 1 if x[1] != None else 0, \
					x[2] if x[2] != None else 0, 1 if x[2] != None else 0, \
					x[3] if x[3] != None else 0, 1 if x[3] != None else 0, \
					math.sin(x[4])*math.pi/180.0 if x[4] != None else 0, math.cos(x[4]).math.pi/180.0 if x[4] != None else 0, ),\
				lambda x, value: (x[0] + value[0], value[1] + 1, x[1]+value[2], 1 + value[3], x[3] + value[4], 1 + value[5],\
					x[6]+value[6], 1 + value[7], x[8] + value[8], x[9] + value[9]),\
				lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6], x[7]+y[7], x[8]+y[8],\
					x[9]+y[9])) 
month_data = month_data.map(lambda (label, (x1, c1, x2, c2, x3, c3, x4, c4, x5a, x5b)): (label, (x1/c1, x2/c2, x3/c3, x4/c4, math.atan2(x5a, x5b))))
month_data = month_data.coalesce(1, True)
month_data.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'all'))

