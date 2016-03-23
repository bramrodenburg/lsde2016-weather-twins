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
month_data = stations.map(lambda x:((x[0][0], x[0][1], x[0][3]), (utils.get_attribute(x[1], 'temp'), utils.get_attribute(x[1], 'windspeed'), \
			utils.get_attribute(x[1], 'sky-condition'), utils.get_attribute(x[1], 'visibility'), utils.get_attribute(x[1], 'wind-direction'))))
month_data = month_data.combineByKey(lambda value: (value[0] if value[0] != None else 0, 1 if value[0] != None else 0,\
					value[1] if value[1] != None else 0, 1 if value[1] != None else 0, \
					value[2] if value[2] != None else 0, 1 if value[2] != None else 0, \
					value[3] if value[3] != None else 0, 1 if value[3] != None else 0, \
					math.sin(value[4]*math.pi/180.0) if value[4] != None else 0, \
					math.cos(value[4]*math.pi/180.0) if value[4] != None else 0),\
				lambda x, value: (utils.get_value(value[0]) + x[0], x[1] + 1, utils.get_value(value[1])+x[2], 1 + x[3], utils.get_value(value[2]) + x[4], 1 + x[5],\
					utils.get_value(value[3])+x[6], 1 + x[7], (math.sin(value[4]*math.pi/180.0) if value[4]!=None else 0) + x[8], (math.cos(value[4]*math.pi/180.0) if value[4] != None else 0) + x[9]),\
				lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6], x[7]+y[7], x[8]+y[8],\
					x[9]+y[9])) 
month_data = month_data.map(lambda (label, (x1, c1, x2, c2, x3, c3, x4, c4, x5a, x5b)): (label, (float(x1)/c1 if c1>0 else "NaN", float(x2)/c2 if c2>0 else "NaN", float(x3)/c3 if c3>0 else "NaN", float(x4)/c4 if c4>0 else "NaN", math.atan2(x5a, x5b))))
month_data = month_data.coalesce(1, True)
month_data.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'all'))

