import sys
import time
import math
import utils
from pyspark.context import SparkContext

if (len(sys.argv) == 2):
	hdfs_file_path = "/user/lsde02/data/%s/*.gz" % sys.argv[1]
	forced_partitions = 12
elif len(sys.argv) == 3:
	directories = "{"
	for i in range(int(sys.argv[2]), int(sys.argv[3])+1):
		directories += i
		if i < int(sys.argv[3]):
			directories += ","
	directories += "}"
	hdfs_file_path = "/user/lsde02/data/%s/*.gz" % directories
	forced_partitions = (int(sys.argv[3])-int(sys.argv[2]))*12
else:
	hdfs_file_path = "/user/lsde02/data/*/*.gz"
	forced_partitions = 1500

hdfs_results_path = "/user/lsde02/results/"
start_time = time.strftime("%Y-%m-%d-%H-%M-%S")
print "Started processing: %s" % hdfs_file_path

sc = SparkContext()
context = sc.textFile(hdfs_file_path, forced_partitions)
stations = context.flatMap(lambda x: [utils.extract(record) for record in x.splitlines()])
stations = stations.filter(lambda x: 'longitude' in x[1] and 'latitude' in x[1])

# Do computations on month level
month_data = stations.map(lambda x:((x[0][0], x[0][1], x[0][3], x[1]['longitude'], x[1]['latitude']), (utils.get_attribute(x[1], 'temp'), utils.get_attribute(x[1], 'windspeed'), \
			utils.get_attribute(x[1], 'sky-condition'), utils.get_attribute(x[1], 'visibility'), utils.get_attribute(x[1], 'wind-direction'))))
month_data = month_data.combineByKey(lambda value: (value[0] if value[0] != None else 0, 1 if value[0] != None else 0,\
					value[1] if value[1] != None else 0, 1 if value[1] != None else 0, \
					value[2] if value[2] != None else 0, 1 if value[2] != None else 0, \
					value[3] if value[3] != None else 0, 1 if value[3] != None else 0, \
					math.sin(value[4]*math.pi/180.0) if value[4] != None else 0, \
					math.cos(value[4]*math.pi/180.0) if value[4] != None else 0, \
					value[0]*value[0] if value[0] != None else 0, \
					value[1]*value[1] if value[1] != None else 0, \
					value[2]*value[2] if value[2] != None else 0, \
					value[3]*value[3] if value[3] != None else 0, \
					value[0] if value[0] != None else sys.maxint, \
					value[1] if value[1] != None else sys.maxint, \
					value[2] if value[2] != None else sys.maxint, \
					value[3] if value[3] != None else sys.maxint, \
					value[0] if value[0] != None else -sys.maxint-1, \
					value[1] if value[1] != None else -sys.maxint-1, \
					value[2] if value[2] != None else -sys.maxint-1, \
					value[3] if value[3] != None else -sys.maxint-1), \
				lambda x, value: (utils.get_value(value[0]) + x[0], x[1] + 1, utils.get_value(value[1])+x[2], 1 + x[3], utils.get_value(value[2]) + x[4], 1 + x[5],\
					utils.get_value(value[3])+x[6], 1 + x[7], (math.sin(value[4]*math.pi/180.0) if value[4]!=None else 0) + x[8], (math.cos(value[4]*math.pi/180.0) if value[4] != None else 0) + x[9], (value[0]*value[0] if value[0]!=None else 0) + x[10],\
					(value[1]*value[1] if value[1]!=None else 0) + x[11],\
					(value[2]*value[2] if value[2]!=None else 0) + x[12],\
					(value[3]*value[3] if value[3]!=None else 0) + x[13],\
					(min(value[0],x[14]) if value[0]!= None else x[14]),\
					(min(value[1],x[15]) if value[1]!= None else x[15]),\
					(min(value[2],x[16]) if value[2]!= None else x[16]),\
					(min(value[3],x[17]) if value[3]!= None else x[17]),\
					(max(value[0],x[18]) if value[0]!= None else x[18]),\
					(max(value[1],x[19]) if value[1]!= None else x[19]),\
					(max(value[2],x[20]) if value[2]!= None else x[20]),\
					(max(value[3],x[21]) if value[3]!= None else x[21])),\
				lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6], x[7]+y[7], x[8]+y[8],\
					x[9]+y[9], x[10]+y[10], x[11]+y[11], x[12]+y[12], x[13]+y[13], min(x[14], y[14]), min(x[15], y[15]), min(x[16], y[16]), \
					min(x[17], y[17]), max(x[18], y[18]), max(x[19], y[19]), max(x[20], y[20]), max(x[21], y[21]))) 
month_data = month_data.map(lambda (label, (x1, c1, x2, c2, x3, c3, x4, c4, x5a, x5b, x1sq, x2sq, x3sq, x4sq, x1min, x2min, x3min, x4min, x1max, x2max, x3max, x4max)): (label, (float(x1)/c1 if c1>0 else "NaN", float(x2)/c2 if c2>0 else "NaN", float(x3)/c3 if c3>0 else "NaN", float(x4)/c4 if c4>0 else "NaN", math.atan2(x5a, x5b),\
		1.0/(c1-1)*(x1sq-2*(float(x1)/c1)*x1+c1*(float(x1)/c1)**2) if c1>1 else "NaN", \
		1.0/(c2-1)*(x2sq-2*(float(x2)/c2)*x2+c2*(float(x2)/c2)**2) if c2>1 else "NaN", \
		1.0/(c3-1)*(x3sq-2*(float(x3)/c3)*x3+c3*(float(x3)/c3)**2) if c3>1 else "NaN", \
		1.0/(c4-1)*(x4sq-2*(float(x4)/c4)*x4+c4*(float(x4)/c4)**2) if c4>1 else "NaN", \
		x1min, x2min, x3min, x4min, x1max, x2max, x3max, x4max)))

if len(sys.argv) == 2:
	c = 12
else if len(sys.argv) == 3:
	c = int((sys.argv[2])-int(sys.argv[1])) * 12
else:
	c = (2016-1901)*12

month_data = month_data.partitionBy(c, lambda x: x[0]*100 + x[1])
month_data.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, 'all'))
