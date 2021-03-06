import sys
import time
import math
import utils
from pyspark.context import SparkContext

MIN_NUM_OF_MONTHLY_OBSERVATIONS = 30

if (len(sys.argv) == 2):
	hdfs_file_path = "/user/lsde02/data/%s/*.gz" % sys.argv[1]
	forced_partitions = 12
elif len(sys.argv) == 3:
	directories = "{"
	for i in range(int(sys.argv[1]), int(sys.argv[2])+1):
		directories += str(i)
		if i < int(sys.argv[2]):
			directories += ","
	directories += "}"
	hdfs_file_path = "/user/lsde02/data/%s/*.gz" % directories
	forced_partitions = (int(sys.argv[2])+1-int(sys.argv[1]))*12
else:
	hdfs_file_path = "/user/lsde02/data/*/*.gz"
	forced_partitions = 1500

hdfs_results_path = "/user/lsde02/results/"
start_time = time.strftime("%Y-%m-%d-%H-%M-%S")
print "Started processing: %s" % hdfs_file_path

sc = SparkContext()
context = sc.textFile(hdfs_file_path, forced_partitions)
stations = context.flatMap(lambda x: [utils.extract(record) for record in x.splitlines()])
#stations = stations.filter(lambda x: 'fixed-weather-station' in x[1] or )

# Do computations on month level
month_data = stations.map(lambda x:((x[0][0], x[0][1], x[0][3]), (utils.get_attribute(x[1], 'temp'), utils.get_attribute(x[1], 'windspeed'), \
			utils.get_attribute(x[1], 'sky-condition'), utils.get_attribute(x[1], 'visibility'), utils.get_attribute(x[1], 'wind-direction'), \
			utils.get_attribute(x[1], 'latitude'), utils.get_attribute(x[1], 'longitude'))))
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
					value[3] if value[3] != None else -sys.maxint-1, \
					value[5] if value[5] != None else 0, 1 if value[5] != None else 0, \
					value[6] if value[6] != None else 0, 1 if value[6] != None else 0,\
					value[5]*value[5] if value[5] != None else 0, \
					value[6]*value[6] if value[6] != None else 0),\
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
					(max(value[3],x[21]) if value[3]!= None else x[21]),\
					utils.get_value(value[5]) + x[22], (1 + x[23]) if value[5] != None else x[23], \
					utils.get_value(value[6]) + x[24], (1 + x[25]) if value[6] != None else x[25],\
					utils.get_value(value[5])**2 + x[26],\
					utils.get_value(value[6])**2 + x[27]),\
				lambda x, y: (x[0]+y[0], x[1]+y[1], x[2]+y[2], x[3]+y[3], x[4]+y[4], x[5]+y[5], x[6]+y[6], x[7]+y[7], x[8]+y[8],\
					x[9]+y[9], x[10]+y[10], x[11]+y[11], x[12]+y[12], x[13]+y[13], min(x[14], y[14]), min(x[15], y[15]), min(x[16], y[16]), \
					min(x[17], y[17]), max(x[18], y[18]), max(x[19], y[19]), max(x[20], y[20]), max(x[21], y[21]), x[22]+y[22], x[23]+y[23], x[24]+y[24], x[25]+y[25], x[26]+y[26], x[27]+y[27])) 
month_data = month_data.filter(lambda (label, data): min(data[23], data[25]) >= MIN_NUM_OF_MONTHLY_OBSERVATIONS)
month_data = month_data.map(lambda (label, (x1, c1, x2, c2, x3, c3, x4, c4, x5a, x5b, x1sq, x2sq, x3sq, x4sq, x1min, x2min, x3min, x4min, x1max, x2max, x3max, x4max, x6, c6, x7, c7, x6sq, x7sq)): (label, (float(x1)/c1 if c1>0 else "NaN", float(x2)/c2 if c2>0 else "NaN", float(x3)/c3 if c3>0 else "NaN", float(x4)/c4 if c4>0 else "NaN", math.atan2(x5a, x5b),\
		1.0/(c1-1)*(x1sq-2*(float(x1)/c1)*x1+c1*(float(x1)/c1)**2) if c1>1 else "NaN", \
		1.0/(c2-1)*(x2sq-2*(float(x2)/c2)*x2+c2*(float(x2)/c2)**2) if c2>1 else "NaN", \
		1.0/(c3-1)*(x3sq-2*(float(x3)/c3)*x3+c3*(float(x3)/c3)**2) if c3>1 else "NaN", \
		1.0/(c4-1)*(x4sq-2*(float(x4)/c4)*x4+c4*(float(x4)/c4)**2) if c4>1 else "NaN", \
		x1min if x1min != sys.maxint else "NaN",\
		x2min if x2min != sys.maxint else "NaN",\
		x3min if x3min != sys.maxint else "NaN",\
		x4min if x4min != sys.maxint else "NaN",\
		x1max if x1max != -sys.maxint-1 else "NaN", \
		x2max if x2max != -sys.maxint-1 else "NaN", \
		x3max if x3max != -sys.maxint-1 else "NaN", \
		x4max if x4max != -sys.maxint-1 else "NaN", \
		float(x6)/c6 if c6>0 else "NaN", float(x7)/c7 if c7>0 else "NaN",\
		1.0/(c6-1)*(x6sq-2*(float(x6)/c6)*x6+c6*(float(x6)/c6)**2) if c6>1 else "NaN",\
		1.0/(c7-1)*(x7sq-2*(float(x7)/c7)*x7+c7*(float(x7)/c7)**2) if c7>1 else "NaN")))
month_data = month_data.filter(lambda (label, x): math.sqrt(x[19]) < 300 and math.sqrt(x[20]) < 300)

if len(sys.argv) == 2:
	c = 12
	name = sys.argv[1]
elif len(sys.argv) == 3:
	c = (int(sys.argv[2])+1-int(sys.argv[1])) * 12
	name = sys.argv[1] + "-" + sys.argv[2]
else:
	c = (2016-1901)*12
	name = "all"

month_data = month_data.partitionBy(c, lambda x: x[0]*100 + x[1])
month_data.saveAsTextFile("%s%s-%s" % (hdfs_results_path, start_time, name))
