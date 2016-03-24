import sys

if len(sys.argv) < 2:
	print "Please provide the HDFS directory on which the script should run."
	exit()

context = sc.textFile(argv[1])
lines = context.flatMap(lambda x: x.splitlines())
weather_stations = lines.map(lambda x: (x[4:10], 1))
station_counts = weather_stations.reduceByKey(lambda x, y: x+y).collect()
print station_counts
