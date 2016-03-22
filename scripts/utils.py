import math

def extract(x):
	header = (int(x[15:19]), int(x[19:21]), int(x[21:23]), int(x[4:10])) # Year, Month, Day, ID
	details = {}

	details['temp'] = x[87:92]
	details['temp-q'] = x[92]
	details['wind-direction'] = x[60:63]
	details['wind-direction-q'] = x[63]
	details['wind-speed'] = x[65:69]
	details['wind-speed-q'] = x[69]
	details['sky-condition'] = x[70:75]
	details['sky-condition-q'] = x[75]
	details['visibility'] = x[78:84]
	details['visibility-q'] = x[85]
	details['latitude'] = x[28:34]
	details['longitude'] = x[34:41]

	verify_records(details)
	
	return (header, details)

def verify_records(details):
	# Verify temperature
	if check_error_code(details['temp'], '+9999', details['temp-q'], '3', '7'):
		details.pop('temp')
	else:
		details['temp'] = int(details['temp'])/10.0
	
	details.pop('temp-q')

	# Verify wind-direction
	if check_error_code(details['wind-direction'], '999', details['wind-direction-q'], '3', '7'):
		details.pop('wind-direction')
	else:
		details['wind-direction'] = int(details['wind-direction'])
	
	details.pop('wind-direction-q')

	# Verify wind-speed
	if check_error_code(details['wind-speed'], '9999', details['wind-speed-q'], '3', '7'):
		details.pop('wind-speed')
	else:
		details['wind-speed'] = int(details['wind-speed'])
	
	details.pop('wind-speed-q')

	# Verify sky-condition
	if check_error_code(details['sky-condition'], '99999', details['sky-condition-q'], '3', '7'):
		details.pop('sky-condition')
	else:
		details['sky-condition'] = int(details['sky-condition'])

	details.pop('sky-condition-q')

	# Verify visibility
	if check_error_code(details['visibility'], '999999', details['visibility-q'], '3', '7'):
		details.pop('visibility')
	else:
		details['visibility'] = int(details['visibility'])

	details.pop('visibility-q')

	# Verify longitude
	if details['longitude']=='+99999':
		details.pop('longitude')
	else:
		details['longitude'] = int(details['longitude'])

	# Verify latitude
        if details['latitude']=='+99999':
                details.pop('latitude')
        else:
                details['latitude'] = int(details['latitude'])

def check_error_code(field, error_value, qfield, qcode1, qcode2):
	return field == error_value or qfield == qcode1 or qfield == qcode2

# Spark specific functions
def noaa_month_average(context, attribute):
	month_avg = context.filter(lambda x: attribute in x[1])
	month_avg = month_avg.map(lambda x: ((x[0][0], x[0][1], x[0][3]), x[1][attribute]))
	month_avg = month_avg.combineByKey(lambda value: (value, 1),\
                                                lambda x, value: (x[0] + value, x[1] + 1),\
                                                lambda x, y: (x[0] + y[0], x[1] + y[1]))
	return month_avg.map(lambda (label, (value_sum, count)): (label, float(value_sum)/count))

def noaa_circ_average(context, attribute):
	month_avg = context.filter(lambda x: attribute in x[1])
	month_avg = month_avg.map(lambda x: ((x[0][0], x[0][1], x[0][3]), (math.sin(x[1][attribute]*math.pi/180.0), math.cos(x[1][attribute]*math.pi/180.0))))
	month_avg = month_avg.combineByKey(lambda value: (value[0], value[1]),\
						lambda x, value: (x[0]+value[0], x[1]+value[1]),\
						lambda x,y: (x[0]+y[0], x[1]+y[1]))
	return month_avg.map(lambda (label, (sin_sum, cos_sum)): (label, math.atan2(sin_sum, cos_sum)))

def get_attribute(x, attribute):
	if attribute in x:
		return x[attribute]
	else:
		return None
