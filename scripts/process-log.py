import sys
from datetime import date, time

'''
An explanation of all the attributes can found in:
Federal Climate Complex Data Documentation For Integrated Surface Data, August 20, 2015
'''
def process_line(line):
	'''Control data section'''
	total_variable_characters = line[0:4]
	usaf_weather_station_id = line[4:10]
	ncei_weather_station_id = line[9:15]
	observation_date = date(int(line[15:19]), int(line[19:21]), int(line[21:23])) # date format: YYYYMMDD
	observation_time = time(int(line[23:25]), int(line[25:27])) # time format: HHMM
	observation_flag = line[27] if line[27] != "9" else None
	latitude = int(line[28:34]) if line[28:34] != "+99999" else None
	longitude = int(line[34:41]) if line[34:41] != "+999999" else None
	print "(%d,%d)" % (latitude, longitude)
	report_type = line[41:46] if line[41:46] != "99999" else None
	elevation = int(line[46:51]) if line[46:51] != "+9999" else None
	call_letter_id = line[51:56] if line[51:56] != "99999" else None
	quality_control_name = line[56:60]

	'''Mandatory data'''
	'''Wind data'''
	wind_angle = int(line[60:63]) if int(line[60:63]) != 999 else None
	wind_direction_quality_code = int(line[63])
	wind_observation_type = line[64] if line[64] != '9' else None
	wind_speed_rate = int(line[65:69]) if line[65:69] != '9999' else None
	wind_speed_quality_code = int(line[69])
	'''Sky data'''
	sky_ceiling_height = int(line[70:75]) if int(line[70:75]) != 99999 else None
	sky_ceiling_quality = int(line[75])
	sky_ceiling_code = line[76] if line[76] != '9' else None
	sky_ceiling_cavok = line[77] if line[77] != '9' else None
	'''Visibility'''
	visibility_distance = int(line[78:84]) if line[78:84] != '999999' else None
	visibility_distance_quality = int(line[84])
	visibility_variability = line[85] if line[85] != '9' else None
	visibility_variability_quality = line[86]
	'''Air temperature'''
	air_temperature = int(line[87:92]) if line[87:92] != "+9999" else None
	air_temperature_quality = line[92]
	'''Air dew temeprature'''
	air_dew_temperature = int(line[93:98]) if line[93:98] != "+9999" else None
	air_dew_temperature_quality = line[98]
	'''Sea level pressure '''
	sea_level_pressure = int(line[99:104]) if line[99:104] != "99999" else None
	sea_level_pressure_quality = line[104]
	'''Additional Data'''
	# Still needs to be written

def process_file(content):
	for line in content:
		process_line(line)

