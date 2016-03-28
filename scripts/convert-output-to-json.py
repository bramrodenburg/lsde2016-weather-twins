import sys
import os
import json

FIELDS = [('identifier', 2), ('latitude', 3), ('longitude', 4), ('avg-temp', 5), ('avg-wind-speed', 6)]

'''
Helper functions
'''

def store_output(json_output, file_name):
	fh = open(file_name, 'w')
	fh.write(json_output)
	fh.close()

def process_field(result, field, line):
	value = line[field[1]].strip()

	if value == '\'NaN\'' or value == '-9223372036854775808' or value == '9223372036854775807':
		return
	else:
		result[field[0]] = float(value)

def process_line(line):
	line = line.replace('(', '').replace(')', '')
	line = line.split(',')
	result = {}

	for field in FIELDS:
		process_field(result, field, line)

	return result

def convert_to_json(fh):
	result = []

	for line in fh:
		entry = process_line(line)
		result.append(entry)

	return json.dumps(result)

def convert_file(file_name):
	fh = open(file_name, 'r')
	json_output = convert_to_json(fh)
	fh.close()
	
	return json_output

'''
Actual program
'''
if len(sys.argv) < 2:
	print "Please provide the file(s) that need to be converted to json."
	exit(1)

for i in range(1,len(sys.argv)):
	file_name = sys.argv[i]
	json_output = convert_file(file_name)
	store_output(json_output, "%s.json" % file_name)
