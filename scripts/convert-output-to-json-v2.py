import os
import glob
import sys
import json

FIELDS = [('avg-temp', 3), ('avg-wind-speed', 4), ('avg-wind-direction', 7), ('avg-sky', 5), ('avg-visibility', 6), ('var-temp', 8), ('var-wind-speed', 9), ('var-sky', 10), ('var-visibility', 11), ('latitude', 22), ('longitude', 23)]

def create_dir(path):
	if not os.path.exists(path):
		os.makedirs(path)

def save_file(json, path, year, month):
	create_dir("%s%s" % (path, year))
	fh = open("%s%s/%s-%s.json" % (path, year, year, month), 'w')
	fh.write(json)
	fh.close()

def process_field(result, field, line):
        value = line[field[1]].strip()

        if value == '\'NaN\'' or value == '-9223372036854775808' or value == '9223372036854775807':
                return
        else:
                result[field[0]] = float(value)

def process_line(line, extracted_month_year):
        line = line.replace('(', '').replace(')', '')
        line = line.split(',')
        result = {}
	result['identifier'] = int(line[2])

        for field in FIELDS:
                process_field(result, field, line)

	if extracted_month_year:
        	return result
	else:
		return (result, int(line[0]), int(line[1]))

def convert_to_json(fh):
	result = []

	extracted_year_month = False
	for line in fh:
		if extracted_year_month:
			line = process_line(line, True)
		else:
			(line, year, month) = process_line(line, extracted_year_month)
			extracted_year_month = True

		result.append(line)

	return (json.dumps(line), year, month)

def process_file(data_file_path, path_to_output):
	fh = open(data_file_path, 'r')

	(json, year, month) = convert_to_json(fh)
	save_file(json, path_to_output, year, month)

	fh.close()

def process_files(path_to_dataset, path_to_output):
	for data_file_path in os.listdir(path_to_dataset):
		process_file(path_to_dataset + data_file_path, path_to_output)

'''
Main program
'''
if (len(sys.argv) < 3):
	print "Please provide the path to the dataset and the path to the output directory..."
	exit(1)

path_to_dataset = sys.argv[1]
path_to_output = sys.argv[2]
process_files(path_to_dataset, path_to_output)
