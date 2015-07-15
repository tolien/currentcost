#!/usr/bin/python

import pg
import re
import os
import datetime, time
import traceback

data_filename=os.path.dirname(__file__)
if len(data_filename) > 0:
    data_filename += '/'
data_filename += 'data.log'

db_name = 'currentcost'

days_to_insert = 7
db = pg.connect(db_name)
rows_to_insert = []

def get_last_timestamp():
	query = "SELECT EXTRACT(epoch FROM max(datetime)) AS max FROM entries WHERE datetime >= current_timestamp - interval '%d days'" % (days_to_insert)
	result = db.query(query).dictresult()
	last_date = result[0]['max']
	if last_date == None:
		last_date = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days = days_to_insert)).timetuple())
	print "Inserting data since " + datetime.datetime.fromtimestamp(last_date).strftime('%d/%m/%Y %H:%M:%S')
	return int(last_date)

def insert_into_db(datetime, sensor, power):
	query = """INSERT INTO entries (sensor, datetime, power)
	VALUES ('%s', to_timestamp(%s), '%s')"""
	
	query = query % (sensor, datetime, power)
	db.query(query)
		
def extract_number(string):
	string = re.sub("[^0-9]", "", string)
	return string

def cleanup():
    query = """DELETE FROM entries
    WHERE datetime <= current_timestamp - interval '%d days'
    
    """
    query = query % (days_to_insert)
    db.query(query)
	
def process(line, last_seen):
	line = line.strip()
	tokens = line.split(',')
	if len(tokens) > 0:
		record_datetime = tokens[0]
		sensor = extract_number(tokens[2])
		unixtime = extract_number(tokens[1])
		power = extract_number(tokens[4])
		
		current_time = int(time.mktime(datetime.datetime.today().timetuple()))
		
		if int(unixtime) > last_seen:
			if int(unixtime) < current_time:
				rows_to_insert.append([unixtime, sensor, power])
				last_seen = int(unixtime)
	return last_seen
	
def main():
	last_seen = get_last_timestamp()
	fd = open(data_filename, "r", 0)
	
	for line in fd:
		last_seen = process(line, last_seen)
	
	db.query("BEGIN")
#	cleanup()
	print "%d rows to insert" % len(rows_to_insert)
	for row in rows_to_insert:
		datetime = row[0]
		sensor = row[1]
		power = row[2]
		insert_into_db(datetime, sensor, power)
	db.query("COMMIT")
			
	
if __name__ == "__main__":
	main()
