#!/usr/bin/env python3

import pg
import re
import os
import datetime, time
import traceback
import logging
import sys
#import cProfile,pstats,io

logging.basicConfig(format='%(asctime)s: %(message)s', level=logging.DEBUG)
data_filename=os.path.dirname(__file__)
if len(data_filename) > 0:
    data_filename += '/'

args = sys.argv
if len(args) > 1:
    data_filename += args[1]
else:
    data_filename += 'data.log'

db_name = 'currentcost'

days_to_insert = 7
db = pg.connect(db_name)
rows_to_insert = []
nonum_regex = re.compile("[^0-9]")

def get_last_timestamp():
	query = "SELECT EXTRACT(epoch FROM max(datetime)) AS max FROM entries WHERE datetime >= current_timestamp - interval '%d days'" % (days_to_insert)
	result = db.query(query).dictresult()
	last_date = result[0]['max']
	if last_date == None:
		last_date = time.mktime((datetime.datetime.utcnow() - datetime.timedelta(days = days_to_insert)).timetuple())
	logging.debug("Inserting data since " + datetime.datetime.fromtimestamp(last_date).strftime('%d/%m/%Y %H:%M:%S'))
	return int(last_date)

def insert_into_db(datetime, sensor, power):
	query = """INSERT INTO entries (sensor, datetime, power)
	VALUES ('%s', to_timestamp(%s), '%s')"""
	
	query = query % (sensor, datetime, power)
	db.query(query)
		
def extract_number(string):
#	string = re.sub("[^0-9]", "", string)
	string = nonum_regex.sub("", string)
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
		
		if int(unixtime) > last_seen:
			current_time = int(time.mktime(datetime.datetime.today().timetuple()))
			if int(unixtime) < current_time:
				rows_to_insert.append([unixtime, sensor, power])
				last_seen = int(unixtime)
	return last_seen

def do_insert():
	db.query("BEGIN")
#	cleanup()
	logging.debug("%d rows to insert" % len(rows_to_insert))
	for row in rows_to_insert:
		datetime = row[0]
		sensor = row[1]
		power = row[2]
		insert_into_db(datetime, sensor, power)
	db.query("COMMIT")
	
def main():
#	pr = cProfile.Profile()
#	pr.enable()
	last_seen = get_last_timestamp()
	fd = open(data_filename, "r")
	rows_processed = 0

	for line in fd:
		rows_processed = rows_processed + 1
		last_seen = process(line, last_seen)
		if rows_processed % 1000000 == 0:
		    logging.debug("Processed %d rows" % rows_processed)
		if len(rows_to_insert) > 0 and len(rows_to_insert) % 1000000 == 0:
		    do_insert()
		    rows_to_insert.clear()
	do_insert()

	
#	pr.disable()
#	s = io.StringIO()
#	sortby = 'cumulative'
#	ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
#	ps.print_stats(.1)
#	print(s.getvalue())
			
	
if __name__ == "__main__":
	main()
