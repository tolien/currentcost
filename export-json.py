#!/usr/bin/python

import time, datetime, calendar
import types
import pg
import json

db = pg.connect('currentcost')

periods = [
	['1 day', 'minute'],
#	['7 days', 'hour']
]

export = {'power' : []}
export_fragment = {
	'period': {},
	'data': []
}

def handle(result, period):
	fragment = export_fragment.copy()
	fragment['period'] = period[0]
	for line in result:
		human_time = line['datetime']
		item_time = line['timestamp']*1000
		power = int(line['power'])
		
		item = [item_time, power]
		fragment['data'].append(item)
		
	export['power'].append(fragment)
	
for period in periods:
	query = """SELECT dt AS datetime, (extract (epoch FROM dt) + extract (timezone FROM dt)) AS timestamp, power FROM
	(
	SELECT date_trunc('%s', datetime) AS dt, avg(power) AS power
	    FROM power
	    WHERE sensor = 0
		AND datetime >= current_timestamp - interval '%s'
	    GROUP BY dt
	    ORDER BY dt
	) AS sq""" % (period[1], period[0])
	
	result = db.query(query).dictresult()
	
	handle(result, period)


file = open('currentcost-data.json', 'w')
file.write(json.dumps(export, sort_keys=True))
