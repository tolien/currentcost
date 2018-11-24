#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime, time
import serial
from xml.etree import ElementTree as ET
import pg
import os
import datetime
import time
import logging

logging.basicConfig(filename='debug.log', level=logging.DEBUG, format='%(asctime)s: %(message)s')
ser = serial.Serial('/dev/ttyUSB0', 57600, timeout=3)
filename = 'data.log'
rotate_every = 100 * 1024 * 1024

bufsize = 0
fd = open(filename, "a+", 1)

logging.debug("Started")
try:
	while 1:
		size = os.path.getsize(filename)
		if size > rotate_every:
			logging.debug("*** rotating log file")
			logging.debug("File size: %d bytes, rotate threshold: %d bytes" % (size, rotate_every))
			logging.debug("***")
			fd.flush()
			os.fsync(fd)
			fd.close()
			os.system("sudo -u sswindells python3 store.py")
			timestamp = datetime.datetime.now()
			timestamp = time.mktime(timestamp.timetuple())
			timestamp = str(int(timestamp))
			os.rename(filename, filename + "." + timestamp)
			fd = open(filename, "a+", 1)
		line = ""
		c = ser.readline()
		try:
			c = c.decode('UTF-8')
			line = line + c
		except UnicodeDecodeError:
		    logging.debug("Unable to decode line %s" % c)
		if len(line) > 0:
			try:
				element = ET.XML(line)
				
				record_time = element.find('time').text
				time_tokens = record_time.split(':')
				
				sensor = element.find('sensor').text
				sensor = int(sensor)
				
				temp = element.find('tmpr').text
				temp = float(temp)
				
				power = element.find('ch1').find('watts').text
				power = int(power)
				
				date = datetime.datetime.today()
				record_time = date.strftime("%H:%M:%S")
				
				unixtime = time.mktime(date.timetuple())
				unixtime = int(unixtime)
				
				date = date.strftime("%d/%m/%Y")
				line = "%s %s, %d, Sensor %d, %f%sC, %dW" % (date, record_time, unixtime, sensor, temp, chr(176), power)
				print(line)
				fd.write(line + '\n')
				
			except AttributeError:
				pass
			except ET.ParseError:
			    logging.debug("Parse Error: %s" % line)
except KeyboardInterrupt:
	logging.debug("Terminated by KeyboardInterrupt.")

