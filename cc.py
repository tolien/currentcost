import datetime, time
import serial
from xml.etree import ElementTree as ET
import pg

ser = serial.Serial('/dev/ttyUSB1', 57600, timeout=3)
filename = 'data.log'
#debugfile = 'debug.log'

bufsize = 0
fd = open(filename, "a+", 0)
#debug_fd = open(debugfile, "w", 0)


while 1:
	line = ""
	c = ser.readline()
	line = line + c
	# print str
	
	if len(line) > 0:
#		debug_fd.write(line + '\n')
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
			# date = date.replace(hour = int(time_tokens[0]), minute = int(time_tokens[1]), second = int(time_tokens[2]))
			# delta = datetime.datetime.today() - date
			#if (delta.days * 86400 + delta.seconds) < 0 and date.hour == 0:
			#	print delta, date
			#	date = date + datetime.timedelta(days = -1)
			
			unixtime = time.mktime(date.timetuple())
			unixtime = int(unixtime)
			
			date = date.strftime("%d/%m/%Y")
			line = "%s %s, %d, Sensor %d, %f%sC, %dW" % (date, record_time, unixtime, sensor, temp, unichr(176).encode("UTF8"), power)
			print line
#			debug_fd.write(line)
			fd.write(line + '\n')
			
			#insert_into_db(date, time, sensor, power)
		except AttributeError:
			pass
		except KeyboardInterrupt:
			print "Terminated."
		except ET.ParseError:
		    print line
