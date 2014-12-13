#!/usr/bin/python

import json
import time
import sqlite3
import os

header2 = ['msgID','navigationalStatus','utcSecond','extension','heading','speedOverGround','utcDay','utcMonth','actorType','mmsi','name','imo','utcMinute','slots','callSign','longitude','messageSequenceNumber','utcYear','timeStamp','latitude','destination','unit','messageIncrement','messagePayload','specialManoeuvre','typeLabel','utcHour','courseOverGround','acknowledgementFor','utcReport','messageDestination','messageOffset','position','messageApplicationID','dimensions','rateOfTurn','timeOut','eta']

headers = {
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/navigationalStatus' : 'navigationalStatus',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcSecond' : 'utcSecond',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/extension' : 'extension',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/heading' : 'heading',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/speedOverGround' : 'speedOverGround',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcDay' : 'utcDay',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcMonth' : 'utcMonth',
'http://semanticweb.cs.vu.nl/2009/11/sem/actorType' : 'actorType',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/mmsi' : 'mmsi',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/name' : 'name',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/imo' : 'imo',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcMinute' : 'utcMinute',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/slots' : 'slots',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/callSign' : 'callSign',
'http://www.w3.org/2003/01/geo/wgs84_pos#long' : 'longitude',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageSequenceNumber' : 'messageSequenceNumber',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcYear' : 'utcYear',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/timeStamp' : 'timeStamp',
'http://www.w3.org/2003/01/geo/wgs84_pos#lat' : 'latitude',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/destination' : 'destination',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/unit' : 'unit',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageIncrement' : 'messageIncrement',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/messagePayload' : 'messagePayload',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/specialManoeuvre' : 'specialManoeuvre',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/typeLabel' : 'typeLabel',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcHour' : 'utcHour',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/courseOverGround' : 'courseOverGround',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/acknowledgementFor' : 'acknowledgementFor',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/utcReport' : 'utcReport',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageDestination' : 'messageDestination',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageOffset' : 'messageOffset',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/position' : 'position',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/messageApplicationID' : 'messageApplicationID',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/dimensions' : 'dimensions',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/rateOfTurn' : 'rateOfTurn',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/timeOut' : 'timeOut',
'http://semanticweb.cs.vu.nl/poseidon/ns/ais/eta' : 'eta'
}

uniq_predicates = {}


filePath = '/Users/shri/devel/marine_data_project/raw_files'

# filelist = open(filePath)
# # for filename in os.listdir(dataFilesPath):
# filename = filePath + '/ais_part0004.ttl.bz2'

skip_ids = {}

for filename in os.listdir(filePath):

	if filename[-4:] != '.bz2':
		continue

	print filename
	os.system('gunzip ' + filePath+'/'+filename)

	t1 = time.time()
	os.system('rapper -i turtle -o json ' + filePath+'/'+filename[:-4] + ' > ais_part_temp.json')
	print 'parsed rdf file : ',(time.time() - t1)

	t1 = time.time()
	f1 = open('ais_part_temp.json')
	jData = json.load(f1)
	print 'json loaded : ',(time.time() - t1)

	t1 = time.time()

	conn = sqlite3.connect('/Users/shri/devel/marine_data_project/message_tmp.sqlite')
	cursor = conn.cursor()


	for msg in jData:
		
		if msg in skip_ids:
			continue

		# inti empty obj
		msgID = msg
		# print msgID
		rowObj = { 'msgID' : "'"+msgID+"'" }

		for h in headers:
			rowObj[headers[h]] = 'null'

		# initialize the row values
		obj = jData[msg]
		# print '-'*50
		for predicate in obj:

			if predicate not in headers:
				continue		

			obj2 = obj[predicate][0]

			if 'type' in obj2 and obj2['type'] == 'bnode':

				bNodeId = obj2['value']
				skip_ids[bNodeId] = True
				rowObj[headers[predicate]] = "'"+str(obj[predicate][0]['value']).replace("'","")+"'"

				obj3 = jData[bNodeId]
				for predicate2 in obj3:
					if predicate2 not in headers:
						continue		
					# print headers[predicate2],':',str(obj3[predicate2][0]['value']).replace("'","")

					rowObj[headers[predicate2]] = "'"+str(obj3[predicate2][0]['value']).replace("'","")+"'"

			else :	

				if predicate not in headers:
					continue		

				# print predicate,'\t',obj[predicate][0]['value']
				rowObj[headers[predicate]] = "'"+str(obj[predicate][0]['value']).replace("'","")+"'"

				# print rowObj[headers[predicate]],':',str(obj[predicate][0]['value']).replace("'","")


			# if predicate not in uniq_predicates:
			# 	uniq_predicates[predicate] = 0
			# uniq_predicates[predicate] = uniq_predicates[predicate] + 1

		valuesArr = []
		for h in header2:
			valuesArr.append(rowObj[h])

		sQuery = "insert into messages values ("+ ','.join(valuesArr) +"); "
		# print sQuery
		cursor.execute(sQuery);

	os.system('rm ais_part_temp.json')

	conn.commit()
	conn.close()	

	print 'processing done : ',(time.time() - t1)

# print len(jData)

# for p in uniq_predicates:
# 	print p,'\t',uniq_predicates[p]