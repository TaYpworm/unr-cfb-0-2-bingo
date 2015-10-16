#!/usr/bin/python

from optparse import OptionParser
from pymongo import MongoClient
import re

def main():
	parser = OptionParser()
	parser.add_option('-i', '--infile', dest='in_file', action='store', help='UNR CFB 0 input file name')
	parser.add_option('-o', '--outfile', dest='out_file', action='store', help='Bingo output file name')
	parser.add_option('-s', '--server', dest='server', action='store', help='MongoDB server hostname', default='localhost')
	parser.add_option('-p', '--port', dest='port', type='int', action='store', help='MongoDB server port', default='27017')
	parser.add_option('-d', '--db', dest='db', action='store', help='database name', default='uniprot')

	option, args = parser.parse_args()

	client = MongoClient(option.server, option.port)
	db = client[option.db]
	collection = db['uniprot_kb']

	convert_to_bingo(option.in_file, option.out_file, collection)

def convert_to_bingo(in_file, out_file, col):
	with open(in_file) as i, open(out_file, 'w') as o:
		for line in i:
			data = line.strip('\n').split('\t')
			fmt = '{0} = '.format(data[0])

			if len(data) != 2:
				raise DataError('Expected 2 columns, read {0}.'.format(len(data)))

			if data[1] == '':
				continue
			elif data[1].startswith('UPI'):
				query = col.find_one({'UniParc': data[1]})
				if query:
					write_gos(o, fmt, query['GO'])
			elif data[1].startswith('UniRef50'):
				query = col.find_one({'UniRef50': data[1]})
				if query:
					write_gos(o, fmt, query['GO'])
			else:
				query = col.find_one({'UniProtKB-ID': data[1]})
				if query:
					write_gos(o, fmt, query['GO'])

def write_gos(f, fmt, gos):
	if gos != '':
		if not isinstance(gos, list):
			gos = [gos]

		for go in gos:
			go_id = re.match('^GO:0*(\d*)', go).group(1)
			if go_id:
				f.write(fmt + go_id + '\n')


class DataError(Exception):
	pass

if __name__ == '__main__':
	main()