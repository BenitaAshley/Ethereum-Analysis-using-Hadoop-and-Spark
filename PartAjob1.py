from mrjob.job import MRJob
import re
import time
import statistics

class PartAjob1(MRJob):

	def mapper(self, _,line):
		fields = line.split(',')
		try:
			if len(fields) == 7:
				timeuk = int(fields[6])
				monthsuk = time.strftime("%m", time.gmtime(timeuk))
				yearuk = time.strftime("%y", time.gmtime(timeuk))
				yield((monthsuk,yearuk), 1)
		except:
			pass
	def reducer(self,word,counts):
		yield(word,sum(counts))

if __name__ == '__main__':
	PartAjob1.run()
