from mrjob.job import MRJob
import re 
import time
import statistics
class PartAjob2(MRJob):

	def mapper(self,_, line):
		fields = line.split(',')
		try:
			if len(fields)==7:
				timeuk = int(fields[6])
				gasuk = int(fields[5])
				monthsuk = time.strftime("%m", time.gmtime(timeuk))
				yearsuk = time.strftime("%y", time.gmtime(timeuk))
				yield((monthsuk,yearsuk),(gasuk,1))
		except:
			pass
	def reducer1(self, date, price):
		average_trans = 0
		count_trans = 0
		for a, b in price:
			average_trans = (average_trans*count_trans+a*b)/(count_trans + b)
			count_trans = count_trans +b
		return(date, (average_trans,count_trans))

	def combiner(self, date, price):
		yield self.reducer1(date,price)

	def reducer2(self,date,price):     
		date, (average_trans,count_trans) = self.reducer1(date,price)
		yield(date,average_trans)

if __name__ == '__main__':
	PartAjob2.run()
