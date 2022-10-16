from mrjob.job import MRJob
from mrjob.step import MRStep

class PartCtop10(MRJob):

#Aggregating the size for addresses in miner field
	def mapper1(self, _, line):
		fields = line.split(',')
		try:
			if len(fields) == 9: #no lines are malformed
				miner = fields[2] #3rd column in the Blocks dataset
				size = float(fields[4])  #5th column in the Blocks dataset
				yield (miner,size)

		except:
			pass

	def reducer1(self, miner, size):
		try:
			yield(miner, sum(size))

		except:
			pass

	def mapper2(self, miner, totalsize):
		try:
			yield(None, (miner,totalsize))
		except:
			pass

	def reducer2(self, _, msize):
		l = 0
		try:
			sortsize = sorted(msize, reverse = True, key = lambda x:x[1])
			for m in sortsize[:10]:
				yield(m[0],m[1])
		except:
			pass
	

	def steps(self):
		return [MRStep(mapper = self.mapper1, reducer=self.reducer1), MRStep(mapper = self.mapper2, reducer = self.reducer2)]

if __name__ == '__main__':
	PartCtop10.run()	
