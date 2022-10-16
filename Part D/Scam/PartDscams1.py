from mrjob.job import MRJob
from mrjob.step import MRStep
import json


class PartDscams1(MRJob):

	def steps(self):
		return [MRStep(mapper=self.mapper1, reducer=self.reducer1),
        		MRStep(mapper=self.mapper2, reducer=self.reducer2)]
	def mapper1(self, _, lines):
		try:
		    Field = lines.split(",")
		    if len(Field) == 7:
		        Address = Field[2]
		        value = float(Field[3])
		        yield Address, (value, 0)
		    else:
		        line = json.loads(lines)
		        keys = line["result"]

		        for i in keys:
		            record = line["result"][i]
		            category = record["category"]
		            addresses = record["addresses"]

		            for j in addresses:
		                yield j, (category, 1)

		except:
		    pass

	def reducer1(self, key, values):
		    tvalue = 0
		    category = None

		    for k in values:
		        if k[1] == 0:
		            tvalue = tvalue+ k[0]
		        else:
		            category = k[0]
		    if category is not None:
		        yield category, tvalue

	def mapper2(self, key, value):
		    yield (key, value)

	def reducer2(self, key, value):
		    yield (key, sum(value))

	def combiner(self, key, Value):
		    yield (key, sum(Value))

if __name__ == "__main__":
	PartDscams1.run()
