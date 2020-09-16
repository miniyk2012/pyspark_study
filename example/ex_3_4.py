from pyspark.context import SparkContext

sc = SparkContext('local', 'test')
lines = sc.textFile('file:///usr/local/spark-2.4.7-bin-hadoop2.6/README.md')
pythonlines = lines.filter(lambda line: 'Spark' in line)
pythonlines.persist()
print(pythonlines.count())
for line in pythonlines.collect():
    print(line)
sc.stop()


