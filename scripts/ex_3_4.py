import findspark
findspark.init()
from pyspark.context import SparkContext

sc = SparkContext(appName='test')
lines = sc.textFile('file:///usr/local/spark-2.4.7-bin-hadoop2.6/README.md')
print(lines.count())
for line in lines.collect():
    print(line)
sc.stop()


