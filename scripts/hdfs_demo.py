import findspark
findspark.init()
from pyspark.context import SparkContext

sc = SparkContext(appName='test')
lines = sc.textFile('hdfs://zjkb-cpc-backend-bigdata-qa-01:8020/user/hive/warehouse/dl_cpc.db/cpc_basedata_union_events/'
                    'day=2020-09-16/hour=18/minute=42/part-00000-ac7b67dc-39ef-43f3-9a52-d7169d698208-c000.snappy.parquet')
print(lines.count())
for line in lines.collect():
    print(line)
sc.stop()


