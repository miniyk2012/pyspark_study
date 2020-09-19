import json

import io, csv
from utils.shared_spark_session_helper import SharedSparkSessionHelper
from pyspark import RDD


class TestChapter5Job(SharedSparkSessionHelper):
    @classmethod
    def spark_conf(cls):
        conf = super().spark_conf()
        conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')\
            .set('hive.metastore.uris', 'thrift://zjkb-cpc-backend-bigdata-qa-05:9083')
        return conf

    def test_ex5_3(self):
        rdd: RDD = self.spark_context.textFile(
            "file:///Users/admin/Documents/pythonprojects/pyspark_study/files/fake_logs")
        for line in rdd.take(10):
            print(line)

    def test_ex5_4(self):
        input: RDD = self.spark_context.wholeTextFiles(
            "file:///Users/admin/Documents/pythonprojects/pyspark_study/files/fake_logs")

        def avg_word_length(content):
            """每个文件中单词的平均长度"""
            words = content.split()
            return sum(len(word) for word in words) / len(words)

        result = input.mapValues(avg_word_length)
        for e in result.collect():
            print(e)

    def test_ex5_9(self):
        """读取json文件, 处理后写入json"""
        input = self.spark_context.textFile(
            "file:///Users/admin/Documents/pythonprojects/pyspark_study/files/pandainfo.json")
        data: RDD = input.map(lambda x: json.loads(x))
        data.cache()
        for value in data.collect():
            print(value)
        outputFile = "file:///Users/admin/Documents/pythonprojects/pyspark_study/files/lovePandasInfo.json"
        data.filter(lambda x: 'lovesPandas' in x and x['lovesPandas']).map(
            lambda x: json.dumps(x)).saveAsTextFile(outputFile)

    def test_ex5_14(self):
        input = self.spark_context.textFile(
            "file:///Users/admin/Documents/pythonprojects/pyspark_study/files/favourite_animals.csv")
        def loadRecord(line):
            input = io.StringIO(line)
            reader = csv.DictReader(input, fieldnames=['name', 'favouriteAnimal'])
            return next(reader)
        data = input.map(loadRecord)
        for record in data.collect():
            print(record['name'], record['favouriteAnimal'])
        pandaLovers = data.filter(lambda x: x['favouriteAnimal'] == "panda")
        def writeRecords(records):
            """对该分区内的所有记录转化为字符串"""
            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=['name', 'favouriteAnimal'])
            for record in records:
                writer.writerow(record)
            print('writeRecords', output.getvalue())
            return [output.getvalue()]

        for line in data.mapPartitions(writeRecords).collect():
            print('final', line)

    def test_ex5_15(self):
        def loadRecords(fileNameContents):
            """Load all the records in a given file"""
            input = io.StringIO(fileNameContents[1])
            reader = csv.DictReader(input, fieldnames=["name", "favouriteAnimal"])
            return reader  # reader是可迭代对象, 因此等效于list(reader)

        fullFileData = self.spark_context\
            .wholeTextFiles("file:///Users/admin/Documents/pythonprojects/pyspark_study/files/favourite_animals.csv")\
            .flatMap(loadRecords)
        fullFileData.cache()
        for e in fullFileData.collect():
            print(e)


    def test_hive(self):
        df = self.spark_session.read.table("dl_cpc.cpc_basedata_adx_event")
        df.show(truncate=False)

    def test_ex5_30(self):
        rows = self.spark_session.sql("""
        select searchid, create_source,day,hour,minute timestamp,day from dl_cpc.cpc_basedata_adx_event where day='2020-09-16' 
        and  hour=18 
        limit 10
        """)
        for row in rows.take(10):
            print(row)
            # print(row.searchid, row.create_source)




