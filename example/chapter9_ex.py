import pytest
from utils.shared_spark_session_helper import SharedSparkSessionHelper

from pyspark.sql import SparkSession
from pyspark.sql import Row


class TestChapter5Job(SharedSparkSessionHelper):
    __DATABASE = 'testing'
    __TABLE = 'example'

    @pytest.fixture
    def prepare_hive_data(self):
        json_path = '/Users/admin/Documents/pythonprojects/pyspark_study/files/awesome.json'
        data_frame = self.spark_session.read.json(path=json_path)
        self.spark_session.sql(f'CREATE DATABASE IF NOT EXISTS {self.__DATABASE}')
        self.spark_session.sql(
            f'CREATE TABLE IF NOT EXISTS `{self.__DATABASE}`.`{self.__TABLE}` (`NAME` STRING, `SURNAME` STRING) '
            f'STORED AS PARQUET'
        )
        data_frame.printSchema()
        data_frame.write \
            .mode('overwrite') \
            .insertInto(f'{self.__DATABASE}.{self.__TABLE}')
        for row in data_frame.collect():
            print(row)
        # 获知数据存储在哪里的
        result = self.spark_session.sql(f'describe extended {self.__DATABASE}.{self.__TABLE}')
        for value in result.collect():
            print(value)

    def test_ex9_15(self, prepare_hive_data):
        sqlDF = self.spark_session.sql(f'select * from {self.__DATABASE}.{self.__TABLE}')
        sqlDF.show()

    def test_ex9_21(self):
        input = self.spark_session.read.json('/Users/admin/Documents/pythonprojects/pyspark_study/files/pandainfo.json')
        input.printSchema()
        input.registerTempTable('pandainfo')
        results = self.spark_session.sql('select * from pandainfo')
        results.show()

        for x in results.rdd.filter(lambda row: row.knows is not None).map(lambda row: row.knows.friends[0]).collect():
            print(x)


