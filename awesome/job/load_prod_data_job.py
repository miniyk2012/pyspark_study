import findspark

findspark.init()

import json
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, FloatType, BooleanType, \
    DoubleType, ShortType

type_map = {
    'int': IntegerType(),
    'bigint': LongType(),
    'smallint': ShortType(),
    'float': FloatType(),
    'double': DoubleType(),
    'string': StringType(),
    'boolean': BooleanType(),
}


class LoadProdDataJob:

    def __init__(self, database, target_table, file_path, partition_columns, dfs_path, schema_path):
        self.database = database
        self.target_table = target_table
        self.file_path = file_path
        self.partition_columns = partition_columns
        self.dfs_path = dfs_path
        self.schema_path = schema_path
        self.spark_session = SparkSession \
            .builder \
            .appName('load-prod-data') \
            .enableHiveSupport() \
            .config(conf=self.spark_cdh_conf()) \
            .getOrCreate()

    @classmethod
    def spark_local_conf(cls):
        return SparkConf() \
            .set('spark.unsafe.exceptionOnMemoryLeak', 'true') \
            .set('spark.ui.enabled', 'false') \
            .set('hive.stats.jdbc.timeout', '80') \
            .set('spark.sql.session.timeZone', 'UTC') \
            .set('spark.sql.warehouse.dir', '/Users/admin/Documents/hive/spark-warehouse') \
            .set('javax.jdo.option.ConnectionURL',
                 'jdbc:derby:;databaseName=/Users/admin/Documents/hive/metastore_db;create=true') \
            .set('spark.debug.maxToStringFields', 1000) \
            .set("hive.exec.dynamici.partition", True) \
            .set('hive.exec.dynamic.partition.mode', 'nonstrict')

    @classmethod
    def spark_cdh_conf(cls):
        os.environ['HADOOP_USER_NAME'] = 'root'  # root才有权限保存parquet文件
        return SparkConf() \
            .set('spark.unsafe.exceptionOnMemoryLeak', 'true') \
            .set('spark.ui.enabled', 'false') \
            .set('hive.stats.jdbc.timeout', '80') \
            .set('spark.sql.session.timeZone', 'UTC') \
            .set('spark.debug.maxToStringFields', 1000) \
            .set("hive.exec.dynamici.partition", True) \
            .set('hive.metastore.uris', 'thrift://zjkb-cpc-backend-bigdata-qa-05:9083') \
            .set('hive.exec.dynamic.partition.mode', 'nonstrict')

    def get_schema(self, schema_file):
        with open(schema_file, 'rt') as f:
            json_schema = json.load(f)
        struct_types = []
        for field, type_ in json_schema.items():
            struct_types.append(StructField(field, type_map[type_.lower()], True))
        return StructType(struct_types)

    def run(self):
        schema = self.get_schema(self.schema_path)
        tmpdf = self.spark_session.read.csv(self.file_path, header=True, schema=schema)
        tmpdf.show()
        tmpdf.write.partitionBy(*self.partition_columns).mode('overwrite').parquet(self.dfs_path)
        self.spark_session.sql(f'msck repair table {self.database}.{self.target_table}')


if __name__ == '__main__':
    file_path = '/Users/admin/Documents/pythonprojects/pyspark_study/files/load_prod_data/cpc_bd_sdk_show_v1.csv'
    dfs_file_path = 'hdfs://zjkb-cpc-backend-bigdata-qa-01:8020/user/hive/warehouse/dl_cpc.db/cpc_basedata_bidsdk_event/event_type=show'
    schema_path = '/Users/admin/Documents/pythonprojects/pyspark_study/files/load_prod_data/cpc_bd_sdk_show_v1_schema.json'
    job = LoadProdDataJob('dl_cpc',
                          'cpc_bd_sdk_show_v1',
                          file_path,
                          ['day', 'hour', 'mm'],
                          dfs_file_path,
                          schema_path)
    job.run()
