import pytest
from utils.shared_spark_session_helper import SharedSparkSessionHelper


class TestLocalHiveJob(SharedSparkSessionHelper):
    __DATABASE = 'testing'
    __TABLE = 'example'

    @classmethod
    def spark_conf(cls):
        conf = super().spark_conf()
        # 目前本地的数据库配置是这样的, 记住derby同时只能有1个实例
        return conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic') \
            .set('spark.sql.warehouse.dir', '/Users/admin/Documents/hive/spark-warehouse') \
            .set('javax.jdo.option.ConnectionURL',
                 'jdbc:derby:;databaseName=/Users/admin/Documents/hive/metastore_db;create=true')

    def test_query_table(self):
        self.spark_session.sql('show databases').show()
        self.spark_session.sql("show tables in test").show()
        self.spark_session.sql('select * from default.teacher').show()
        self.spark_session.sql('select * from test.test').show()
