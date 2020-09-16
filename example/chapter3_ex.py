from utils.shared_spark_session_helper import SharedSparkSessionHelper
from pyspark import RDD


class TestChapter3Job(SharedSparkSessionHelper):
    @classmethod
    def spark_conf(cls):
        conf = super().spark_conf()
        return conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')

    def test_ex_3_14(self, datafiles):
        destination_path = self.path / 'a.txt'
        with open(destination_path, "w") as testFile:
            _ = testFile.write("Hello\nnihao\nWorld!")
        textFile = self.spark_context.textFile(str('file://' / destination_path))
        rdd = self.spark_context.parallelize(["World!", "Hello", "Niubi"])
        assert sorted(self.spark_context.union([textFile, rdd]).distinct().collect()) == ['Hello', 'Niubi',
                                                                                          'World!',
                                                                                          'nihao']

    def test_ex_3_15(self):
        rdd = self.spark_context.parallelize(list(range(100)))
        for num in rdd.take(10):
            print(num)

    def test_ex_3_20(self):
        rdd = self.spark_context.parallelize(list(range(20)))
        for num in rdd.filter(lambda num: num % 2 == 0).take(20):
            print(num)

    def test_ex_3_31(self):
        rdd1 = self.spark_context.parallelize([1, 2, 3])
        rdd2 = self.spark_context.parallelize(["a", "b"])
        rdd = rdd1.cartesian(rdd2)
        for ele in rdd.collect():
            print(ele)

    def test_ex_3_32(self):
        sum = self.spark_context.parallelize([1, 2, 3]).reduce(lambda x, y: x + y)
        assert sum == 6

    def test_avg(self):
        rdd = self.spark_context.parallelize([1, 2, 3, 4, 5, 6])
        value = rdd.map(lambda x: (x, 1)).reduce(lambda e1, e2: (e1[0] + e2[0], e1[1] + e2[1]))
        print(value, value[0] / value[1])

    def test_ex_3_35(self):
        rdd: RDD = self.spark_context.parallelize([1, 2, 3, 4, 5, 6])
        acc = rdd.aggregate((0, 0),
                            lambda acc, value: (acc[0] + value, acc[1] + 1),
                            lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
                            )
        print(acc, acc[0] / acc[1])
