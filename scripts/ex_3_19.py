import findspark
findspark.init()
from pyspark.context import SparkContext

class SearchFunctions(object):
    def __init__(self, query):
        self.query = query

    def isMatch(self, num):
        return num % self.query == 0

    def getMatchesFunction(self, rdd):
        query = self.query
        return rdd.filter(lambda x: x % query == 0)


if __name__ == '__main__':
    sc = SparkContext(appName='test')
    rdd = sc.parallelize(list(range(20)))
    search_func = SearchFunctions(3)
    for num in search_func.getMatchesFunction(rdd).take(10):
        print(num)
    sc.stop()
