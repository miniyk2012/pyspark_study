import bisect
import json
import re
import urllib3

from utils.shared_spark_session_helper import SharedSparkSessionHelper


class TestChapter6Job(SharedSparkSessionHelper):
    def setup_method(self):
        super(TestChapter6Job, self).setup_method()
        self.outputDir = 'files/output'
        self.inputFile = 'files/callsigns'
        self.validSignCount = self.spark_context.accumulator(0)
        self.partitionCount = self.spark_context.accumulator(0)

    # ------------------ helper function ---------------------------
    def lookupCountry(self, sign, prefixes):
        pos = bisect.bisect_left(prefixes, sign)
        return prefixes[pos].split(",")[1]

    def processSignCount_with_broadcast(self, sign_count, signPrefixes):
        country = self.lookupCountry(sign_count[0], signPrefixes.value)
        count = sign_count[1]
        return (country, count)

    def loadCallSignTable(self):
        f = open("./files/callsign_tbl_sorted", "r")
        return f.readlines()

    def extractCallSigns(self, line):
        return line.split(" ")

    def validateSign(self, sign):
        if re.match(r"\A\d?[a-zA-Z]{1,2}\d{1,4}[a-zA-Z]{1,3}\Z", sign):
            self.validSignCount += 1
            return True
        else:
            self.invalidSignCount += 1
            return False

    def processCallSigns(self, signs):
        """Lookup call signs using a connection pool"""
        # Create a connection pool
        self.partitionCount += 1
        http = urllib3.PoolManager()
        # the URL associated with each call sign record
        signs = list(signs)
        # print('partition:', signs)
        urls = map(lambda x: "http://73s.com/qsos/%s.json" % x, signs)
        urls = list(urls)
        # print('urls: ', urls)
        # create the requests (non-blocking)
        requests = map(lambda x: (x, http.request('GET', x)), urls)
        # fetch the results
        result = map(lambda x: (x[0], json.loads(x[1].data)), requests)
        # remove any empty results and return
        return filter(lambda x: x[1] is not None, result)

    # --------------- helper function ----------------------------------

    def test_with_broadcast(self):
        file = self.spark_context.textFile(self.inputFile)
        callSigns = file.flatMap(self.extractCallSigns)
        validSigns = callSigns.filter(self.validateSign)
        contactCounts = validSigns.map(
            lambda sign: (sign, 1)).reduceByKey((lambda x, y: x + y))
        # Force evaluation so the counters are populated
        contactCounts.count()
        print('invalidSignCount', self.invalidSignCount.value)
        print('validSignCount', self.validSignCount.value)
        for v in contactCounts.collect():
            print(v)

        # 国家表
        signPrefixes = self.spark_context.broadcast(self.loadCallSignTable())
        print()
        countryContactCounts = (contactCounts
                                .map(lambda signCount: self.processSignCount_with_broadcast(signCount, signPrefixes))
                                .reduceByKey((lambda x, y: x + y)))
        for v in countryContactCounts.collect():
            print(v)
        countryContactCounts.saveAsTextFile(self.outputDir + '/countries.txt')

    def test_6_14(self):
        def partitionCtr(nums):
            self.partitionCount += 1
            ret = [0, 0]
            for num in nums:
                ret[0] += num
                ret[1] += 1
            return [ret]

        rdd = self.spark_context.parallelize(range(100))
        print("getNumPartitions: ", rdd.getNumPartitions())
        sum_count = rdd.mapPartitions(partitionCtr).reduce(lambda acc, val: (acc[0] + val[0], acc[1] + val[1]))
        print(sum_count)
        print('partitionCount: ', self.partitionCount.value)
        print('average is ' + str(sum_count[0] / sum_count[1]))

    def test_6_10(self):
        file = self.spark_context.textFile(self.inputFile)
        callSigns = file.flatMap(self.extractCallSigns)
        validSigns = callSigns.filter(self.validateSign)
        contactsContactList = validSigns.mapPartitions(lambda callSigns: self.processCallSigns(callSigns))
        print("getNumPartitions: ", contactsContactList.getNumPartitions())
        contactsContactList.foreach(lambda x: print(x))
        print('partitionCount: ', self.partitionCount.value)
