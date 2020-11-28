from pyspark import SparkConf, SparkContext

appName = 'testSpark'

def main(sc):
    pass

if __name__ == '__main__':
    #Configure Spark
    
    conf = SparkConf().setAppName(appName).setMaster('local[2]')
#     sc.stop()
    sc = SparkContext(conf=conf)
    
    print(sc.version)
    main(sc)