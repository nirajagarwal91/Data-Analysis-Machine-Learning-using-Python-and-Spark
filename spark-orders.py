from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Sales")

sc = SparkContext(conf = conf)

sale = sc.textFile('/home/niraj/Downloads/customer-orders.csv')

def parseData(line):

    row = line.split(',')

    cust_id = int(row[0])

    amount = float(row[2])

    return (cust_id, amount)

rdd = sale.map(parseData)

sale_total = rdd.reduceByKey(lambda x,y: round(x+y))

sale_total.top(15)