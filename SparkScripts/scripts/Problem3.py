from pyspark import SparkContext
import sys

def toCSVLine(data):
  return "\t".join(str(d) for d in data)

if __name__ == "__main__":
    sc = SparkContext(appName="Problem3")
    graph_raw = sc.textFile(sys.argv[1] + sys.argv[2])
    header = graph_raw.first()
    graph_data = graph_raw.filter(lambda row: row != header)\
                            .map(lambda line: line.split("\t"))\
                            .map(lambda record: (record[1],record[2]))\
                            .reduceByKey(lambda x,y: int(x)+int(y))\
                            .sortByKey(True).map(toCSVLine)
    graph_data.saveAsTextFile(sys.argv[1] + sys.argv[3])


