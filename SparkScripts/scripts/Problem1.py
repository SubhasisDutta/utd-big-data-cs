from pyspark import SparkContext
import sys

def toCSVLine(data):
  return "\t".join(str(d) for d in data)

if __name__ == "__main__":
    sc = SparkContext(appName="Problem1")
    business_raw = sc.textFile(sys.argv[1]+sys.argv[2])
    review_raw = sc.textFile(sys.argv[1]+sys.argv[3])
    filter_string = sys.argv[4].lower() + ", "

    filtered_business = business_raw.map(lambda line: line.split("::"))\
                                    .filter(lambda record: filter_string in record[1].lower())\
                                    .map(lambda record: record[0]).collect()

    projected_reviews = review_raw.map(lambda line: line.split("::"))\
                                  .filter(lambda record: record[2] in filtered_business)\
                                  .map(lambda record: (record[1],record[3])).map(toCSVLine)
    projected_reviews.saveAsTextFile(sys.argv[1]+sys.argv[5])

    # print "User ID                    Rating"
    # for record in projected_reviews:
    #     print record[0] + "     " + str(record[1])



