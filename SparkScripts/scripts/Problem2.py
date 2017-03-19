from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import functions as F
import sys

def toCSVLine(data):
  return "\t".join(str(d) for d in data)

if __name__ == "__main__":
    sc = SparkContext(appName="Problem2")
    sqlContext = SQLContext(sc)
    review_raw = sc.textFile(sys.argv[1]+sys.argv[3])
    #load the review businessID and rating , group by business id avg of rating take top 10 rated business
    review_project = review_raw.map(lambda line: line.split("::"))\
                               .map(lambda record: (record[2],float(record[3])))
    #print review_project.top(2)

    review_fields = [StructField('businessID', StringType(), True), StructField('rating', FloatType(), True)]
    review_schema = StructType(review_fields)

    review_df = sqlContext.createDataFrame(review_project, review_schema)
    avg_review = review_df.groupBy('businessID').agg(F.avg('rating').alias('Avg_rating')).orderBy(F.desc('Avg_rating')).collect()

    count = 0
    previous = 5.0
    top_N = 10
    buisness_topN = []
    for record in avg_review:
        if previous != record[1]:
            count +=1
            previous = record[1]
        if count == top_N:
            break
        buisness_topN.append(record)
    # join with business and get the details
    business_raw = sc.textFile(sys.argv[1] + sys.argv[2])
    business_project = business_raw.map(lambda line: line.split("::")) \
                                   .map(lambda record: (record[0], record[1], record[2]))
    business_fields = [StructField('business_id', StringType(), True), StructField('full_address', StringType(), True),
                       StructField('categories', StringType(), True)]
    business_schema = StructType(business_fields)

    business_df = sqlContext.createDataFrame(business_project, business_schema)

    business_topN_rdd = sc.parallelize(buisness_topN)

    buisness_topN_df = sqlContext.createDataFrame(business_topN_rdd)

    result = buisness_topN_df.join(business_df, buisness_topN_df.businessID == business_df.business_id, 'inner')\
                    .select(buisness_topN_df.businessID, business_df.full_address, business_df.categories, buisness_topN_df.Avg_rating).rdd

    flat_results = result.map(toCSVLine)
    flat_results.saveAsTextFile(sys.argv[1] + sys.argv[4])
