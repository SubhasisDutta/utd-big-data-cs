
1.Copy the input files to HDFS from the local directory in which input files are present
```
$ hdfs dfs -mkdir input3
$ hdfs dfs -put business.csv input3/business.csv
$ hdfs dfs -put review.csv input3/review.csv
$ hdfs dfs -put user.csv input3/user.csv
$ hdfs dfs -put graph.csv input3/graph.csv
```

2.Go to Scripts folder and run the following commands:
```
$ hdfs dfs -rm -r spark_output1
$ spark-submit Problem1.py hdfs://localhost:54310/user/subhasis/ input3/business.csv input3/review.csv Stanford spark_output1
$ hdfs dfs -cat spark_output1/*
```

3.Go to Scripts folder and run the following commands:
```
$ hdfs dfs -rm -r spark_output2
$ spark-submit Problem2.py hdfs://localhost:54310/user/subhasis/ input3/business.csv input3/review.csv spark_output2
$ hdfs dfs -cat spark_output2/*
```

4.Go to Scripts folder and run the following commands:
```
$ hdfs dfs -rm -r spark_output3
$ spark-submit Problem3.py hdfs://localhost:54310/user/subhasis/ input3/graph.csv spark_output3
$ hdfs dfs -cat spark_output3/*
```

spark-submit Problem3.py ../data/graph.csv output.txt
