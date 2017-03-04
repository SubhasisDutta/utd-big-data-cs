1. Copy the input files to HDFS from the local directory in which input files are present
	$ hdfs dfs -mkdir input1
	$ hdfs dfs -put business.csv input1/business.csv
	$ hdfs dfs -put review.csv input1/review.csv
	$ hdfs dfs -put user.csv input1/user.csv

2. Go to the directory in which the jars are present
3. Run the JAR for Problem 1
	$ hadoop jar Problem1.jar input1/business.csv output1
	$ hdfs dfs -cat output1/*
3. Run the JAR for Problem 2
	$ hadoop jar Problem2.jar input1/review.csv output2
	$ hdfs dfs -cat output2/*
4. Run the JAR for Problem 3
	$ hadoop jar Problem3.jar input1/review.csv input1/business.csv output3
	$ hdfs dfs -cat output3/*
5. Run the JAR for Problem 4
	$ hadoop jar Problem4.jar input1/review.csv input1/business.csv Stanford output4
	$ hdfs dfs -cat output4/*
