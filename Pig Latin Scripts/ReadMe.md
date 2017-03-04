1. Copy the input files to HDFS from the local directory in which input files are present
	$ hdfs dfs -mkdir input2
	$ hdfs dfs -put business.csv input2/business.csv
	$ hdfs dfs -put review.csv input2/review.csv
	$ hdfs dfs -put user.csv input2/user.csv

2. Go to Scripts folder and run the following commands:
	$ hdfs dfs -rm -r pig_output1
	$ pig Problem1.pig
	$ hdfs dfs -cat pig_output1/*

3. Go to Scripts folder and run the following commands:
	$ hdfs dfs -rm -r pig_output2
	$ pig Problem2.pig
	$ hdfs dfs -cat pig_output2/*

4. Go to Scripts folder and run the following commands:
	$ hdfs dfs -rm -r pig_output3
	$ pig Problem3.pig
	$ hdfs dfs -cat pig_output3/*


5. Go to Scripts folder and run the following commands:
	$ hdfs dfs -rm -r pig_output4
	$ pig Problem4.pig
	$ hdfs dfs -cat pig_output4/*
