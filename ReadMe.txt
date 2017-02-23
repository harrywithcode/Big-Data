1. put the jar file under your work space in Linux file system
2. run following commands for each question
hadoop jar yelp.jar <java class name> <csv file address> <hadoop output address>

Q1: 
hadoop jar yelp.jar CountYelpBusiness hdfs://cshadoop1/user/bxc151130/yelp/business/business.csv hdfs://cshadoop1/user/bxc151130/assignment2q1
To check the result,type command:
hdfs dfs -cat assignment2q1/part-r-00000

Q2:
hadoop jar yelp.jar CountYelpBusinessQ2 hdfs://cshadoop1/user/bxc151130/yelp/business/business.csv hdfs://cshadoop1/user/bxc151130/assignment2q2
To check the result,type command:
hdfs dfs -cat assignment2q2/part-r-00000

Q3:
hadoop jar yelp.jar TopNQ3 hdfs://cshadoop1/user/bxc151130/yelp/business/business.csv hdfs://cshadoop1/user/bxc151130/assignment2q3
To check the result,type command:
hdfs dfs -cat assignment2q3/part-r-00000

Q4:
hadoop jar yelp.jar CountYelpReviewQ4 hdfs://cshadoop1/user/bxc151130/yelp/review/review.csv hdfs://cshadoop1/user/bxc151130/assignment2q4
To check the result,type command:
hdfs dfs -cat assignment2q4/part-r-00000

Q5:
hadoop jar yelp.jar CountYelpReviewQ5 hdfs://cshadoop1/user/bxc151130/yelp/review/review.csv hdfs://cshadoop1/user/bxc151130/assignment2q5
To check the result,type command:
hdfs dfs -cat assignment2q5/part-r-00000