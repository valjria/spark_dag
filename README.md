# spark_dag
airflow dag example with spark and multiple tasks pipeline.  
TASK 1: Downloads the dataset \n  

TASK 2: Check if the dataset is installed correctly to the path.  

TASK 3: Run the specified python file to clean the data (Deletes elements that are not alphanumeric, casts data types makes the date structured).  

TASK 4: Create the table on postgreSQL using the specifies SQL file.  

TASK 5: Insert cleaned records into table except the columns you don't want user to see.  

This DAG runs in 5 minute intervals.

