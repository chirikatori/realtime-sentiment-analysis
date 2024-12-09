## Run Locally  
Clone the project  

~~~bash  
  git clone https://github.com/chirikatori/realtime-sentiment-analysis.git
~~~

Go to the project directory  

~~~bash  
  cd realtime-sentiment-analysis
~~~

Install dependencies  

~~~bash  
conda env create -f environment.yml
~~~

~~~bash  
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
~~~

Run project
~~~bash
conda activate wsentiment
~~~

~~~bash
spark-submit --packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 --conf spark.pyspark.python=$(which python) main.py
~~~
