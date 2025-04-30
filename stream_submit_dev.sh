sudo -u hdfs spark-submit \
  --master yarn \
  --deploy-mode client \
  --jars /opt/cloudera/parcels/CDH-7.1.7-1.cdh7.1.7.p0.15945976/jars/kudu-spark2_2.11-1.15.0.7.1.7.0-551.jar \
  /home/aditya/cloudera/cdc_kafka_spark/cdc_kafka_spark_dev_3.py
