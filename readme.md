# MySpark Project

After doing so many courses and trying so many resources to learn spark and medallion architecture, I decided to build my own project and do my own scripts to learn that the best way I can.

I did add some of my own ideas, usually people dont do SCD 2 on spark, but I thought it would add some flash to it.

## 1 - Creating docker network (mandatory)
docker network create lakehouse_net

## 2 - Dowload and run docker for mysql, spark e notebook
cd <spark and mysql folders> 
docker compose up -d

## 3 - Test spark connection to mysql
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/test_mysql.py

any errors here, are likely due to the lack of network 
check step 1 and and script credentials

## 4 - To run jobs
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/bronze_tbcategories.py
docker exec -it spark_master spark-submit --master local[*] /opt/spark/jobs/silver_tbcategories.py
docker exec -it spark_master spark-submit --master local[*] /opt/spark/jobs/gold_dim_categories_scd2.py
same for the others...

## 5 - To run notebooks (under work folder)
http://localhost:8888
