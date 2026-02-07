# Criando a rede
docker network create lakehouse_net

# Baixar a imagem e rodar o mysql
cd mysql 
docker compose up -d

# Baixar a imagem e rodar o spark
cd spark
docker compose -f docker-compose.yml up -d

# Testar a conexão com mysql
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/test_mysql.py

# Para rodar os jobs
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/bronze_tbcategories.py
