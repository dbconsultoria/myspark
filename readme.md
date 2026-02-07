# Criando a rede (obrigatório)
docker network create lakehouse_net

# Baixar a imagem e rodar o mysql, spark e notebook
cd <folder> 
docker compose up -d

# Testar a conexão com mysql
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/test_mysql.py

# Para rodar os jobs
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/bronze_tbcategories.py

# Acessar os notebooks
http://localhost:8888
