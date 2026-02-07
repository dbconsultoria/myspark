# 1 - Criando a rede (obrigatório)
docker network create lakehouse_net

# 2 - Baixar a imagem e rodar o mysql, spark e notebook
cd <em cada pasta> 
docker compose up -d

# Testar a conexão com mysql
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/test_mysql.py

se existe erro aqui, provavelmente é erro de rede entre o spark e mysql. verifique o passo 1 ou verifique as credenciais no script python

# Para rodar os jobs
docker exec -it spark_master spark-submit --master local[*] --packages com.mysql:mysql-connector-j:8.0.33 /opt/spark/jobs/bronze_tbcategories.py

# Acessar os notebooks
http://localhost:8888
