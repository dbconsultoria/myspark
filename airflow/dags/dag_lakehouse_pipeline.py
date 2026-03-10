from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

JOBS_PATH = "/opt/spark/jobs"
PACKAGES = "com.mysql:mysql-connector-j:8.0.33"

def spark_submit(job_file):
    return (
        f"docker exec spark_master spark-submit "
        f"--master local[*] "
        f"--packages {PACKAGES} "
        f"{JOBS_PATH}/{job_file}"
    )

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    description="Pipeline Bronze → Silver → Gold",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["lakehouse", "bronze", "silver", "gold"],
) as dag:

    # ─────────────────────────────────────────
    # BRONZE
    # ─────────────────────────────────────────
    bronze_categories = BashOperator(
        task_id="bronze_tbcategories",
        bash_command=spark_submit("bronze_tbcategories.py"),
    )

    bronze_customers = BashOperator(
        task_id="bronze_tbcustomers",
        bash_command=spark_submit("bronze_tbcustomers.py"),
    )

    bronze_products = BashOperator(
        task_id="bronze_tbproducts",
        bash_command=spark_submit("bronze_tbproducts.py"),
    )

    bronze_orders = BashOperator(
        task_id="bronze_tborders",
        bash_command=spark_submit("bronze_tborders.py"),
    )

    bronze_orderdetail = BashOperator(
        task_id="bronze_tborderdetail",
        bash_command=spark_submit("bronze_tborderdetail.py"),
    )

    # ─────────────────────────────────────────
    # SILVER
    # ─────────────────────────────────────────
    silver_categories = BashOperator(
        task_id="silver_tbcategories",
        bash_command=spark_submit("silver_tbcategories.py"),
    )

    silver_customers = BashOperator(
        task_id="silver_tbcustomers",
        bash_command=spark_submit("silver_tbcustomers.py"),
    )

    silver_products = BashOperator(
        task_id="silver_tbproducts",
        bash_command=spark_submit("silver_tbproducts.py"),
    )

    silver_orders = BashOperator(
        task_id="silver_tborders",
        bash_command=spark_submit("silver_tborders.py"),
    )

    silver_orderdetail = BashOperator(
        task_id="silver_tborderdetail",
        bash_command=spark_submit("silver_tborderdetail.py"),
    )

    # ─────────────────────────────────────────
    # GOLD
    # ─────────────────────────────────────────
    gold_dim_categories = BashOperator(
        task_id="gold_dim_categories_scd2",
        bash_command=spark_submit("gold_dim_categories_scd2.py"),
    )

    gold_dim_customers = BashOperator(
        task_id="gold_dim_customers_scd2",
        bash_command=spark_submit("gold_dim_customers_scd2.py"),
    )

    gold_dim_products = BashOperator(
        task_id="gold_dim_products_scd2",
        bash_command=spark_submit("gold_dim_products_scd2.py"),
    )

    gold_fact_orders = BashOperator(
        task_id="gold_fact_orders",
        bash_command=spark_submit("gold_fact_orders.py"),
    )

    # ─────────────────────────────────────────
    # DEPENDÊNCIAS
    # ─────────────────────────────────────────
    bronze_categories  >> silver_categories  >> gold_dim_categories
    bronze_customers   >> silver_customers   >> gold_dim_customers
    bronze_products    >> silver_products    >> gold_dim_products
    bronze_orders      >> silver_orders
    bronze_orderdetail >> silver_orderdetail

    [silver_orders, silver_orderdetail,
     gold_dim_categories, gold_dim_customers, gold_dim_products] >> gold_fact_orders