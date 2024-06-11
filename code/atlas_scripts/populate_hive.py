import monitoring_oracle
import os
import pandas

for table in os.listdir("/opt/airflow/dags/data/data"):
    monitoring_oracle.csv_to_hive(f"/opt/airflow/dags/data/data/{table}")