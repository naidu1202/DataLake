"""
Extract data for DAE team
"""
# pylint:disable=C0301
import os
import yaml
import pendulum
from datetime import timedelta, datetime
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from sg_data.lib.utils.slack_lib import (
    dag_success_slack_alert,
    dag_sla_miss_slack_alert,
)
from sg_data.lib.utils.opsgenie_lib import dag_fail_alert


DAG_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
DAG_BASE_NAME = "learning-examples"
env = Variable.get("env", "dev")
cert_file_path = file_path = f"{DAG_DIR_PATH}/../../keys/email_api_ca_cer_{env}.pem"

with open(f"{DAG_DIR_PATH}/config.yaml") as infile:
    config_list = yaml.safe_load(infile)
for config in config_list:
    extract_name = config["name"]
    DAG_NAME = "test_email_new2"
    default_args = {
        "owner": "sg-data-team",
        "depends_on_past": False,
        "start_date": days_ago(2)
        if "start_date" not in config
        else datetime.fromisoformat(config["start_date"]),
        "sla": timedelta(hours=1),
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
    role: str = "SG_ETL" if "role" not in config else config["role"]
    database: str = "SINGLIFE" if "database" not in config else config["database"]
    schema: str = "LANDING" if "schema" not in config else config["schema"]
    warehouse: str = "SG_ETL" if "warehouse" not in config else config["warehouse"]
    with open(f"{DAG_DIR_PATH}/sql/{extract_name}.sql", encoding="utf-8") as infile:
        sql_text = infile.read().split(";\n")
    with DAG(
        DAG_NAME,
        description=f'Extract data for "{extract_name}"',
        schedule_interval=config["schedule_interval"],
        default_args=default_args,
        max_active_runs=1,
        concurrency=2,        
    ) as dag:


        task_run_job = AwsGlueJobOperator(
                    dag=dag,
                    task_id="run_job",
                    aws_conn_id="<<Aws connection>>",
                    job_name="assessment",
                    region_name="<<region>>",                    
                )


        snowflake_to_s3 = SnowflakeOperator(
            dag=dag,
            task_id=f"extract_from_snowflake_{extract_name}",
            sql=sql_text,
            snowflake_conn_id="<<snowflake Conn>>",
            autocommit=False,
            role=role,
            database=database,
            schema=schema,
            warehouse=warehouse,
        )

        task_run_job >> snowflake_to_s3

        globals()[DAG_NAME] = dag
