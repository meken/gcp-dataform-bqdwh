import os

from datetime import datetime

from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
)


DAG_ID = "etl-flow"

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
REGION = os.getenv("GCP_REGION")

REPOSITORY_ID = os.getenv("REPOSITORY_ID")
GIT_REF = os.getenv("GIT_REF")

INFERENCE_QUERY = """
CREATE OR REPLACE TABLE dwh.churn_predictions AS 
SELECT
  business_entity_id as customer_id,
  predicted_churned,
  predicted_churned_probs
FROM
ML.PREDICT (
  MODEL `dwh.churn_model`,
  (
    SELECT * FROM curated.stg_person
  )
)
"""


with models.DAG(
    DAG_ID,
    schedule_interval='@once', 
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dataform'],
) as dag:

    prep_landing_bucket_op = EmptyOperator(task_id="sources_to_landing")

    df_compilation_op = DataformCreateCompilationResultOperator(
        task_id="compile_dataform_workflow",
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        compilation_result={
            "git_commitish": GIT_REF,
            "code_compilation_config": { "default_database": PROJECT_ID}
        },
    )

    df_invocation_op = DataformCreateWorkflowInvocationOperator(
        task_id='run_dataform_workflow',
        project_id=PROJECT_ID,
        region=REGION,
        repository_id=REPOSITORY_ID,
        workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('compile_dataform_workflow')['name'] }}"
        },
    )

    bq_inference_op = BigQueryInsertJobOperator(
        task_id="customer_churn_inferencing",
        location=REGION,
        configuration={
            "query": {
                "query": INFERENCE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

prep_landing_bucket_op >> df_compilation_op >> df_invocation_op >> bq_inference_op