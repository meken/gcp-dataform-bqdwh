import os

from datetime import datetime

from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow import models
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)
from airflow.providers.google.cloud.sensors.dataform import DataformWorkflowInvocationStateSensor

DAG_ID = "etl-flow"
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
REGION = os.getenv("GCP_REGION")
REPOSITORY_ID = os.getenv("REPOSITORY_ID")
GIT_REF = os.getenv("GIT_REF")

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

    bq_inference_op = EmptyOperator(task_id="customer_churn_inferencing")

prep_landing_bucket_op >> df_compilation_op >> df_invocation_op >> bq_inference_op