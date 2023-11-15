#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for testing Google Dataflow
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
)
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowCreateJobSnapshotOperator,
    DataflowListActiveJobsOperator,
    DataflowTemplatedJobStartOperator,
    DataflowUpdateJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "dataflow_job_snapshot"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
TABLE_NAME = "realtime"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"

LOCATION = "us-central1"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
    }
}


with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create_dataset", dataset_id=DATASET_NAME)

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=[
            {"name": "ride_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "point_idx", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "latitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "longitude", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "meter_reading", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "meter_increment", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "ride_status", "type": "STRING", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        time_partitioning={"type": "DAY", "field": "timestamp"},
    )

    create_streaming_job = DataflowTemplatedJobStartOperator(
        task_id="create_streaming_job",
        project_id=PROJECT_ID,
        template=f"gs://dataflow-templates-{LOCATION}/latest/PubSub_to_BigQuery",
        parameters={
            "inputTopic": "projects/pubsub-public-data/topics/taxirides-realtime",
            "outputTableSpec": f"{PROJECT_ID}:{DATASET_NAME}.{TABLE_NAME}",
        },
        location=LOCATION,
    )

    create_job_snapshot = DataflowCreateJobSnapshotOperator(
        task_id="create_job_snapshot",
        job_id="{{ task_instance.xcom_pull(task_ids='create_streaming_job') }}",
        project_id=PROJECT_ID,
        location=LOCATION,
    )

    update_job = DataflowUpdateJobOperator(
        task_id="update_job",
        job_id="test_job",
        update_mask="",
        updated_body={},
    )

    list_job = DataflowListActiveJobsOperator(task_id="list_job")

    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table",
        deletion_dataset_table=f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}",
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )
    delete_dataset.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        create_bucket
        >> create_dataset
        >> create_table
        >> create_streaming_job
        >> create_job_snapshot
        >> update_job
        >> list_job
        >> delete_table
        >> delete_dataset
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
