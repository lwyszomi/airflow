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
Example Airflow DAG that show how to use various Dataproc Metastore
operators to manage a service.
"""

import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreUpdateServiceOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreRestoreServiceOperator,
    DataprocMetastoreDeleteServiceOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "default-project-id")
SERVICE_ID = os.environ.get("GCP_DATAPROC_METASTORE_SERVICE_ID", "example-service-9a25")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west1-b")
BUCKET = os.environ.get("GCP_DATAPROC_METASTORE_BUCKET", "dataproc-metastore-system-tests")

# Service definition.
# Docs: https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services#Service
# [START how_to_cloud_dataproc_metastore_create_service]

SERVICE = {
  "name": "test-service",
}

# [END how_to_cloud_dataproc_metastore_create_service]


with models.DAG("example_gcp_dataproc_metastore", start_date=days_ago(1), schedule_interval="@once") as dag:
    # [START how_to_cloud_dataproc_metastore_create_service_operator]
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        location_id=REGION,
        project_number=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_create_service_operator]

    update_service = DataprocMetastoreUpdateServiceOperator()

    # [START how_to_cloud_dataproc_metastore_get_service_operator]
    get_service_details = DataprocMetastoreGetServiceOperator(
        task_id="get_service",
        location_id=REGION,
        project_number=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_get_service_operator]

    import_metadata = DataprocMetastoreCreateMetadataImportOperator()

    export_metadata = DataprocMetastoreExportMetadataOperator()

    backup_service = DataprocMetastoreCreateBackupOperator()

    lists_backups = DataprocMetastoreListBackupsOperator()
    
    delete_backup = DataprocMetastoreDeleteBackupOperator()

    restore_service = DataprocMetastoreRestoreServiceOperator()

    # [START how_to_cloud_dataproc_metastore_delete_service_operator]
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        location_id=REGION,
        project_number=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_delete_service_operator]


    create_service
    create_service >> update_service >> delete_service
    create_service >> get_service_details >> delete_service
    create_service >> import_metadata >> delete_service
    create_service >> export_metadata >> delete_service
    create_service >> backup_service >> delete_service
    create_service >> lists_backups >> delete_service
    create_service >> delete_backup >> delete_service
    create_service >> restore_service >> delete_service

