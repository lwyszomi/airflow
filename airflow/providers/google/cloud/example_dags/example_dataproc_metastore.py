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
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreRestoreServiceOperator,
    DataprocMetastoreUpdateServiceOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "airflow-system-tests-303516")
SERVICE_ID = os.environ.get("GCP_DATAPROC_METASTORE_SERVICE_ID", "wj-service-2")
BACKUP_ID = os.environ.get("GCP_DATAPROC_METASTORE_BACKUP_ID", "wj-backup-1")
METADATA_IMPORT_ID = os.environ.get("GCP_DATAPROC_METASTORE_METADATA_IMPORT_ID", "wj-metadata-import-1")
REGION = os.environ.get("GCP_LOCATION", "europe-west1")
ZONE = os.environ.get("GCP_REGION", "europe-west1-b")
BUCKET = os.environ.get("GCP_DATAPROC_METASTORE_BUCKET", "dataproc-metastore-system-tests")
# os.environ["GRPC_DNS_RESOLVER"] = "native"
# os.environ["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] = "True"
METADATA_IMPORT_FILE = os.environ.get("GCS_METADATA_IMPORT_FILE", None)
# Service definition.
# Docs: https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services#Service
# [START how_to_cloud_dataproc_metastore_create_service]

SERVICE = {
    "name": "test-service",
}
BACKUP = {
    "name": "test-backup",
}

METADATA_IMPORT = {
    "name": "test-metadata-import-1",
    "database_dump": {
        "gcs_uri": f"gs://{BUCKET}/{METADATA_IMPORT_FILE}",
        "database_type": "MYSQL",
    },
}

SERVICE_TO_UPDATE = {
    "labels": {
        "mylocalmachine": "mylocalmachine",
        "systemtest": "systemtest",
    }
}
UPDATE_MASK = {"paths": ["labels"]}

# [END how_to_cloud_dataproc_metastore_create_service]


with models.DAG("example_gcp_dataproc_metastore", start_date=days_ago(1), schedule_interval="@once") as dag:
    # [START how_to_cloud_dataproc_metastore_create_service_operator]
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        location_id=REGION,
        project_number=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
        timeout=1200,
    )
    # [END how_to_cloud_dataproc_metastore_create_service_operator]

    # [START how_to_cloud_dataproc_metastore_get_service_operator]
    get_service_details = DataprocMetastoreGetServiceOperator(
        task_id="get_service",
        location_id=REGION,
        project_number=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_get_service_operator]

    # [START how_to_cloud_dataproc_metastore_update_service_operator]
    update_service = DataprocMetastoreUpdateServiceOperator(
        task_id="update_service",
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        location_id=REGION,
        service=SERVICE_TO_UPDATE,
        update_mask=UPDATE_MASK,
    )

    # [END how_to_cloud_dataproc_metastore_update_service_operator]

    # [START how_to_cloud_dataproc_metastore_create_metadata_import_operator]
    import_metadata = DataprocMetastoreCreateMetadataImportOperator(
        task_id="create_metadata_import",
        project_number=PROJECT_ID,
        location_id=REGION,
        service_id=SERVICE_ID,
        metadata_import=METADATA_IMPORT,
        metadata_import_id=METADATA_IMPORT_ID,
    )
    # [END how_to_cloud_dataproc_metastore_create_metadata_import_operator]

    # [START how_to_cloud_dataproc_metastore_export_metadata_operator]
    export_metadata = DataprocMetastoreExportMetadataOperator(
        task_id="export_metadata",
        destination_gcs_folder="gs://wj-dm-1/dataproc-metastore",
        project_id=PROJECT_ID,
        location_id=REGION,
        service_id=SERVICE_ID,
        timeout=1200,
    )
    # [END how_to_cloud_dataproc_metastore_export_metadata_operator]

    # [START how_to_cloud_dataproc_metastore_create_backup_operator]
    backup_service = DataprocMetastoreCreateBackupOperator(
        task_id="create_backup",
        project_number=PROJECT_ID,
        location_id=REGION,
        service_id=SERVICE_ID,
        backup=BACKUP,
        backup_id=BACKUP_ID,
    )
    # [END how_to_cloud_dataproc_metastore_create_backup_operator]

    # [START how_to_cloud_dataproc_metastore_list_backups_operator]
    lists_backups = DataprocMetastoreListBackupsOperator(
        task_id="list_backups",
        project_number=PROJECT_ID,
        location_id=REGION,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_list_backups_operator]

    # [START how_to_cloud_dataproc_metastore_delete_backup_operator]
    delete_backup = DataprocMetastoreDeleteBackupOperator(
        task_id="delete_backup",
        project_number=PROJECT_ID,
        location_id=REGION,
        service_id=SERVICE_ID,
        backup_id=BACKUP_ID,
    )
    # [END how_to_cloud_dataproc_metastore_delete_backup_operator]

    restore_service = DataprocMetastoreRestoreServiceOperator()

    # [START how_to_cloud_dataproc_metastore_delete_service_operator]
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        location_id=REGION,
        project_number=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_delete_service_operator]

    # create_service
    # delete_service
    # get_service_details
    # backup_service
    # delete_backup
    # lists_backups
    export_metadata
    # backup_service >> lists_backups
    # create_service >> update_service >> delete_service
    # create_service >> get_service_details >> delete_service
    # create_service >> import_metadata >> delete_service
    # create_service >> export_metadata >> delete_service
    # create_service >> backup_service >> delete_service
    # create_service >> lists_backups >> delete_service
    # create_service >> delete_backup >> delete_service
    # create_service >> restore_service >> delete_service
