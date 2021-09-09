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

from typing import Dict, Optional, Sequence, Tuple, Union
from unittest import TestCase, mock

from google.api_core.exceptions import AlreadyExists
from google.api_core.retry import Retry
from google.cloud.redis_v1beta1 import CloudRedisClient
from google.cloud.metastore_v1.types import (
    Backup,
    CreateBackupRequest,
    CreateMetadataImportRequest,
    CreateServiceRequest,
    DeleteBackupRequest,
    DeleteServiceRequest,
    ExportMetadataRequest,
    GetBackupRequest,
    GetMetadataImportRequest,
    GetServiceRequest,
    ListBackupsRequest,
    ListMetadataImportsRequest,
    ListServicesRequest,
    MetadataImport,
    RestoreServiceRequest,
    Service,
    UpdateMetadataImportRequest,
    UpdateServiceRequest,
)
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreGetDefaultMtlsEndpointOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreRestoreServiceOperator,
    DataprocMetastoreUpdateServiceOperator,
)

DATAPROC_METASTORE_PATH = "airflow.providers.google.cloud.operators.dataproc_metastore.{}"

TASK_ID: str = "task_id"
GCP_LOCATION: str = "test-location"
GCP_PROJECT_ID: str = "test-project-number"

TEST_API_ENDPOINT: str = "test-api-endpoint"
GCP_CONN_ID: str = "test-gcp-conn-id"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

SERVICE: Service = None  # TODO: Fill missing value
SERVICE_ID: str = "test-service-id"

TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]
REQUEST_ID = "request_id_uuid"

TEST_BACKUP: str = "test-backup"
TEST_BACKUP_ID: str = "test-backup-id"
TEST_METADATA_IMPORT: MetadataImport = None  # TODO: Fill missing value
TEST_METADATA_IMPORT_ID: str = "test-metadata-import-id"
TEST_UPDATE_MASK: FieldMask = None  # TODO: Fill missing value


class TestDataprocMetastoreGetDefaultMtlsEndpointOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreGetDefaultMtlsEndpointOperator(
            api_endpoint=TEST_API_ENDPOINT, gcp_conn_id=GCP_CONN_ID
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value._get_default_mtls_endpoint.assert_called_once_with(
            api_endpoint=TEST_API_ENDPOINT
        )


class TestDataprocMetastoreCreateBackupOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreCreateBackupOperator(
            project_number=GCP_PROJECT_ID,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_backup.assert_called_once_with(
            project_number=GCP_PROJECT_ID,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreCreateMetadataImportOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreCreateMetadataImportOperator(
            request=TEST_REQUEST,
            project_number=GCP_PROJECT_ID,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.create_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=GCP_PROJECT_ID,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreCreateServiceOperator(TestCase):
    @mock.patch(DATAPROC_METASTORE_PATH.format("DataprocMetastoreHook"))
    def test_execute(self, mock_hook) -> None:
        task = DataprocMetastoreCreateServiceOperator(
            task_id=TASK_ID,
            location_id=GCP_LOCATION,
            project_number=GCP_PROJECT_ID,
            service=SERVICE,
            service_id=SERVICE_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute({})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_service.assert_called_once_with(
            location_id=GCP_LOCATION,
            project_number=GCP_PROJECT_ID,
            service=SERVICE,
            service_id=SERVICE_ID,
            request_id=REQUEST_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestDataprocMetastoreDeleteBackupOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreDeleteBackupOperator(
            request=TEST_REQUEST,
            project_number=GCP_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.delete_backup.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=GCP_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreDeleteServiceOperator(TestCase):
    @mock.patch(DATAPROC_METASTORE_PATH.format("DataprocMetastoreHook"))
    def test_execute(self, mock_hook) -> None:
        task = DataprocMetastoreDeleteServiceOperator(
            task_id=TASK_ID,
            location_id=GCP_LOCATION,
            project_number=GCP_PROJECT_ID,
            service_id=SERVICE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute({})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_service.assert_called_once_with(
            location_id=GCP_LOCATION,
            project_number=GCP_PROJECT_ID,
            service_id=SERVICE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestDataprocMetastoreExportMetadataOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreExportMetadataOperator(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.export_metadata.assert_called_once_with(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreGetServiceOperator(TestCase):
    @mock.patch(DATAPROC_METASTORE_PATH.format("DataprocMetastoreHook"))
    def test_execute(self, mock_hook) -> None:
        task = DataprocMetastoreGetServiceOperator(
            task_id=TASK_ID,
            location_id=GCP_LOCATION,
            project_number=GCP_PROJECT_ID,
            service_id=SERVICE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute({})
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_service.assert_called_once_with(
            location_id=GCP_LOCATION,
            project_number=GCP_PROJECT_ID,
            service_id=SERVICE_ID,
            retry=RETRY,
            timeout=TIMEOUT,
            metadata=METADATA,
        )

class TestDataprocMetastoreListBackupsOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreListBackupsOperator(
            request=TEST_REQUEST,
            project_number=GCP_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.list_backups.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=GCP_PROJECT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreRestoreServiceOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreRestoreServiceOperator(
            request=TEST_REQUEST,
            service=SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.restore_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreUpdateServiceOperator(TestCase):
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreUpdateServiceOperator(
            request=TEST_REQUEST,
            service=SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID)
        mock_hook.return_value.update_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
