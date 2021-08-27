from typing import Dict, Optional, Sequence, Tuple, Union
from unittest import TestCase, mock

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.contrib.operator.dataproc_metastore_operator import (
    DataprocMetastoreBackupPathOperator,
    DataprocMetastoreCommonBillingAccountPathOperator,
    DataprocMetastoreCommonFolderPathOperator,
    DataprocMetastoreCommonLocationPathOperator,
    DataprocMetastoreCommonOrganizationPathOperator,
    DataprocMetastoreCommonProjectPathOperator,
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreGetBackupOperator,
    DataprocMetastoreGetDefaultMtlsEndpointOperator,
    DataprocMetastoreGetMetadataImportOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreListMetadataImportsOperator,
    DataprocMetastoreListServicesOperator,
    DataprocMetastoreMetadataImportPathOperator,
    DataprocMetastoreNetworkPathOperator,
    DataprocMetastoreParseBackupPathOperator,
    DataprocMetastoreParseCommonBillingAccountPathOperator,
    DataprocMetastoreParseCommonFolderPathOperator,
    DataprocMetastoreParseCommonLocationPathOperator,
    DataprocMetastoreParseCommonOrganizationPathOperator,
    DataprocMetastoreParseCommonProjectPathOperator,
    DataprocMetastoreParseMetadataImportPathOperator,
    DataprocMetastoreParseNetworkPathOperator,
    DataprocMetastoreParseServicePathOperator,
    DataprocMetastoreRestoreServiceOperator,
    DataprocMetastoreServicePathOperator,
    DataprocMetastoreUpdateMetadataImportOperator,
    DataprocMetastoreUpdateServiceOperator,
)
from airflow.utils.decorators import apply_defaults
from google.api_core.retry import Retry
from google.cloud.redis_v1beta1 import CloudRedisClient

from tests.contrib.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_API_ENDPOINT: str = "test-api-endpoint"
TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REQUEST: UpdateServiceRequest = None  # TODO: Fill missing value
TEST_PROJECT_NUMBER: str = "test-project-number"
TEST_BACKUP: str = "test-backup"
TEST_BACKUP_ID: str = "test-backup-id"
TEST_RETRY: Retry = None  # TODO: Fill missing value
TEST_TIMEOUT: float = None  # TODO: Fill missing value
TEST_METADATA: Sequence[Tuple[str, str]] = None  # TODO: Fill missing value
TEST_METADATA_IMPORT: MetadataImport = None  # TODO: Fill missing value
TEST_METADATA_IMPORT_ID: str = "test-metadata-import-id"
TEST_SERVICE: Service = None  # TODO: Fill missing value
TEST_SERVICE_ID: str = "test-service-id"
TEST_UPDATE_MASK: FieldMask = None  # TODO: Fill missing value


class TestDataprocMetastoreGetDefaultMtlsEndpointOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreGetDefaultMtlsEndpointOperator(
            api_endpoint=TEST_API_ENDPOINT, gcp_conn_id=TEST_GCP_CONN_ID
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value._get_default_mtls_endpoint.assert_called_once_with(
            api_endpoint=TEST_API_ENDPOINT
        )


class TestDataprocMetastoreCreateBackupOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreCreateBackupOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_backup.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreCreateMetadataImportOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreCreateMetadataImportOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreCreateServiceOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreCreateServiceOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.create_service.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreDeleteBackupOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreDeleteBackupOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.delete_backup.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreDeleteServiceOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreDeleteServiceOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.delete_service.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreExportMetadataOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreExportMetadataOperator(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.export_metadata.assert_called_once_with(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreGetBackupOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreGetBackupOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.get_backup.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreGetMetadataImportOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreGetMetadataImportOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.get_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreGetServiceOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreGetServiceOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.get_service.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreListBackupsOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreListBackupsOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.list_backups.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreListMetadataImportsOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreListMetadataImportsOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.list_metadata_imports.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreListServicesOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreListServicesOperator(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.list_services.assert_called_once_with(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreRestoreServiceOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreRestoreServiceOperator(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.restore_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreUpdateMetadataImportOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreUpdateMetadataImportOperator(
            request=TEST_REQUEST,
            metadata_import=TEST_METADATA_IMPORT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.update_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            metadata_import=TEST_METADATA_IMPORT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreUpdateServiceOperator(TestCase):
    @mock.patch(
        "airflow.contrib.operator.dataproc_metastore_operator.DataprocMetastoreHook"
    )
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreUpdateServiceOperator(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=TEST_GCP_CONN_ID,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=TEST_GCP_CONN_ID)
        mock_hook.return_value.update_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
