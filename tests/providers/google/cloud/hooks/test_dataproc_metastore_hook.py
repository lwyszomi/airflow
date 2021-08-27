from typing import Dict, Optional, Sequence, Tuple, Union
from unittest import TestCase, mock

from airflow import AirflowException
from airflow.contrib.hooks.dataproc_metastore_hook import DataprocMetastoreHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.decorators import apply_defaults
from google.api_core.retry import Retry
from google.cloud.redis_v1beta1 import CloudRedisClient

from tests.contrib.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_DELEGATE_TO: str = "test-delegate-to"
TEST_API_ENDPOINT: str = "test-api-endpoint"
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
TEST_PARENT: str = "test-parent"
TEST_NAME: str = "test-name"


class TestDataprocMetastoreWithDefaultProjectIdHook(TestCase):
    def setUp(self,) -> None:
        with mock.patch(
            "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = DataprocMetastoreHook(gcp_conn_id="test")

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test__get_default_mtls_endpoint(self, mock_get_conn) -> None:
        self.hook._get_default_mtls_endpoint(api_endpoint=TEST_API_ENDPOINT)
        mock_get_conn._get_default_mtls_endpoint.assert_called_once_with(
            api_endpoint=TEST_API_ENDPOINT
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_create_backup(self, mock_get_conn) -> None:
        self.hook.create_backup(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_backup.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_create_metadata_import(self, mock_get_conn) -> None:
        self.hook.create_metadata_import(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_create_service(self, mock_get_conn) -> None:
        self.hook.create_service(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_service.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_delete_backup(self, mock_get_conn) -> None:
        self.hook.delete_backup(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.delete_backup.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_delete_service(self, mock_get_conn) -> None:
        self.hook.delete_service(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.delete_service.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_export_metadata(self, mock_get_conn) -> None:
        self.hook.export_metadata(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.export_metadata.assert_called_once_with(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_get_backup(self, mock_get_conn) -> None:
        self.hook.get_backup(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_backup.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_get_metadata_import(self, mock_get_conn) -> None:
        self.hook.get_metadata_import(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_get_service(self, mock_get_conn) -> None:
        self.hook.get_service(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_service.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_list_backups(self, mock_get_conn) -> None:
        self.hook.list_backups(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_backups.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_list_metadata_imports(self, mock_get_conn) -> None:
        self.hook.list_metadata_imports(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_metadata_imports.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_list_services(self, mock_get_conn) -> None:
        self.hook.list_services(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_services.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_restore_service(self, mock_get_conn) -> None:
        self.hook.restore_service(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.restore_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_update_metadata_import(self, mock_get_conn) -> None:
        self.hook.update_metadata_import(
            request=TEST_REQUEST,
            metadata_import=TEST_METADATA_IMPORT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.update_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            metadata_import=TEST_METADATA_IMPORT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_update_service(self, mock_get_conn) -> None:
        self.hook.update_service(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.update_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreWithoutDefaultProjectIdHook(TestCase):
    def setUp(self,) -> None:
        with mock.patch(
            "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = DataprocMetastoreHook(gcp_conn_id="test")

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test__get_default_mtls_endpoint(self, mock_get_conn) -> None:
        self.hook._get_default_mtls_endpoint(api_endpoint=TEST_API_ENDPOINT)
        mock_get_conn._get_default_mtls_endpoint.assert_called_once_with(
            api_endpoint=TEST_API_ENDPOINT
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_create_backup(self, mock_get_conn) -> None:
        self.hook.create_backup(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_backup.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_create_metadata_import(self, mock_get_conn) -> None:
        self.hook.create_metadata_import(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_create_service(self, mock_get_conn) -> None:
        self.hook.create_service(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.create_service.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_delete_backup(self, mock_get_conn) -> None:
        self.hook.delete_backup(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.delete_backup.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_delete_service(self, mock_get_conn) -> None:
        self.hook.delete_service(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.delete_service.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_export_metadata(self, mock_get_conn) -> None:
        self.hook.export_metadata(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.export_metadata.assert_called_once_with(
            request=TEST_REQUEST,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_get_backup(self, mock_get_conn) -> None:
        self.hook.get_backup(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_backup.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_get_metadata_import(self, mock_get_conn) -> None:
        self.hook.get_metadata_import(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_get_service(self, mock_get_conn) -> None:
        self.hook.get_service(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.get_service.assert_called_once_with(
            request=TEST_REQUEST,
            name=TEST_NAME,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_list_backups(self, mock_get_conn) -> None:
        self.hook.list_backups(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_backups.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_list_metadata_imports(self, mock_get_conn) -> None:
        self.hook.list_metadata_imports(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_metadata_imports.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_list_services(self, mock_get_conn) -> None:
        self.hook.list_services(
            request=TEST_REQUEST,
            project_number=TEST_PROJECT_NUMBER,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.list_services.assert_called_once_with(
            request=TEST_REQUEST,
            parent=TEST_PARENT,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_restore_service(self, mock_get_conn) -> None:
        self.hook.restore_service(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.restore_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            backup=TEST_BACKUP,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_update_metadata_import(self, mock_get_conn) -> None:
        self.hook.update_metadata_import(
            request=TEST_REQUEST,
            metadata_import=TEST_METADATA_IMPORT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.update_metadata_import.assert_called_once_with(
            request=TEST_REQUEST,
            metadata_import=TEST_METADATA_IMPORT,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )

    @mock.patch(  # type: ignore
        "airflow.contrib.hooks.dataproc_metastore_hook.DataprocMetastoreHook.get_conn"
    )
    def test_update_service(self, mock_get_conn) -> None:
        self.hook.update_service(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
        mock_get_conn.update_service.assert_called_once_with(
            request=TEST_REQUEST,
            service=TEST_SERVICE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
