from typing import Dict, Optional, Sequence, Tuple, Union
from unittest import TestCase, mock

from airflow import AirflowException
from airflow.contrib.hooks.dataproc_metastore_hook import DataprocMetastoreHook
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from google.api_core.retry import Retry
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
from google.cloud.redis_v1beta1 import CloudRedisClient
from google.protobuf.field_mask_pb2 import FieldMask

from tests.contrib.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)


class DataprocMetastoreGetDefaultMtlsEndpointOperator(BaseOperator):
    """
    Converts api endpoint to mTLS endpoint.

            Convert "*.sandbox.googleapis.com" and "*.googleapis.com" to "*.mtls.sandbox.googleapis.com" and
    "*.mtls.googleapis.com" respectively.

    :param api_endpoint: the api endpoint to convert.
    :type api_endpoint: str
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        api_endpoint: str = None,
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.api_endpoint = api_endpoint
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook._get_default_mtls_endpoint(api_endpoint=self.api_endpoint)


class DataprocMetastoreCreateBackupOperator(BaseOperator):
    """
    Creates a new backup in a given project and location.

    :param request:  The request object. Request message for
        [DataprocMetastore.CreateBackup][google.cloud.metastore.v1.DataprocMetastore.CreateBackup].
    :type request: google.cloud.metastore_v1.types.CreateBackupRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param backup:  Required. The backup to create. The ``name`` field is ignored. The ID of the created
        backup must be provided in the request's ``backup_id`` field.

        This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided, this
        should not be set.
    :type backup: google.cloud.metastore_v1.types.Backup
    :param backup_id:  Required. The ID of the backup, which is used as the final component of the backup's
        name. This value must be between 1 and 64 characters long, begin with a letter, end with a letter or
        number, and consist of alpha-numeric ASCII characters or hyphens.

        This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type backup_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: CreateBackupRequest,
        project_number: str,
        backup: Backup,
        backup_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.backup = backup
        self.backup_id = backup_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.create_backup(
            request=self.request,
            project_number=self.project_number,
            backup=self.backup,
            backup_id=self.backup_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreCreateMetadataImportOperator(BaseOperator):
    """
    Creates a new MetadataImport in a given project and location.

    :param request:  The request object. Request message for [DataprocMetastore.CreateMetadataImport][google.c
        loud.metastore.v1.DataprocMetastore.CreateMetadataImport].
    :type request: google.cloud.metastore_v1.types.CreateMetadataImportRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param metadata_import:  Required. The metadata import to create. The ``name`` field is ignored. The ID of
        the created metadata import must be provided in the request's ``metadata_import_id`` field.

        This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type metadata_import: google.cloud.metastore_v1.types.MetadataImport
    :param metadata_import_id:  Required. The ID of the metadata import, which is used as the final component
        of the metadata import's name. This value must be between 1 and 64 characters long, begin with a
        letter, end with a letter or number, and consist of alpha-numeric ASCII characters or hyphens.

        This corresponds to the ``metadata_import_id`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type metadata_import_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: CreateMetadataImportRequest,
        project_number: str,
        metadata_import: MetadataImport,
        metadata_import_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.metadata_import = metadata_import
        self.metadata_import_id = metadata_import_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.create_metadata_import(
            request=self.request,
            project_number=self.project_number,
            metadata_import=self.metadata_import,
            metadata_import_id=self.metadata_import_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreCreateServiceOperator(BaseOperator):
    """
    Creates a metastore service in a project and location.

    :param request:  The request object. Request message for
        [DataprocMetastore.CreateService][google.cloud.metastore.v1.DataprocMetastore.CreateService].
    :type request: google.cloud.metastore_v1.types.CreateServiceRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param service:  Required. The Metastore service to create. The ``name`` field is ignored. The ID of the
        created metastore service must be provided in the request's ``service_id`` field.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type service: google.cloud.metastore_v1.types.Service
    :param service_id:  Required. The ID of the metastore service, which is used as the final component of the
        metastore service's name. This value must be between 2 and 63 characters long inclusive, begin with a
        letter, end with a letter or number, and consist of alpha-numeric ASCII characters or hyphens.

        This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type service_id: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: CreateServiceRequest,
        project_number: str,
        service: Service,
        service_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.service = service
        self.service_id = service_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.create_service(
            request=self.request,
            project_number=self.project_number,
            service=self.service,
            service_id=self.service_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreDeleteBackupOperator(BaseOperator):
    """
    Deletes a single backup.

    :param request:  The request object. Request message for
        [DataprocMetastore.DeleteBackup][google.cloud.metastore.v1.DataprocMetastore.DeleteBackup].
    :type request: google.cloud.metastore_v1.types.DeleteBackupRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: DeleteBackupRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_backup(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreDeleteServiceOperator(BaseOperator):
    """
    Deletes a single service.

    :param request:  The request object. Request message for
        [DataprocMetastore.DeleteService][google.cloud.metastore.v1.DataprocMetastore.DeleteService].
    :type request: google.cloud.metastore_v1.types.DeleteServiceRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: DeleteServiceRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.delete_service(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreExportMetadataOperator(BaseOperator):
    """
    Exports metadata from a service.

    :param request:  The request object. Request message for
        [DataprocMetastore.ExportMetadata][google.cloud.metastore.v1.DataprocMetastore.ExportMetadata].
    :type request: google.cloud.metastore_v1.types.ExportMetadataRequest
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: ExportMetadataRequest,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.export_metadata(
            request=self.request,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreGetBackupOperator(BaseOperator):
    """
    Gets details of a single backup.

    :param request:  The request object. Request message for
        [DataprocMetastore.GetBackup][google.cloud.metastore.v1.DataprocMetastore.GetBackup].
    :type request: google.cloud.metastore_v1.types.GetBackupRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: GetBackupRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.get_backup(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreGetMetadataImportOperator(BaseOperator):
    """
    Gets details of a single import.

    :param request:  The request object. Request message for
        [DataprocMetastore.GetMetadataImport][google.cloud.metastore.v1.DataprocMetastore.GetMetadataImport].
    :type request: google.cloud.metastore_v1.types.GetMetadataImportRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: GetMetadataImportRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.get_metadata_import(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreGetServiceOperator(BaseOperator):
    """
    Gets the details of a single service.

    :param request:  The request object. Request message for
        [DataprocMetastore.GetService][google.cloud.metastore.v1.DataprocMetastore.GetService].
    :type request: google.cloud.metastore_v1.types.GetServiceRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: GetServiceRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.get_service(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreListBackupsOperator(BaseOperator):
    """
    Lists backups in a service.

    :param request:  The request object. Request message for
        [DataprocMetastore.ListBackups][google.cloud.metastore.v1.DataprocMetastore.ListBackups].
    :type request: google.cloud.metastore_v1.types.ListBackupsRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: ListBackupsRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.list_backups(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreListMetadataImportsOperator(BaseOperator):
    """
    Lists imports in a service.

    :param request:  The request object. Request message for
        [DataprocMetastore.ListMetadataImports][google.cloud.metastore.v1.DataprocMetastore.ListMetadataImport
        s].
    :type request: google.cloud.metastore_v1.types.ListMetadataImportsRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: ListMetadataImportsRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.list_metadata_imports(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreListServicesOperator(BaseOperator):
    """
    Lists services in a project and location.

    :param request:  The request object. Request message for
        [DataprocMetastore.ListServices][google.cloud.metastore.v1.DataprocMetastore.ListServices].
    :type request: google.cloud.metastore_v1.types.ListServicesRequest
    :param project_number: TODO: Fill description
    :type project_number: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: ListServicesRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.project_number = project_number
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.list_services(
            request=self.request,
            project_number=self.project_number,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreRestoreServiceOperator(BaseOperator):
    """
    Restores a service from a backup.

    :param request:  The request object. Request message for [DataprocMetastore.Restore][].
    :type request: google.cloud.metastore_v1.types.RestoreServiceRequest
    :param service:  Required. The relative resource name of the metastore service to run restore, in the
        following form:

        ``projects/{project_id}/locations/{location_id}/services/{service_id}``.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type service: str
    :param backup:  Required. The relative resource name of the metastore service backup to restore from, in
        the following form:

        ``projects/{project_id}/locations/{location_id}/services/{service_id}/backups/{backup_id}``.

        This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided, this
        should not be set.
    :type backup: str
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: RestoreServiceRequest,
        service: str,
        backup: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.service = service
        self.backup = backup
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.restore_service(
            request=self.request,
            service=self.service,
            backup=self.backup,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreUpdateMetadataImportOperator(BaseOperator):
    """
    Updates a single import. Only the description field of MetadataImport is supported to be updated.

    :param request:  The request object. Request message for [DataprocMetastore.UpdateMetadataImport][google.c
        loud.metastore.v1.DataprocMetastore.UpdateMetadataImport].
    :type request: google.cloud.metastore_v1.types.UpdateMetadataImportRequest
    :param metadata_import:  Required. The metadata import to update. The server only merges fields in the
        import if they are specified in ``update_mask``.

        The metadata import's ``name`` field is used to identify the metastore import to be updated.

        This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
        provided, this should not be set.
    :type metadata_import: google.cloud.metastore_v1.types.MetadataImport
    :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the metadata
        import resource by the update. Fields specified in the ``update_mask`` are relative to the resource
        (not to the full request). A field is overwritten if it is in the mask.

        This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type update_mask: google.protobuf.field_mask_pb2.FieldMask
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: UpdateMetadataImportRequest,
        metadata_import: MetadataImport,
        update_mask: FieldMask,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.metadata_import = metadata_import
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.update_metadata_import(
            request=self.request,
            metadata_import=self.metadata_import,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )


class DataprocMetastoreUpdateServiceOperator(BaseOperator):
    """
    Updates the parameters of a single service.

    :param request:  The request object. Request message for
        [DataprocMetastore.UpdateService][google.cloud.metastore.v1.DataprocMetastore.UpdateService].
    :type request: google.cloud.metastore_v1.types.UpdateServiceRequest
    :param service:  Required. The metastore service to update. The server only merges fields in the service
        if they are specified in ``update_mask``.

        The metastore service's ``name`` field is used to identify the metastore service to be updated.

        This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type service: google.cloud.metastore_v1.types.Service
    :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the metastore
        service resource by the update. Fields specified in the ``update_mask`` are relative to the resource
        (not to the full request). A field is overwritten if it is in the mask.

        This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :type update_mask: google.protobuf.field_mask_pb2.FieldMask
    :param retry: Designation of what errors, if any, should be retried.
    :type retry: google.api_core.retry.Retry
    :param timeout: The timeout for this request.
    :type timeout: float
    :param metadata: Strings which should be sent along with the request as metadata.
    :type metadata: Sequence[Tuple[str, str]]
    :param gcp_conn_id:
    :type gcp_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        request: UpdateServiceRequest,
        service: Service,
        update_mask: FieldMask,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
        gcp_conn_id: str = "google_cloud_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request = request
        self.service = service
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context: Dict):
        hook = DataprocMetastoreHook(gcp_conn_id=self.gcp_conn_id)
        hook.update_service(
            request=self.request,
            service=self.service,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
