from typing import Dict, Optional, Sequence, Tuple, Union
from unittest import TestCase, mock

from airflow import AirflowException
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.utils.decorators import apply_defaults
from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.metastore_v1 import DataprocMetastoreClient
from google.cloud.metastore_v1.services.dataproc_metastore.pagers import (
    ListBackupsPager,
    ListMetadataImportsPager,
    ListServicesPager,
)
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


class DataprocMetastoreHook(GoogleCloudBaseHook):
    """
    Instantiates the dataproc metastore client.

    :param gcp_conn_id:
    :type gcp_conn_id: str
    :param delegate_to:
    :type delegate_to: str
    """

    def __init__(self, gcp_conn_id: str, delegate_to: str) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self._client: DataprocMetastoreClient = None

    def get_conn(self,) -> google.cloud.metastore_v1.DataprocMetastoreClient:
        """
        Retrieves client library object that allow access to Dataproc Metastore service.

        """
        if not self._client:
            self._client = DataprocMetastoreClient(credentials=self._get_credentials())
        return self._client

    def _get_default_mtls_endpoint(self, api_endpoint: str = None) -> str:
        """
        Converts api endpoint to mTLS endpoint.

                Convert "*.sandbox.googleapis.com" and "*.googleapis.com" to "*.mtls.sandbox.googleapis.com"
        and "*.mtls.googleapis.com" respectively.

        :param api_endpoint: the api endpoint to convert.
        :type api_endpoint: str
        """
        client = self.get_conn()
        result = client._get_default_mtls_endpoint(api_endpoint=api_endpoint)
        return result

    def create_backup(
        self,
        request: CreateBackupRequest,
        project_number: str,
        backup: Backup,
        backup_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
        """
        Creates a new backup in a given project and location.

        :param request:  The request object. Request message for
            [DataprocMetastore.CreateBackup][google.cloud.metastore.v1.DataprocMetastore.CreateBackup].
        :type request: google.cloud.metastore_v1.types.CreateBackupRequest
        :param project_number: TODO: Fill description
        :type project_number: str
        :param backup:  Required. The backup to create. The ``name`` field is ignored. The ID of the created
            backup must be provided in the request's ``backup_id`` field.

            This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :type backup: google.cloud.metastore_v1.types.Backup
        :param backup_id:  Required. The ID of the backup, which is used as the final component of the
            backup's name. This value must be between 1 and 64 characters long, begin with a letter, end with
            a letter or number, and consist of alpha-numeric ASCII characters or hyphens.

            This corresponds to the ``backup_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :type backup_id: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        parent = DataprocMetastoreClient.TODO(project_number)
        result = client.create_backup(
            request=request,
            parent=parent,
            backup=backup,
            backup_id=backup_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def create_metadata_import(
        self,
        request: CreateMetadataImportRequest,
        project_number: str,
        metadata_import: MetadataImport,
        metadata_import_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
        """
        Creates a new MetadataImport in a given project and location.

        :param request:  The request object. Request message for [DataprocMetastore.CreateMetadataImport][goog
            le.cloud.metastore.v1.DataprocMetastore.CreateMetadataImport].
        :type request: google.cloud.metastore_v1.types.CreateMetadataImportRequest
        :param project_number: TODO: Fill description
        :type project_number: str
        :param metadata_import:  Required. The metadata import to create. The ``name`` field is ignored. The
            ID of the created metadata import must be provided in the request's ``metadata_import_id`` field.

            This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :type metadata_import: google.cloud.metastore_v1.types.MetadataImport
        :param metadata_import_id:  Required. The ID of the metadata import, which is used as the final
            component of the metadata import's name. This value must be between 1 and 64 characters long,
            begin with a letter, end with a letter or number, and consist of alpha-numeric ASCII characters or
            hyphens.

            This corresponds to the ``metadata_import_id`` field on the ``request`` instance; if ``request``
            is provided, this should not be set.
        :type metadata_import_id: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        parent = DataprocMetastoreClient.TODO(project_number)
        result = client.create_metadata_import(
            request=request,
            parent=parent,
            metadata_import=metadata_import,
            metadata_import_id=metadata_import_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def create_service(
        self,
        request: CreateServiceRequest,
        project_number: str,
        service: Service,
        service_id: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
        """
        Creates a metastore service in a project and location.

        :param request:  The request object. Request message for
            [DataprocMetastore.CreateService][google.cloud.metastore.v1.DataprocMetastore.CreateService].
        :type request: google.cloud.metastore_v1.types.CreateServiceRequest
        :param project_number: TODO: Fill description
        :type project_number: str
        :param service:  Required. The Metastore service to create. The ``name`` field is ignored. The ID of
            the created metastore service must be provided in the request's ``service_id`` field.

            This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :type service: google.cloud.metastore_v1.types.Service
        :param service_id:  Required. The ID of the metastore service, which is used as the final component of
            the metastore service's name. This value must be between 2 and 63 characters long inclusive, begin
            with a letter, end with a letter or number, and consist of alpha-numeric ASCII characters or
            hyphens.

            This corresponds to the ``service_id`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :type service_id: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        parent = DataprocMetastoreClient.TODO(project_number)
        result = client.create_service(
            request=request,
            parent=parent,
            service=service,
            service_id=service_id,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def delete_backup(
        self,
        request: DeleteBackupRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
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
        """
        client = self.get_conn()
        name = DataprocMetastoreClient.TODO(project_number)
        result = client.delete_backup(
            request=request, name=name, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def delete_service(
        self,
        request: DeleteServiceRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
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
        """
        client = self.get_conn()
        name = DataprocMetastoreClient.TODO(project_number)
        result = client.delete_service(
            request=request, name=name, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def export_metadata(
        self,
        request: ExportMetadataRequest,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
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
        """
        client = self.get_conn()
        result = client.export_metadata(
            request=request, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def get_backup(
        self,
        request: GetBackupRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.cloud.metastore_v1.types.Backup:
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
        """
        client = self.get_conn()
        name = DataprocMetastoreClient.TODO(project_number)
        result = client.get_backup(
            request=request, name=name, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def get_metadata_import(
        self,
        request: GetMetadataImportRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.cloud.metastore_v1.types.MetadataImport:
        """
        Gets details of a single import.

        :param request:  The request object. Request message for
            [DataprocMetastore.GetMetadataImport][google.cloud.metastore.v1.DataprocMetastore.GetMetadataImpor
            t].
        :type request: google.cloud.metastore_v1.types.GetMetadataImportRequest
        :param project_number: TODO: Fill description
        :type project_number: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        name = DataprocMetastoreClient.TODO(project_number)
        result = client.get_metadata_import(
            request=request, name=name, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def get_service(
        self,
        request: GetServiceRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.cloud.metastore_v1.types.Service:
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
        """
        client = self.get_conn()
        name = DataprocMetastoreClient.TODO(project_number)
        result = client.get_service(
            request=request, name=name, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    def list_backups(
        self,
        request: ListBackupsRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.cloud.metastore_v1.services.dataproc_metastore.pagers.ListBackupsPager:
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
        """
        client = self.get_conn()
        parent = DataprocMetastoreClient.TODO(project_number)
        result = client.list_backups(
            request=request,
            parent=parent,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def list_metadata_imports(
        self,
        request: ListMetadataImportsRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.cloud.metastore_v1.services.dataproc_metastore.pagers.ListMetadataImportsPager:
        """
        Lists imports in a service.

        :param request:  The request object. Request message for [DataprocMetastore.ListMetadataImports][googl
            e.cloud.metastore.v1.DataprocMetastore.ListMetadataImports].
        :type request: google.cloud.metastore_v1.types.ListMetadataImportsRequest
        :param project_number: TODO: Fill description
        :type project_number: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        parent = DataprocMetastoreClient.TODO(project_number)
        result = client.list_metadata_imports(
            request=request,
            parent=parent,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def list_services(
        self,
        request: ListServicesRequest,
        project_number: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.cloud.metastore_v1.services.dataproc_metastore.pagers.ListServicesPager:
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
        """
        client = self.get_conn()
        parent = DataprocMetastoreClient.TODO(project_number)
        result = client.list_services(
            request=request,
            parent=parent,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def restore_service(
        self,
        request: RestoreServiceRequest,
        service: str,
        backup: str,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
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
        :param backup:  Required. The relative resource name of the metastore service backup to restore from,
            in the following form:

            ``projects/{project_id}/locations/{location_id}/services/{service_id}/backups/{backup_id}``.

            This corresponds to the ``backup`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :type backup: str
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        result = client.restore_service(
            request=request,
            service=service,
            backup=backup,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def update_metadata_import(
        self,
        request: UpdateMetadataImportRequest,
        metadata_import: MetadataImport,
        update_mask: FieldMask,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
        """
        Updates a single import. Only the description field of MetadataImport is supported to be updated.

        :param request:  The request object. Request message for [DataprocMetastore.UpdateMetadataImport][goog
            le.cloud.metastore.v1.DataprocMetastore.UpdateMetadataImport].
        :type request: google.cloud.metastore_v1.types.UpdateMetadataImportRequest
        :param metadata_import:  Required. The metadata import to update. The server only merges fields in the
            import if they are specified in ``update_mask``.

            The metadata import's ``name`` field is used to identify the metastore import to be updated.

            This corresponds to the ``metadata_import`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :type metadata_import: google.cloud.metastore_v1.types.MetadataImport
        :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the
            metadata import resource by the update. Fields specified in the ``update_mask`` are relative to
            the resource (not to the full request). A field is overwritten if it is in the mask.

            This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :type update_mask: google.protobuf.field_mask_pb2.FieldMask
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        result = client.update_metadata_import(
            request=request,
            metadata_import=metadata_import,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    def update_service(
        self,
        request: UpdateServiceRequest,
        service: Service,
        update_mask: FieldMask,
        retry: Retry,
        timeout: float,
        metadata: Sequence[Tuple[str, str]],
    ) -> google.api_core.operation.Operation:
        """
        Updates the parameters of a single service.

        :param request:  The request object. Request message for
            [DataprocMetastore.UpdateService][google.cloud.metastore.v1.DataprocMetastore.UpdateService].
        :type request: google.cloud.metastore_v1.types.UpdateServiceRequest
        :param service:  Required. The metastore service to update. The server only merges fields in the
            service if they are specified in ``update_mask``.

            The metastore service's ``name`` field is used to identify the metastore service to be updated.

            This corresponds to the ``service`` field on the ``request`` instance; if ``request`` is provided,
            this should not be set.
        :type service: google.cloud.metastore_v1.types.Service
        :param update_mask:  Required. A field mask used to specify the fields to be overwritten in the
            metastore service resource by the update. Fields specified in the ``update_mask`` are relative to
            the resource (not to the full request). A field is overwritten if it is in the mask.

            This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :type update_mask: google.protobuf.field_mask_pb2.FieldMask
        :param retry: Designation of what errors, if any, should be retried.
        :type retry: google.api_core.retry.Retry
        :param timeout: The timeout for this request.
        :type timeout: float
        :param metadata: Strings which should be sent along with the request as metadata.
        :type metadata: Sequence[Tuple[str, str]]
        """
        client = self.get_conn()
        result = client.update_service(
            request=request,
            service=service,
            update_mask=update_mask,
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
