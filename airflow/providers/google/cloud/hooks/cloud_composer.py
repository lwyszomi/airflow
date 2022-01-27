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

from typing import Dict, Optional, Sequence, Tuple, Union

from google.api_core.operation import Operation
from google.api_core.retry import Retry
from google.cloud.orchestration.airflow.service_v1 import EnvironmentsClient, ImageVersionsClient
from google.cloud.orchestration.airflow.service_v1.services.environments.pagers import ListEnvironmentsPager
from google.cloud.orchestration.airflow.service_v1.services.image_versions.pagers import (
    ListImageVersionsPager,
)
from google.cloud.orchestration.airflow.service_v1.types import Environment
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class CloudComposerHook(GoogleBaseHook):
    """Hook for Google Cloud Composer APIs."""

    client_options = {'api_endpoint': 'composer.googleapis.com:443'}

    def get_environment_client(self) -> EnvironmentsClient:
        """Retrieves client library object that allow access Environments service."""
        return EnvironmentsClient(
            credentials=self._get_credentials(),
            client_info=self.client_info,
            client_options=self.client_options,
        )

    def get_image_versions_client(
        self,
    ) -> ImageVersionsClient:
        """Retrieves client library object that allow access Image Versions service."""
        return ImageVersionsClient(
            credentials=self._get_credentials(),
            client_info=self.client_info,
            client_options=self.client_options,
        )

    def wait_for_operation(self, operation: Operation, timeout: Optional[float] = None):
        """Waits for long-lasting operation to complete."""
        try:
            return operation.result(timeout=timeout)
        except Exception:
            error = operation.exception(timeout=timeout)
            raise AirflowException(error)

    def get_operation(self, operation_name):
        return self.get_environment_client().transport.operations_client.get_operation(name=operation_name)

    def get_environment_name(self, project_id, region, environment_id):
        return f'projects/{project_id}/locations/{region}/environments/{environment_id}'

    def get_parent(self, project_id, region):
        return f'projects/{project_id}/locations/{region}'

    @GoogleBaseHook.fallback_to_default_project_id
    def create_environment(
        self,
        project_id: str,
        region: str,
        environment: Union[Environment, Dict],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Create a new environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment:  The environment to create. This corresponds to the ``environment`` field on the
            ``request`` instance; if ``request`` is provided, this should not be set.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.create_environment(
            request={'parent': self.get_parent(project_id, region), 'environment': environment},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def delete_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        """
        Delete an environment.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        name = self.get_environment_name(project_id, region, environment_id)
        result = client.delete_environment(
            request={"name": name}, retry=retry, timeout=timeout, metadata=metadata
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def get_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Environment:
        """
        Get an existing environment.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.get_environment(
            request={'name': self.get_environment_name(project_id, region, environment_id)},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_environments(
        self,
        project_id: str,
        region: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListEnvironmentsPager:
        """
        List environments.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param page_size: The maximum number of environments to return.
        :param page_token: The next_page_token value returned from a previous List
            request, if any.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        result = client.list_environments(
            request={
                "parent": self.get_parent(project_id, region),
                "page_size": page_size,
                "page_token": page_token,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def update_environment(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        environment: Union[Environment, Dict],
        update_mask: Union[Dict, FieldMask],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> Operation:
        r"""
        Update an environment.

        :param environment:  A patch environment. Fields specified by the ``updateMask`` will be copied from
            the patch environment into the environment under update.

            This corresponds to the ``environment`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param update_mask:  Required. A comma-separated list of paths, relative to ``Environment``, of fields
            to update. For example, to set the version of scikit-learn to install in the environment to 0.19.0
            and to remove an existing installation of numpy, the ``updateMask`` parameter would include the
            following two ``paths`` values: "config.softwareConfig.pypiPackages.scikit-learn" and
            "config.softwareConfig.pypiPackages.numpy". The included patch environment would specify the
            scikit-learn version as follows:

            ::

                { "config":{ "softwareConfig":{ "pypiPackages":{ "scikit-learn":"==0.19.0" } } } }

            Note that in the above example, any existing PyPI packages other than scikit-learn and numpy will
            be unaffected.

            Only one update type may be included in a single request's ``updateMask``. For example, one cannot
            update both the PyPI packages and labels in the same request. However, it is possible to update
            multiple members of a map field simultaneously in the same request. For example, to set the labels
            "label1" and "label2" while clearing "label3" (assuming it already exists), one can provide the
            paths "labels.label1", "labels.label2", and "labels.label3" and populate the patch environment as
            follows:

            ::

                { "labels":{ "label1":"new-label1-value" "label2":"new-label2-value" } }

            Note that in the above example, any existing labels that are not included in the ``updateMask``
            will be unaffected.

            It is also possible to replace an entire map field by providing the map field's path in the
            ``updateMask``. The new value of the field will be that which is provided in the patch
            environment. For example, to delete all pre-existing user-specified PyPI packages and install
            botocore at version 1.7.14, the ``updateMask`` would contain the path
            "config.softwareConfig.pypiPackages", and the patch environment would be the following:

            ::

                { "config":{ "softwareConfig":{ "pypiPackages":{ "botocore":"==1.7.14" } } } }

            **Note:** Only the following fields can be updated:

            -  ``config.softwareConfig.pypiPackages``

               -  Replace all custom custom PyPI packages. If a replacement package map is not included in
            ``environment``, all custom PyPI packages are cleared. It is an error to provide both this mask
            and a mask specifying an individual package.

            -  ``config.softwareConfig.pypiPackages.``\ packagename

               -  Update the custom PyPI package *packagename*, preserving other packages. To delete the
            package, include it in ``updateMask``, and omit the mapping for it in
            ``environment.config.softwareConfig.pypiPackages``. It is an error to provide both a mask of this
            form and the ``config.softwareConfig.pypiPackages`` mask.

            -  ``labels``

               -  Replace all environment labels. If a replacement labels map is not included in
            ``environment``, all labels are cleared. It is an error to provide both this mask and a mask
            specifying one or more individual labels.

            -  ``labels.``\ labelName

               -  Set the label named *labelName*, while preserving other labels. To delete the label, include
            it in ``updateMask`` and omit its mapping in ``environment.labels``. It is an error to provide
            both a mask of this form and the ``labels`` mask.

            -  ``config.nodeCount``

               -  Horizontally scale the number of nodes in the environment. An integer greater than or equal
            to 3 must be provided in the ``config.nodeCount`` field.

            -  ``config.webServerNetworkAccessControl``

               -  Replace the environment's current ``WebServerNetworkAccessControl``.

            -  ``config.databaseConfig``

               -  Replace the environment's current ``DatabaseConfig``.

            -  ``config.webServerConfig``

               -  Replace the environment's current ``WebServerConfig``.

            -  ``config.softwareConfig.airflowConfigOverrides``

               -  Replace all Apache Airflow config overrides. If a replacement config overrides map is not
            included in ``environment``, all config overrides are cleared. It is an error to provide both this
            mask and a mask specifying one or more individual config overrides.

            -  ``config.softwareConfig.airflowConfigOverrides.``\ section-name

               -  Override the Apache Airflow config property *name* in the section named *section*,
            preserving other properties. To delete the property override, include it in ``updateMask`` and
            omit its mapping in ``environment.config.softwareConfig.airflowConfigOverrides``. It is an error
            to provide both a mask of this form and the ``config.softwareConfig.airflowConfigOverrides`` mask.

            -  ``config.softwareConfig.envVariables``

               -  Replace all environment variables. If a replacement environment variable map is not included
            in ``environment``, all custom environment variables are cleared. It is an error to provide both
            this mask and a mask specifying one or more individual environment variables.

            This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is
            provided, this should not be set.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_environment_client()
        name = self.get_environment_name(project_id, region, environment_id)

        result = client.update_environment(
            request={"name": name, "environment": environment, "update_mask": update_mask},
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result

    @GoogleBaseHook.fallback_to_default_project_id
    def list_image_versions(
        self,
        project_id: str,
        region: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        include_past_releases: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
    ) -> ListImageVersionsPager:
        """
        List ImageVersions for provided location.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param region: Required. The ID of the Google Cloud region that the service belongs to.
        :param retry: Designation of what errors, if any, should be retried.
        :param timeout: The timeout for this request.
        :param metadata: Strings which should be sent along with the request as metadata.
        """
        client = self.get_image_versions_client()
        result = client.list_image_versions(
            request={
                'parent': self.get_parent(project_id, region),
                "page_size": page_size,
                "page_token": page_token,
                "include_past_releases": include_past_releases,
            },
            retry=retry,
            timeout=timeout,
            metadata=metadata,
        )
        return result
