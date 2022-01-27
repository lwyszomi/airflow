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
from datetime import datetime
from typing import TYPE_CHECKING, Dict, Optional, Sequence, Tuple, Union

from google.api_core.exceptions import AlreadyExists
from google.api_core.retry import Retry
from google.cloud.orchestration.airflow.service_v1 import ImageVersion
from google.cloud.orchestration.airflow.service_v1.types import Environment
from google.protobuf.field_mask_pb2 import FieldMask

from airflow import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink, TaskInstance
from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerHook
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerExecutionTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME

if TYPE_CHECKING:
    from airflow.utils.context import Context

CLOUD_COMPOSER_BASE_LINK = "https://console.cloud.google.com/composer/environments"
CLOUD_COMPOSER_DETAILS_LINK = (
    CLOUD_COMPOSER_BASE_LINK + "/detail/{region}/{environment_id}/monitoring?project={project_id}"
)
CLOUD_COMPOSER_ENVIRONMENTS_LINK = CLOUD_COMPOSER_BASE_LINK + '?project={project_id}'


class CloudComposerEnvironmentLink(BaseOperatorLink):
    """Helper class for constructing Cloud Composer Environment Link"""

    name = "Cloud Composer Environment"

    def get_link(self, operator: BaseOperator, dttm: datetime) -> str:
        ti = TaskInstance(task=operator, execution_date=dttm)
        conf = ti.xcom_pull(task_ids=operator.task_id, key="composer_conf")
        return (
            CLOUD_COMPOSER_DETAILS_LINK.format(
                project_id=conf["project_id"], region=conf['region'], environment_id=conf['environment_id']
            )
            if conf
            else ""
        )


class CloudComposerEnvironmentsLink(BaseOperatorLink):
    """Helper class for constructing Cloud Composer Environment Link"""

    name = "Cloud Composer Environment List"

    def get_link(self, operator: BaseOperator, dttm: datetime) -> str:
        ti = TaskInstance(task=operator, execution_date=dttm)
        conf = ti.xcom_pull(task_ids=operator.task_id, key="composer_conf")
        return CLOUD_COMPOSER_ENVIRONMENTS_LINK.format(project_id=conf["project_id"]) if conf else ""


class CloudComposerCreateEnvironmentOperator(BaseOperator):
    """
    Create a new environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param environment:  The environment to create.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param deferrable: Run operator in the deferrable mode
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default the trigger will poll every 30 seconds.
    """

    template_fields = (
        'project_id',
        'region',
        'environment_id',
        'environment',
        'impersonation_chain',
    )

    operator_extra_links = (CloudComposerEnvironmentLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        environment: Union[Environment, Dict],
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        deferrable: bool = False,
        pooling_period_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.environment = environment
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.deferrable = deferrable
        self.pooling_period_seconds = pooling_period_seconds

    def execute(self, context: 'Context'):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )

        name = hook.get_environment_name(self.project_id, self.region, self.environment_id)
        if isinstance(self.environment, Environment):
            self.environment.name = name
        else:
            self.environment["name"] = name

        self.xcom_push(
            context,
            key='composer_conf',
            value={
                "project_id": self.project_id,
                "region": self.region,
                "environment_id": self.environment_id,
            },
        )
        try:
            result = hook.create_environment(
                project_id=self.project_id,
                region=self.region,
                environment=self.environment,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            if not self.deferrable:
                environment = hook.wait_for_operation(timeout=self.timeout, operation=result)
                return Environment.to_dict(environment)
            else:
                self.defer(
                    trigger=CloudComposerExecutionTrigger(
                        project_id=self.project_id,
                        region=self.region,
                        operation_name=result.operation.name,
                        gcp_conn_id=self.gcp_conn_id,
                        impersonation_chain=self.impersonation_chain,
                        delegate_to=self.delegate_to,
                        pooling_period_seconds=self.pooling_period_seconds,
                    ),
                    method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
                )
        except AlreadyExists:
            environment = hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return Environment.to_dict(environment)

    def execute_complete(self, context: Optional[dict], event: dict):
        if event["operation_done"]:
            hook = CloudComposerHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                delegate_to=self.delegate_to,
            )

            env = hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return Environment.to_dict(env)
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['operation_name']}")


class CloudComposerDeleteEnvironmentOperator(BaseOperator):
    """
    Delete an environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param deferrable: Run operator in the deferrable mode
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default the trigger will poll every 30 seconds.
    """

    template_fields = (
        'project_id',
        'region',
        'environment_id',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        deferrable: bool = False,
        pooling_period_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.deferrable = deferrable
        self.pooling_period_seconds = pooling_period_seconds

    def execute(self, context: 'Context'):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        result = hook.delete_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        if not self.deferrable:
            hook.wait_for_operation(timeout=self.timeout, operation=result)
        else:
            self.defer(
                trigger=CloudComposerExecutionTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    operation_name=result.operation.name,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    delegate_to=self.delegate_to,
                    pooling_period_seconds=self.pooling_period_seconds,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )

    def execute_complete(self, context: Optional[dict], event: dict):
        pass


class CloudComposerGetEnvironmentOperator(BaseOperator):
    """
    Get an existing environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    """

    template_fields = (
        'project_id',
        'region',
        'environment_id',
        'impersonation_chain',
    )

    operator_extra_links = (CloudComposerEnvironmentLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: 'Context'):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )

        result = hook.get_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        self.xcom_push(
            context,
            key='composer_conf',
            value={
                "project_id": self.project_id,
                "region": self.region,
                "environment_id": self.environment_id,
            },
        )
        return Environment.to_dict(result)


class CloudComposerListEnvironmentsOperator(BaseOperator):
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
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    """

    template_fields = (
        'project_id',
        'region',
        'impersonation_chain',
    )

    operator_extra_links = (CloudComposerEnvironmentsLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.page_size = page_size
        self.page_token = page_token
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: 'Context'):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        result = hook.list_environments(
            project_id=self.project_id,
            region=self.region,
            page_size=self.page_size,
            page_token=self.page_token,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [Environment.to_dict(env) for env in result]


class CloudComposerUpdateEnvironmentOperator(BaseOperator):
    r"""
    Update an environment.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: Required. The ID of the Google Cloud environment that the service belongs to.
    :param environment:  A patch environment. Fields specified by the ``updateMask`` will be copied from the
        patch environment into the environment under update.
    :param update_mask:  Required. A comma-separated list of paths, relative to ``Environment``, of fields to
        update. For example, to set the version of scikit-learn to install in the environment to 0.19.0 and to
        remove an existing installation of numpy, the ``updateMask`` parameter would include the following two
        ``paths`` values: "config.softwareConfig.pypiPackages.scikit-learn" and
        "config.softwareConfig.pypiPackages.numpy". The included patch environment would specify the scikit-
        learn version as follows:

        ::

            { "config":{ "softwareConfig":{ "pypiPackages":{ "scikit-learn":"==0.19.0" } } } }

        Note that in the above example, any existing PyPI packages other than scikit-learn and numpy will be
        unaffected.

        Only one update type may be included in a single request's ``updateMask``. For example, one cannot
        update both the PyPI packages and labels in the same request. However, it is possible to update
        multiple members of a map field simultaneously in the same request. For example, to set the labels
        "label1" and "label2" while clearing "label3" (assuming it already exists), one can provide the paths
        "labels.label1", "labels.label2", and "labels.label3" and populate the patch environment as follows:

        ::

            { "labels":{ "label1":"new-label1-value" "label2":"new-label2-value" } }

        Note that in the above example, any existing labels that are not included in the ``updateMask`` will
        be unaffected.

        It is also possible to replace an entire map field by providing the map field's path in the
        ``updateMask``. The new value of the field will be that which is provided in the patch environment.
        For example, to delete all pre-existing user-specified PyPI packages and install botocore at version
        1.7.14, the ``updateMask`` would contain the path "config.softwareConfig.pypiPackages", and the patch
        environment would be the following:

        ::

            { "config":{ "softwareConfig":{ "pypiPackages":{ "botocore":"==1.7.14" } } } }

        **Note:** Only the following fields can be updated:

        -  ``config.softwareConfig.pypiPackages``

           -  Replace all custom custom PyPI packages. If a replacement package map is not included in
        ``environment``, all custom PyPI packages are cleared. It is an error to provide both this mask and a
        mask specifying an individual package.

        -  ``config.softwareConfig.pypiPackages.``\ packagename

           -  Update the custom PyPI package *packagename*, preserving other packages. To delete the package,
        include it in ``updateMask``, and omit the mapping for it in
        ``environment.config.softwareConfig.pypiPackages``. It is an error to provide both a mask of this form
        and the ``config.softwareConfig.pypiPackages`` mask.

        -  ``labels``

           -  Replace all environment labels. If a replacement labels map is not included in ``environment``,
        all labels are cleared. It is an error to provide both this mask and a mask specifying one or more
        individual labels.

        -  ``labels.``\ labelName

           -  Set the label named *labelName*, while preserving other labels. To delete the label, include it
        in ``updateMask`` and omit its mapping in ``environment.labels``. It is an error to provide both a
        mask of this form and the ``labels`` mask.

        -  ``config.nodeCount``

           -  Horizontally scale the number of nodes in the environment. An integer greater than or equal to 3
        must be provided in the ``config.nodeCount`` field.

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

           -  Override the Apache Airflow config property *name* in the section named *section*, preserving
        other properties. To delete the property override, include it in ``updateMask`` and omit its mapping
        in ``environment.config.softwareConfig.airflowConfigOverrides``. It is an error to provide both a mask
        of this form and the ``config.softwareConfig.airflowConfigOverrides`` mask.

        -  ``config.softwareConfig.envVariables``

           -  Replace all environment variables. If a replacement environment variable map is not included in
        ``environment``, all custom environment variables are cleared. It is an error to provide both this
        mask and a mask specifying one or more individual environment variables.

        This corresponds to the ``update_mask`` field on the ``request`` instance; if ``request`` is provided,
        this should not be set.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param deferrable: Run operator in the deferrable mode
    :param pooling_period_seconds: Optional: Control the rate of the poll for the result of deferrable run.
        By default the trigger will poll every 30 seconds.
    """

    template_fields = (
        'project_id',
        'region',
        'environment_id',
        'impersonation_chain',
    )

    operator_extra_links = (CloudComposerEnvironmentLink(),)

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        environment: Union[Dict, Environment],
        update_mask: Union[Dict, FieldMask],
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        deferrable: bool = False,
        pooling_period_seconds: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.environment = environment
        self.update_mask = update_mask
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to
        self.deferrable = deferrable
        self.pooling_period_seconds = pooling_period_seconds

    def execute(self, context: 'Context'):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )

        result = hook.update_environment(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            environment=self.environment,
            update_mask=self.update_mask,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )

        self.xcom_push(
            context,
            key='composer_conf',
            value={
                "project_id": self.project_id,
                "region": self.region,
                "environment_id": self.environment_id,
            },
        )

        if not self.deferrable:
            environment = hook.wait_for_operation(timeout=self.timeout, operation=result)
            return Environment.to_dict(environment)
        else:
            self.defer(
                trigger=CloudComposerExecutionTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    operation_name=result.operation.name,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    delegate_to=self.delegate_to,
                    pooling_period_seconds=self.pooling_period_seconds,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )

    def execute_complete(self, context: Optional[dict], event: dict):
        if event["operation_done"]:
            hook = CloudComposerHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
                delegate_to=self.delegate_to,
            )

            env = hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                retry=self.retry,
                timeout=self.timeout,
                metadata=self.metadata,
            )
            return Environment.to_dict(env)
        else:
            raise AirflowException(f"Unexpected error in the operation: {event['operation_name']}")


class CloudComposerListImageVersionsOperator(BaseOperator):
    """
    List ImageVersions for provided location.

    :param request:  The request object. List ImageVersions in a project and location.
    :param retry: Designation of what errors, if any, should be retried.
    :param timeout: The timeout for this request.
    :param metadata: Strings which should be sent along with the request as metadata.
    :param gcp_conn_id:
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    """

    template_fields = (
        'project_id',
        'region',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
        include_past_releases: bool = False,
        retry: Optional[Retry] = None,
        timeout: Optional[float] = None,
        metadata: Sequence[Tuple[str, str]] = (),
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        delegate_to: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.page_size = page_size
        self.page_token = page_token
        self.include_past_releases = include_past_releases
        self.retry = retry
        self.timeout = timeout
        self.metadata = metadata
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.delegate_to = delegate_to

    def execute(self, context: 'Context'):
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            delegate_to=self.delegate_to,
        )
        result = hook.list_image_versions(
            project_id=self.project_id,
            region=self.region,
            page_size=self.page_size,
            page_token=self.page_token,
            include_past_releases=self.include_past_releases,
            retry=self.retry,
            timeout=self.timeout,
            metadata=self.metadata,
        )
        return [ImageVersion.to_dict(image) for image in result]
