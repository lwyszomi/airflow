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
Example Airflow DAG that displays interactions with Google Cloud Build.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlsplit

import yaml
from googleapiclient import discovery

from airflow import models
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.models.xcom_arg import XComArg
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_build import (
    CloudBuildCancelBuildOperator,
    CloudBuildCreateBuildOperator,
    CloudBuildCreateBuildTriggerOperator,
    CloudBuildDeleteBuildTriggerOperator,
    CloudBuildGetBuildOperator,
    CloudBuildGetBuildTriggerOperator,
    CloudBuildListBuildsOperator,
    CloudBuildListBuildTriggersOperator,
    CloudBuildRetryBuildOperator,
    CloudBuildRunBuildTriggerOperator,
    CloudBuildUpdateBuildTriggerOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_gcp_cloud_build"

BUCKET_NAME_SRC = f"bucket-src-{DAG_ID}-{ENV_ID}"

FILE_NAME = "file.tar.gz"
GCP_SOURCE_ARCHIVE_URL = f"gs://{BUCKET_NAME_SRC}/{FILE_NAME}"
FILE_NAME_YAML = "example_cloud_build.yaml"
GCP_SOURCE_ARCHIVE_URL_PARTS = urlsplit(GCP_SOURCE_ARCHIVE_URL)
GCP_SOURCE_BUCKET_NAME = GCP_SOURCE_ARCHIVE_URL_PARTS.netloc

CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / FILE_NAME)

# [START howto_operator_gcp_create_build_from_storage_body]
CREATE_BUILD_FROM_STORAGE_BODY = {
    "source": {"storage_source": GCP_SOURCE_ARCHIVE_URL},
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world"]}],
}
# [END howto_operator_gcp_create_build_from_storage_body]
REPO_NAME = f"repo-test-{ENV_ID}"

# [START howto_operator_create_build_from_repo_body]
create_build_from_repo_body: dict[str, Any] = {
    "source": {"repo_source": {"repo_name": REPO_NAME, "branch_name": "master"}},
    "steps": [{"name": "ubuntu", "args": ["echo", "Hello world"]}],
}
# [END howto_operator_create_build_from_repo_body]
TRIGGER_NAME = f"cloud-build-trigger-{ENV_ID}"

# [START howto_operator_gcp_create_build_trigger_body]
create_build_trigger_body = {
    "name": TRIGGER_NAME,
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": REPO_NAME,
        "branch_name": "main",
    },
    "filename": FILE_NAME_YAML,
}
# [END howto_operator_gcp_create_build_trigger_body]

# [START howto_operator_gcp_update_build_trigger_body]
update_build_trigger_body = {
    "name": TRIGGER_NAME,
    "trigger_template": {
        "project_id": PROJECT_ID,
        "repo_name": REPO_NAME,
        "branch_name": "master",
    },
    "filename": FILE_NAME_YAML,
}
# [END START howto_operator_gcp_update_build_trigger_body]

CONTENT_YAML = """steps:
  - name: 'ubuntu'
    args: ['echo', 'Hello Test']
"""
BASH_COMMAND = (
    "git init && "
    f"git remote add google https://source.developers.google.com/p/{PROJECT_ID}/r/{REPO_NAME} && "
    "git config --global user.email 'test' && "
    "git config --global user.name 'test' && "
    f"echo '{CONTENT_YAML}' > {FILE_NAME_YAML} &&"
    "git add -A && "
    "git commit -m 'test commit' && "
    "git push --all google"
)

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloud_build"],
) as dag:

    @task
    def create_repo() -> None:
        with discovery.build("sourcerepo", "v1") as service:
            request = (
                service.projects()
                .repos()
                .create(
                    parent=f"projects/{PROJECT_ID}",
                    body={
                        "name": f"projects/{PROJECT_ID}/repos/{REPO_NAME}",
                    },
                )
            )
            request.execute()

    create_repo_task = create_repo()

    @task
    def delete_repo() -> None:
        with discovery.build("sourcerepo", "v1") as service:
            request = (
                service.projects()
                .repos()
                .delete(
                    name=f"projects/{PROJECT_ID}/repos/{REPO_NAME}",
                )
            )
            request.execute()

    delete_repo_task = delete_repo()
    create_bucket_src = GCSCreateBucketOperator(task_id="create_bucket_src", bucket_name=BUCKET_NAME_SRC)
    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME_SRC,
    )
    # [START howto_operator_create_build_from_storage]
    create_build_from_storage = CloudBuildCreateBuildOperator(
        task_id="create_build_from_storage",
        project_id=PROJECT_ID,
        build=CREATE_BUILD_FROM_STORAGE_BODY,
    )
    # [END howto_operator_create_build_from_storage]
    # [START howto_operator_create_build_from_storage_result]
    create_build_from_storage_result = BashOperator(
        bash_command=f"echo {cast(str, XComArg(create_build_from_storage, key='results'))}",
        task_id="create_build_from_storage_result",
    )
    # [END howto_operator_create_build_from_storage_result]
    # [START howto_operator_create_build_from_storage_async]
    create_build_from_storage_def = CloudBuildCreateBuildOperator(
        task_id="create_build_from_storage_def",
        project_id=PROJECT_ID,
        build=CREATE_BUILD_FROM_STORAGE_BODY,
        deferrable=True,
    )
    # [END howto_operator_create_build_from_storage_async]
    add_file_to_repo = BashOperator(
        bash_command=BASH_COMMAND,
        task_id="add_file_to_repo",
    )
    # [START howto_operator_create_build_from_repo]
    create_build_from_repo = CloudBuildCreateBuildOperator(
        task_id="create_build_from_repo",
        project_id=PROJECT_ID,
        build=create_build_from_repo_body,
    )
    # [END howto_operator_create_build_from_repo]
    # [START howto_operator_create_build_from_repo_result]
    create_build_from_repo_result = BashOperator(
        bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
        task_id="create_build_from_repo_result",
    )
    # [END howto_operator_create_build_from_repo_result]
    # [START howto_operator_create_build_from_repo_async]
    create_build_from_repo_async = CloudBuildCreateBuildOperator(
        task_id="create_build_from_repo_async",
        project_id=PROJECT_ID,
        build=create_build_from_repo_body,
        deferrable=True,
    )
    # [END howto_operator_create_build_from_repo_async]

    # [START howto_operator_create_build_from_repo_result]
    create_build_from_repo_result_async = BashOperator(
        bash_command=f"echo {cast(str, XComArg(create_build_from_repo, key='results'))}",
        task_id="create_build_from_repo_result_async",
    )
    # [END howto_operator_create_build_from_repo_result]
    # [START howto_operator_list_builds]
    list_builds = CloudBuildListBuildsOperator(
        task_id="list_builds",
        project_id=PROJECT_ID,
        location="global",
    )
    # [END howto_operator_list_builds]
    # [START howto_operator_create_build_without_wait]
    create_build_without_wait = CloudBuildCreateBuildOperator(
        task_id="create_build_without_wait",
        project_id=PROJECT_ID,
        build=create_build_from_repo_body,
        wait=False,
    )
    # [END howto_operator_create_build_without_wait]
    # [START howto_operator_cancel_build]
    cancel_build = CloudBuildCancelBuildOperator(
        task_id="cancel_build",
        id_=cast(str, XComArg(create_build_without_wait, key="id")),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_cancel_build]
    # [START howto_operator_retry_build]
    retry_build = CloudBuildRetryBuildOperator(
        task_id="retry_build",
        id_=cast(str, XComArg(cancel_build, key="id")),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_retry_build]
    # [START howto_operator_get_build]
    get_build = CloudBuildGetBuildOperator(
        task_id="get_build",
        id_=cast(str, XComArg(retry_build, key="id")),
        project_id=PROJECT_ID,
    )
    # [END howto_operator_get_build]
    # [START howto_operator_gcp_create_build_from_yaml_body]
    create_build_from_file = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file",
        project_id=PROJECT_ID,
        build=yaml.safe_load((Path(CURRENT_FOLDER) / "resources" / FILE_NAME_YAML).read_text()),
        params={"name": "Airflow"},
    )
    # [END howto_operator_gcp_create_build_from_yaml_body]
    # [START howto_operator_gcp_create_build_from_yaml_body_async]
    create_build_from_file_def = CloudBuildCreateBuildOperator(
        task_id="create_build_from_file_def",
        project_id=PROJECT_ID,
        build=yaml.safe_load((Path(CURRENT_FOLDER) / "resources" / FILE_NAME_YAML).read_text()),
        params={"name": "Airflow"},
        deferrable=True,
    )
    # [END howto_operator_gcp_create_build_from_yaml_body_async]
    # [START howto_operator_create_build_trigger]
    create_build_trigger = CloudBuildCreateBuildTriggerOperator(
        task_id="create_build_trigger", project_id=PROJECT_ID, trigger=create_build_trigger_body
    )
    # [END howto_operator_create_build_trigger]
    build_trigger_id = cast(str, XComArg(create_build_trigger, key="id"))
    # [START howto_operator_run_build_trigger]
    run_build_trigger = CloudBuildRunBuildTriggerOperator(
        task_id="run_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        source=create_build_from_repo_body["source"]["repo_source"],
    )
    # [END howto_operator_run_build_trigger]
    # [START howto_operator_update_build_trigger]
    update_build_trigger = CloudBuildUpdateBuildTriggerOperator(
        task_id="update_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
        trigger=update_build_trigger_body,
    )
    # [END howto_operator_update_build_trigger]
    # [START howto_operator_get_build_trigger]
    get_build_trigger = CloudBuildGetBuildTriggerOperator(
        task_id="get_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
    )
    # [END howto_operator_get_build_trigger]
    # [START howto_operator_list_build_triggers]
    list_build_triggers = CloudBuildListBuildTriggersOperator(
        task_id="list_build_triggers",
        project_id=PROJECT_ID,
        location="global",
        page_size=5,
    )
    # [END howto_operator_list_build_triggers]
    delete_bucket_src = GCSDeleteBucketOperator(
        task_id="delete_bucket_src",
        bucket_name=BUCKET_NAME_SRC,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [START howto_operator_delete_build_trigger]
    delete_build_trigger = CloudBuildDeleteBuildTriggerOperator(
        task_id="delete_build_trigger",
        project_id=PROJECT_ID,
        trigger_id=build_trigger_id,
    )
    # [END howto_operator_delete_build_trigger]
    delete_build_trigger.trigger_rule = TriggerRule.ALL_DONE

    chain(
        # TEST SETUP
        create_repo_task,
        create_bucket_src,
        upload_file,
        # TEST BODY
        create_build_from_storage,
        create_build_from_storage_result,
        create_build_from_storage_def,
        add_file_to_repo,
        create_build_from_repo,
        create_build_from_repo_result,
        create_build_from_repo_async,
        create_build_from_repo_result_async,
        list_builds,
        create_build_without_wait,
        cancel_build,
        retry_build,
        get_build,
        create_build_from_file,
        create_build_from_file_def,
        create_build_trigger,
        run_build_trigger,
        update_build_trigger,
        get_build_trigger,
        list_build_triggers,
        # TEST TEARDOWN
        delete_build_trigger,
        delete_bucket_src,
        delete_repo_task,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
