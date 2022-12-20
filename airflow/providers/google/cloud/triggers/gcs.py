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
from __future__ import annotations

from typing import Any, AsyncIterator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class GCSUploadObjectsTrigger(BaseTrigger):
    """
    GCSTrigger run on the trigger worker to perform upload operation

    :param conn_id: Reference to google cloud connection id
    """

    def __init__(
        self,
        conn_id: str,
        # job_id: str | None,
        # project_id: str | None,
        # dataset_id: str | None = None,
        # table_id: str | None = None,
        # poll_interval: float = 4.0,
    ):
        super().__init__()
        self.log.info("Using the connection  %s .", conn_id)
        self.conn_id = conn_id
        # self.job_id = job_id
        # self._job_conn = None
        # self.dataset_id = dataset_id
        # self.project_id = project_id
        # self.table_id = table_id
        # self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes BigQueryInsertJobTrigger arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gcs.BigQueryInsertJobTrigger",
            {
                "conn_id": self.conn_id,
                # "job_id": self.job_id,
                # "dataset_id": self.dataset_id,
                # "project_id": self.project_id,
                # "table_id": self.table_id,
                # "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """Gets current job execution status and yields a TriggerEvent"""
        ...

    def _get_async_hook(self) -> BigQueryAsyncHook:
        return BigQueryAsyncHook(gcp_conn_id=self.conn_id)
