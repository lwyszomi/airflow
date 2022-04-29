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
import ast
import glob
import itertools
import mmap
import os
import unittest
from typing import Dict, Set

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


class TestProjectStructure(unittest.TestCase):
    def test_reference_to_providers_from_core(self):
        for filename in glob.glob(f"{ROOT_FOLDER}/example_dags/**/*.py", recursive=True):
            self.assert_file_not_contains(filename, "providers")

    def test_deprecated_packages(self):
        path_pattern = f"{ROOT_FOLDER}/airflow/contrib/**/*.py"

        for filename in glob.glob(path_pattern, recursive=True):
            if filename.endswith("/__init__.py"):
                self.assert_file_contains(filename, "This package is deprecated.")
            else:
                self.assert_file_contains(filename, "This module is deprecated.")

    def assert_file_not_contains(self, filename: str, pattern: str):
        with open(filename, 'rb', 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, 'utf-8')) != -1:
                self.fail(f"File {filename} not contains pattern - {pattern}")

    def assert_file_contains(self, filename: str, pattern: str):
        with open(filename, 'rb', 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, 'utf-8')) == -1:
                self.fail(f"File {filename} contains illegal pattern - {pattern}")

    def test_providers_modules_should_have_tests(self):
        """
        Assert every module in /airflow/providers has a corresponding test_ file in tests/airflow/providers.
        """
        # Deprecated modules that don't have corresponded test
        expected_missing_providers_modules = {
            (
                'airflow/providers/amazon/aws/hooks/aws_dynamodb.py',
                'tests/providers/amazon/aws/hooks/test_aws_dynamodb.py',
            )
        }

        # TODO: Should we extend this test to cover other directories?
        modules_files = glob.glob(f"{ROOT_FOLDER}/airflow/providers/**/*.py", recursive=True)

        # Make path relative
        modules_files = (os.path.relpath(f, ROOT_FOLDER) for f in modules_files)
        # Exclude example_dags
        modules_files = (f for f in modules_files if "/example_dags/" not in f)
        # Exclude __init__.py
        modules_files = (f for f in modules_files if not f.endswith("__init__.py"))
        # Change airflow/ to tests/
        expected_test_files = (
            f'tests/{f.partition("/")[2]}' for f in modules_files if not f.endswith("__init__.py")
        )
        # Add test_ prefix to filename
        expected_test_files = (
            f'{f.rpartition("/")[0]}/test_{f.rpartition("/")[2]}'
            for f in expected_test_files
            if not f.endswith("__init__.py")
        )

        current_test_files = glob.glob(f"{ROOT_FOLDER}/tests/providers/**/*.py", recursive=True)
        # Make path relative
        current_test_files = (os.path.relpath(f, ROOT_FOLDER) for f in current_test_files)
        # Exclude __init__.py
        current_test_files = (f for f in current_test_files if not f.endswith("__init__.py"))

        modules_files = set(modules_files)
        expected_test_files = set(expected_test_files)
        current_test_files = set(current_test_files)

        missing_tests_files = expected_test_files - expected_test_files.intersection(current_test_files)

        with self.subTest("Detect missing tests in providers module"):
            expected_missing_test_modules = {pair[1] for pair in expected_missing_providers_modules}
            missing_tests_files = missing_tests_files - set(expected_missing_test_modules)
            assert set() == missing_tests_files

        with self.subTest("Verify removed deprecated module also removed from deprecated list"):
            expected_missing_modules = {pair[0] for pair in expected_missing_providers_modules}
            removed_deprecated_module = expected_missing_modules - modules_files
            if removed_deprecated_module:
                self.fail(
                    "You've removed a deprecated module:\n"
                    f"{removed_deprecated_module}"
                    "\n"
                    "Thank you very much.\n"
                    "Can you remove it from the list of expected missing modules tests, please?"
                )


def get_imports_from_file(filepath: str):
    with open(filepath) as py_file:
        content = py_file.read()
    doc_node = ast.parse(content, filepath)
    import_names: Set[str] = set()
    for current_node in ast.walk(doc_node):
        if not isinstance(current_node, (ast.Import, ast.ImportFrom)):
            continue
        for alias in current_node.names:
            name = alias.name
            fullname = f'{current_node.module}.{name}' if isinstance(current_node, ast.ImportFrom) else name
            import_names.add(fullname)
    return import_names


def filepath_to_module(filepath: str):
    filepath = os.path.relpath(os.path.abspath(filepath), ROOT_FOLDER)
    return filepath.replace("/", ".")[: -(len('.py'))]


class ProjectStructureTest:
    PROVIDER = "dummy"
    OPERATOR_DIRS = {"operators", "sensors", "transfers"}

    def operator_paths(self):
        """Override this method if your operators are located under different paths"""
        for resource_type in self.OPERATOR_DIRS:
            python_files = glob.glob(
                f"{ROOT_FOLDER}/airflow/providers/{self.PROVIDER}/*/{resource_type}/**.py"
            )
            # Make path relative
            resource_files = (os.path.relpath(f, ROOT_FOLDER) for f in python_files)
            # Exclude __init__.py and pycache
            resource_files = (f for f in resource_files if not f.endswith("__init__.py"))
            yield from resource_files

    def list_of_operators(self):
        all_operators = {}
        for operator_file in self.operator_paths():
            operators_paths = self.get_classes_from_file(f"{ROOT_FOLDER}/{operator_file}")
            all_operators.update(operators_paths)
        return all_operators

    @staticmethod
    def get_classes_from_file(filepath: str):
        with open(filepath) as py_file:
            content = py_file.read()
        doc_node = ast.parse(content, filepath)
        module = filepath_to_module(filepath)
        results: Dict = {}
        for current_node in ast.walk(doc_node):
            if not isinstance(current_node, ast.ClassDef):
                continue
            name = current_node.name
            if (
                not name.endswith("Operator")
                and not name.endswith("Sensor")
                and not name.endswith("Operator")
            ):
                continue
            results[f"{module}.{name}"] = current_node
        return results


class ExampleCoverageTest(ProjectStructureTest):
    # Those operators are deprecated, so we do not need examples for them
    DEPRECATED_OPERATORS: Set = set()

    # Those operators should not have examples as they are never used standalone (they are abstract)
    BASE_OPERATORS: Set = set()

    # Please add the examples to those operators at the earliest convenience :)
    MISSING_EXAMPLES_FOR_OPERATORS: Set = set()

    def example_paths(self):
        """Override this method if your example dags are located elsewhere"""
        # old_design:
        yield from glob.glob(f"{ROOT_FOLDER}/airflow/providers/{self.PROVIDER}/*/example_dags/example_*.py")
        # new_design:
        yield from glob.glob(f"{ROOT_FOLDER}/tests/system/providers/{self.PROVIDER}/**/example_*.py")

    def test_missing_example_for_operator(self):
        """
        Assert that all operators defined under operators, sensors and transfers directories
        are used in any of the example dags
        """
        all_operators = self.list_of_operators()
        all_operators = set(all_operators.keys())
        for example in self.example_paths():
            all_operators -= get_imports_from_file(example)

        covered_but_omitted = self.MISSING_EXAMPLES_FOR_OPERATORS - all_operators
        all_operators -= self.MISSING_EXAMPLES_FOR_OPERATORS
        all_operators -= self.DEPRECATED_OPERATORS
        all_operators -= self.BASE_OPERATORS
        assert set() == all_operators, (
            "Not all operators are covered with example dags. "
            "Update self.MISSING_EXAMPLES_FOR_OPERATORS if you want to skip this error"
        )
        assert set() == covered_but_omitted, "Operator listed in missing examples but is used in example dag"


class AssetsCoverageTest(ProjectStructureTest):

    # These operators should not have assets
    ASSETS_NOT_REQUIRED: Set = set()

    def test_missing_assets_for_operator(self):
        all_operators = self.list_of_operators()
        assets, no_assets = set(), set()
        for name, operator in all_operators.items():
            for attr in operator.body:
                if (
                    isinstance(attr, ast.Assign)
                    and attr.targets
                    and getattr(attr.targets[0], "id", "") == "operator_extra_links"
                ):
                    assets.add(name)
                    break
            else:
                no_assets.add(name)

        asset_should_be_missing = self.ASSETS_NOT_REQUIRED - no_assets
        no_assets -= self.ASSETS_NOT_REQUIRED
        # TODO: (bhirsz): uncomment when we reach full coverage
        # assert set() == no_assets, "Operator is missing assets"
        assert set() == asset_should_be_missing, "Operator should not have assets"


class TestGoogleProviderProjectStructure(ExampleCoverageTest, AssetsCoverageTest):
    PROVIDER = "google"
    OPERATOR_DIRS = ProjectStructureTest.OPERATOR_DIRS | {"operators/vertex_ai"}

    DEPRECATED_OPERATORS = {
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service'
        '.CloudDataTransferServiceS3ToGCSOperator',
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service'
        '.CloudDataTransferServiceGCSToGCSOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHadoopJobOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocScaleClusterOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkJobOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitSparkSqlJobOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitHiveJobOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPigJobOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocSubmitPySparkJobOperator',
        'airflow.providers.google.cloud.operators.mlengine.MLEngineManageModelOperator',
        'airflow.providers.google.cloud.operators.mlengine.MLEngineManageVersionOperator',
        'airflow.providers.google.cloud.operators.dataflow.DataflowCreateJavaJobOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryPatchDatasetOperator',
        'airflow.providers.google.cloud.operators.dataflow.DataflowCreatePythonJobOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryExecuteQueryOperator',
    }

    BASE_OPERATORS = {
        'airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator',
        'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocJobBaseOperator',
        'airflow.providers.google.cloud.operators.vertex_ai.custom_job.CustomTrainingJobBaseOperator',
    }

    MISSING_EXAMPLES_FOR_OPERATORS = {
        'airflow.providers.google.cloud.operators.mlengine.MLEngineTrainingCancelJobOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPGetStoredInfoTypeOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDeidentifyTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPCreateDLPJobOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateDeidentifyTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobTriggerOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPListDeidentifyTemplatesOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDeidentifyTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPListInspectTemplatesOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPListStoredInfoTypesOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPUpdateInspectTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPListJobTriggersOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPCancelDLPJobOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPGetDLPJobOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPGetInspectTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPListInfoTypesOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPListDLPJobsOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator',
        'airflow.providers.google.cloud.transfers.cassandra_to_gcs.CassandraToGCSOperator',
        'airflow.providers.google.cloud.transfers.adls_to_gcs.ADLSToGCSOperator',
        'airflow.providers.google.cloud.transfers.bigquery_to_mysql.BigQueryToMySqlOperator',
        'airflow.providers.google.cloud.transfers.sql_to_gcs.BaseSQLToGCSOperator',
        'airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.GetEndpointOperator',
        'airflow.providers.google.cloud.operators.vertex_ai.auto_ml.AutoMLTrainingJobBaseOperator',
        'airflow.providers.google.cloud.operators.vertex_ai.endpoint_service.UpdateEndpointOperator',
        'airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job.'
        'GetBatchPredictionJobOperator',
    }

    # These operators should not have assets
    MISSING_ASSETS_FOR_OPERATORS = {
        'airflow.providers.google.cloud.operators.automl.AutoMLDeleteDatasetOperator',
        'airflow.providers.google.cloud.operators.automl.AutoMLDeleteModelOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryCheckOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteDatasetOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryDeleteTableOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryIntervalCheckOperator',
        'airflow.providers.google.cloud.operators.bigquery.BigQueryValueCheckOperator',
        'airflow.providers.google.cloud.operators.bigquery_dts.BigQueryDeleteDataTransferConfigOperator',
        'airflow.providers.google.cloud.operators.bigtable.BigtableDeleteInstanceOperator',
        'airflow.providers.google.cloud.operators.bigtable.BigtableDeleteTableOperator',
        'airflow.providers.google.cloud.operators.cloud_build.CloudBuildDeleteBuildTriggerOperator',
        'airflow.providers.google.cloud.operators.cloud_memorystore.CloudMemorystoreDeleteInstanceOperator',
        'airflow.providers.google.cloud.operators.cloud_memorystore.'
        'CloudMemorystoreMemcachedDeleteInstanceOperator',
        'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLBaseOperator',
        'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceDatabaseOperator',
        'airflow.providers.google.cloud.operators.cloud_sql.CloudSQLDeleteInstanceOperator',
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
        'CloudDataTransferServiceDeleteJobOperator',
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
        'CloudDataTransferServiceGetOperationOperator',
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
        'CloudDataTransferServiceListOperationsOperator',
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
        'CloudDataTransferServicePauseOperationOperator',
        'airflow.providers.google.cloud.operators.cloud_storage_transfer_service.'
        'CloudDataTransferServiceResumeOperationOperator',
        'airflow.providers.google.cloud.operators.compute.ComputeEngineBaseOperator',
        'airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryGroupOperator',
        'airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteEntryOperator',
        'airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagOperator',
        'airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateFieldOperator',
        'airflow.providers.google.cloud.operators.datacatalog.CloudDataCatalogDeleteTagTemplateOperator',
        'airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeleteInstanceOperator',
        'airflow.providers.google.cloud.operators.datafusion.CloudDataFusionDeletePipelineOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocDeleteBatchOperator',
        'airflow.providers.google.cloud.operators.dataproc.DataprocDeleteClusterOperator',
        'airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreDeleteBackupOperator',
        'airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreDeleteServiceOperator',
        'airflow.providers.google.cloud.operators.datastore.CloudDatastoreBeginTransactionOperator',
        'airflow.providers.google.cloud.operators.datastore.CloudDatastoreDeleteOperationOperator',
        'airflow.providers.google.cloud.operators.datastore.CloudDatastoreGetOperationOperator',
        'airflow.providers.google.cloud.operators.datastore.CloudDatastoreRollbackOperator',
        'airflow.providers.google.cloud.operators.datastore.CloudDatastoreRunQueryOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeidentifyContentOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDLPJobOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteDeidentifyTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteInspectTemplateOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteJobTriggerOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPDeleteStoredInfoTypeOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPInspectContentOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPRedactImageOperator',
        'airflow.providers.google.cloud.operators.dlp.CloudDLPReidentifyContentOperator',
        'airflow.providers.google.cloud.operators.functions.CloudFunctionDeleteFunctionOperator',
        'airflow.providers.google.cloud.operators.gcs.GCSDeleteBucketOperator',
        'airflow.providers.google.cloud.operators.gcs.GCSDeleteObjectsOperator',
        'airflow.providers.google.cloud.operators.kubernetes_engine.GKEDeleteClusterOperator',
        'airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteModelOperator',
        'airflow.providers.google.cloud.operators.mlengine.MLEngineDeleteVersionOperator',
        'airflow.providers.google.cloud.operators.pubsub.PubSubDeleteSubscriptionOperator',
        'airflow.providers.google.cloud.operators.pubsub.PubSubDeleteTopicOperator',
        'airflow.providers.google.cloud.operators.spanner.SpannerDeleteDatabaseInstanceOperator',
        'airflow.providers.google.cloud.operators.spanner.SpannerDeleteInstanceOperator',
        'airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteAlertOperator',
        'airflow.providers.google.cloud.operators.stackdriver.StackdriverDeleteNotificationChannelOperator',
        'airflow.providers.google.cloud.operators.tasks.CloudTasksQueueDeleteOperator',
        'airflow.providers.google.cloud.operators.tasks.CloudTasksTaskDeleteOperator',
        'airflow.providers.google.cloud.operators.translate.CloudTranslateTextOperator',
        'airflow.providers.google.cloud.operators.translate_speech.CloudTranslateSpeechOperator',
        'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductOperator',
        'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteProductSetOperator',
        'airflow.providers.google.cloud.operators.vision.CloudVisionDeleteReferenceImageOperator',
        'airflow.providers.google.cloud.operators.workflows.WorkflowsDeleteWorkflowOperator',
        'airflow.providers.google.marketing_platform.sensors.campaign_manager.'
        'GoogleCampaignManagerReportSensor',
        'airflow.providers.google.marketing_platform.sensors.display_video.'
        'GoogleDisplayVideo360GetSDFDownloadOperationSensor',
        'airflow.providers.google.marketing_platform.sensors.display_video.'
        'GoogleDisplayVideo360ReportSensor',
        'airflow.providers.google.marketing_platform.sensors.search_ads.GoogleSearchAdsReportSensor',
    }


class TestOperatorsHooks(unittest.TestCase):
    def test_no_illegal_suffixes(self):
        illegal_suffixes = ["_operator.py", "_hook.py", "_sensor.py"]
        files = itertools.chain(
            *(
                glob.glob(f"{ROOT_FOLDER}/{part}/providers/**/{resource_type}/*.py", recursive=True)
                for resource_type in ["operators", "hooks", "sensors", "example_dags"]
                for part in ["airflow", "tests"]
            )
        )

        invalid_files = [f for f in files if any(f.endswith(suffix) for suffix in illegal_suffixes)]

        assert [] == invalid_files
