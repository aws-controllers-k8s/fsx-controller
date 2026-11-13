# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for the FSx DataRepositoryAssociation resource.

A DRA needs a Lustre file system that supports associations (not SCRATCH_1),
so the module stands up its own PERSISTENT_2 FileSystem CR from the shared
file_system template and links it to the bootstrapped S3 bucket through
fileSystemRef.

The whole lifecycle is one test. The suite runs under pytest-xdist, where a
module-scoped fixture is built once per worker and LoadScheduling does not keep
the order. Split into four tests, the steps each started their own 1200 GiB file
system and DRA, and test_delete could run before test_create.
"""

import logging
import time

import pytest

from acktest import tags
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from acktest.resources import random_suffix_name

from e2e import CRD_GROUP, CRD_VERSION, load_fsx_resource, service_marker
from e2e.replacement_values import REPLACEMENT_VALUES
from e2e.tests.helper import (
    DataRepositoryAssociationValidator,
    FSxValidator,
    wait_for_synced_or_terminal,
)

FS_RESOURCE_PLURAL = "filesystems"
DRA_RESOURCE_PLURAL = "datarepositoryassociations"

FS_STORAGE_CAPACITY = 1200
FS_MAINTENANCE_WINDOW = "2:03:00"

FILE_SYSTEM_PATH = "/ack-e2e"
DATA_REPOSITORY_PREFIX = "ack-e2e"
INITIAL_CHUNK_SIZE = 1024
UPDATED_CHUNK_SIZE = 2048

TAG_KEY = "ack-fsx-e2e"
INITIAL_TAG_VALUE = "initial"
UPDATED_TAG_VALUE = "updated"
SECOND_TAG_KEY = "ack-fsx-e2e-second"
SECOND_TAG_VALUE = "second"
NEW_TAG_KEY = "ack-fsx-e2e-new"
NEW_TAG_VALUE = "new"

# User tags at create time.
INITIAL_TAGS = {TAG_KEY: INITIAL_TAG_VALUE, SECOND_TAG_KEY: SECOND_TAG_VALUE}
# User tags after the tag step: TAG_KEY changed, SECOND_TAG_KEY removed and
# NEW_TAG_KEY added.
UPDATED_TAGS = {TAG_KEY: UPDATED_TAG_VALUE, NEW_TAG_KEY: NEW_TAG_VALUE}

# File system: 5-10 min to AVAILABLE and 5-10 min to disappear.
FS_CREATE_WAIT_PERIODS = 40
FS_CREATE_PERIOD_LENGTH = 30
FS_DELETE_WAIT_PERIODS = 30
FS_DELETE_PERIOD_LENGTH = 30

# DRA lifecycle states that will never become AVAILABLE without a Spec or
# bucket change. The controller marks FAILED as Terminal, but MISCONFIGURED is
# left retryable (the fix is usually bucket-side), so the test checks both
# itself to fail fast and report failureDetails.
DRA_FAILED_LIFECYCLES = ("FAILED", "MISCONFIGURED")

# An earlier run saw associations stay CREATING for more than 20 minutes, with
# four created at once against one bucket. The wait is long, but a FAILED or
# MISCONFIGURED association still fails within one poll.
CREATE_WAIT_AFTER_SECONDS = 30
DRA_CREATE_WAIT_PERIODS = 120
DRA_CREATE_PERIOD_LENGTH = 15
MODIFY_WAIT_AFTER_SECONDS = 15
DRA_MODIFY_WAIT_PERIODS = 40
DRA_MODIFY_PERIOD_LENGTH = 15
DRA_DELETE_WAIT_PERIODS = 40
DRA_DELETE_PERIOD_LENGTH = 15


def _wait_for_dra_synced(ref, wait_periods, period_length):
    """wait_for_synced_or_terminal, but also fails as soon as the association
    reaches a lifecycle state that cannot become AVAILABLE, and reports
    FSx's FailureDetails.
    """
    for _ in range(wait_periods):
        cr = k8s.get_resource(ref) or {}
        status = cr.get("status", {})
        lifecycle = status.get("lifecycle")
        if lifecycle in DRA_FAILED_LIFECYCLES:
            raise AssertionError(
                f"{ref.name} reached lifecycle {lifecycle}: "
                f"{status.get('failureDetails')}"
            )
        conditions = status.get("conditions") or []
        by_type = {c["type"]: c for c in conditions}
        synced = by_type.get(condition.CONDITION_TYPE_RESOURCE_SYNCED)
        if synced is not None and synced.get("status") == "True":
            return
        terminal = by_type.get(condition.CONDITION_TYPE_TERMINAL)
        if terminal is not None and terminal.get("status") == "True":
            raise AssertionError(
                f"{ref.name} went terminal: {terminal.get('message')}"
            )
        time.sleep(period_length)
    cr = k8s.get_resource(ref) or {}
    raise AssertionError(
        f"{ref.name} did not sync within {wait_periods * period_length}s; "
        f"lifecycle {cr.get('status', {}).get('lifecycle')}"
    )


def _delete_cr(ref, wait_periods, period_length):
    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(
            ref, wait_periods=wait_periods, period_length=period_length,
        )
        assert deleted
        assert not k8s.get_resource_exists(ref)


@pytest.fixture(scope="module")
def lustre_file_system(fsx_client):
    resource_name = random_suffix_name("ack-fsx-dra-fs", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["FILE_SYSTEM_NAME"] = resource_name
    replacements["STORAGE_CAPACITY"] = str(FS_STORAGE_CAPACITY)
    replacements["WEEKLY_MAINTENANCE_START_TIME"] = FS_MAINTENANCE_WINDOW
    replacements["TAG_KEY"] = TAG_KEY
    replacements["TAG_VALUE"] = INITIAL_TAG_VALUE
    replacements["SECOND_TAG_KEY"] = SECOND_TAG_KEY
    replacements["SECOND_TAG_VALUE"] = SECOND_TAG_VALUE

    resource_data = load_fsx_resource(
        "file_system", additional_replacements=replacements,
    )
    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, FS_RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)

    # Wrapped so a mid-setup failure still tears down the 1200 GiB file system.
    file_system_id = None
    try:
        k8s.wait_resource_consumed_by_controller(ref)
        wait_for_synced_or_terminal(
            ref,
            wait_periods=FS_CREATE_WAIT_PERIODS,
            period_length=FS_CREATE_PERIOD_LENGTH,
        )
        file_system_id = k8s.get_resource(ref)["status"]["id"]
        yield (resource_name, file_system_id)
    finally:
        if file_system_id is None and k8s.get_resource_exists(ref):
            cr = k8s.get_resource(ref) or {}
            file_system_id = cr.get("status", {}).get("id")
        _delete_cr(ref, FS_DELETE_WAIT_PERIODS, FS_DELETE_PERIOD_LENGTH)
        if file_system_id is not None:
            assert FSxValidator(fsx_client).wait_until_gone(file_system_id)


@pytest.fixture(scope="module")
def data_repository_association(fsx_client, lustre_file_system):
    """Creates the DRA and waits for sync. Teardown runs before the file
    system's, since a file system with associations cannot be deleted cleanly.
    test_crud normally deletes the DRA itself; teardown is the safety net.
    """
    (file_system_name, file_system_id) = lustre_file_system
    resource_name = random_suffix_name("ack-fsx-dra", 32)

    replacements = REPLACEMENT_VALUES.copy()
    replacements["DRA_NAME"] = resource_name
    replacements["FILE_SYSTEM_NAME"] = file_system_name
    replacements["FILE_SYSTEM_PATH"] = FILE_SYSTEM_PATH
    # A unique prefix per run, so associations from concurrent or leaked runs
    # never link the same S3 prefix (FSx manages bucket event notifications
    # per prefix for auto-import).
    replacements["DATA_REPOSITORY_PREFIX"] = random_suffix_name(
        DATA_REPOSITORY_PREFIX, 24,
    )
    replacements["IMPORTED_FILE_CHUNK_SIZE"] = str(INITIAL_CHUNK_SIZE)
    replacements["TAG_KEY"] = TAG_KEY
    replacements["TAG_VALUE"] = INITIAL_TAG_VALUE
    replacements["SECOND_TAG_KEY"] = SECOND_TAG_KEY
    replacements["SECOND_TAG_VALUE"] = SECOND_TAG_VALUE

    resource_data = load_fsx_resource(
        "data_repository_association", additional_replacements=replacements,
    )
    logging.debug(resource_data)

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, DRA_RESOURCE_PLURAL,
        resource_name, namespace="default",
    )
    k8s.create_custom_resource(ref, resource_data)

    association_id = None
    try:
        time.sleep(CREATE_WAIT_AFTER_SECONDS)
        cr = k8s.wait_resource_consumed_by_controller(ref)
        assert cr is not None
        cr = k8s.get_resource(ref) or {}
        association_id = cr.get("status", {}).get("id")
        _wait_for_dra_synced(
            ref,
            wait_periods=DRA_CREATE_WAIT_PERIODS,
            period_length=DRA_CREATE_PERIOD_LENGTH,
        )
        cr = k8s.get_resource(ref)
        association_id = cr["status"]["id"]
        yield (ref, file_system_id, association_id,
               replacements["DATA_REPOSITORY_PREFIX"])
    finally:
        if association_id is None and k8s.get_resource_exists(ref):
            cr = k8s.get_resource(ref) or {}
            association_id = cr.get("status", {}).get("id")
        _delete_cr(ref, DRA_DELETE_WAIT_PERIODS, DRA_DELETE_PERIOD_LENGTH)
        if association_id is not None:
            assert DataRepositoryAssociationValidator(fsx_client).wait_until_gone(
                association_id
            )


@service_marker
class TestDataRepositoryAssociation:
    def test_crud(self, fsx_client, data_repository_association):
        (ref, file_system_id, association_id, prefix) = data_repository_association
        validator = DataRepositoryAssociationValidator(fsx_client)
        self._assert_created(ref, validator, file_system_id, association_id, prefix)
        self._update_and_assert(ref, validator, association_id)
        self._remove_policy_rejected(ref, validator, association_id)
        self._update_tags_and_assert(ref, validator, association_id)
        self._delete_and_assert(ref, validator, association_id)

    def _assert_created(self, ref, validator, file_system_id, association_id, prefix):
        assert association_id is not None

        # Verify via the CR.
        condition.assert_synced(ref)
        cr = k8s.get_resource(ref)
        assert cr["status"]["lifecycle"] == "AVAILABLE"
        assert cr["status"]["ackResourceMetadata"]["arn"] is not None
        # The runtime clears a reference-resolved field before persisting the
        # spec, so spec.fileSystemID stays unset when fileSystemRef is used.
        # The AWS check below proves the reference resolved.
        assert cr["spec"]["fileSystemRef"]["from"]["name"]
        assert cr["spec"]["importedFileChunkSize"] == INITIAL_CHUNK_SIZE
        # Late-initialized from the server default.
        assert cr["spec"]["batchImportMetaDataOnCreate"] is False

        # Verify via the AWS API.
        dra = validator.get(association_id)
        assert dra is not None
        assert dra["Lifecycle"] == "AVAILABLE"
        assert dra["FileSystemId"] == file_system_id
        assert dra["FileSystemPath"] == FILE_SYSTEM_PATH
        assert dra["DataRepositoryPath"].rstrip("/").endswith(prefix)
        assert dra["ImportedFileChunkSize"] == INITIAL_CHUNK_SIZE
        assert dra["S3"]["AutoImportPolicy"]["Events"] == ["NEW"]
        aws_tags = validator.get_tags(association_id)
        tags.assert_ack_system_tags(tags=aws_tags)
        tags.assert_equal_without_ack_tags(expected=INITIAL_TAGS, actual=aws_tags)
        # FilterSystemTags must keep the ACK system tags out of spec.tags.
        tags.assert_equal(
            expected=INITIAL_TAGS, actual=cr["spec"]["tags"],
            key_member_name="key", value_member_name="value",
        )

        # Describe must not have rewritten the declared paths (no perpetual
        # delta from server-side normalization).
        assert cr["spec"]["fileSystemPath"] == dra["FileSystemPath"]
        assert cr["spec"]["dataRepositoryPath"] == dra["DataRepositoryPath"]

    def _update_and_assert(self, ref, validator, association_id):
        """ImportedFileChunkSize and S3 go through
        UpdateDataRepositoryAssociation.
        """
        k8s.patch_custom_resource(ref, {
            "spec": {
                "importedFileChunkSize": UPDATED_CHUNK_SIZE,
                "s3": {
                    "autoImportPolicy": {"events": ["NEW", "CHANGED"]},
                    "autoExportPolicy": {"events": ["NEW"]},
                },
            },
        })
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Synced may still be True from the pre-update reconcile; poll AWS.
        assert validator.wait_until(
            association_id,
            lambda d: d.get("Lifecycle") == "AVAILABLE"
            and d.get("ImportedFileChunkSize") == UPDATED_CHUNK_SIZE
            and sorted(d["S3"]["AutoImportPolicy"]["Events"]) == ["CHANGED", "NEW"]
            and d["S3"].get("AutoExportPolicy", {}).get("Events") == ["NEW"],
            "updated",
            wait_periods=DRA_MODIFY_WAIT_PERIODS,
            period_length=DRA_MODIFY_PERIOD_LENGTH,
        )
        assert k8s.wait_on_condition(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            wait_periods=DRA_MODIFY_WAIT_PERIODS,
            period_length=DRA_MODIFY_PERIOD_LENGTH,
        )
        cr = k8s.get_resource(ref)
        assert cr["spec"]["importedFileChunkSize"] == UPDATED_CHUNK_SIZE
        assert cr["status"]["lifecycle"] == "AVAILABLE"
        # UpdateDataRepositoryAssociation carries no tags; the update must not
        # clear or roll back spec.tags, nor untag in AWS.
        aws_tags = validator.get_tags(association_id)
        tags.assert_ack_system_tags(tags=aws_tags)
        tags.assert_equal_without_ack_tags(expected=INITIAL_TAGS, actual=aws_tags)
        tags.assert_equal(
            expected=INITIAL_TAGS, actual=cr["spec"]["tags"],
            key_member_name="key", value_member_name="value",
        )

    def _remove_policy_rejected(self, ref, validator, association_id):
        """Removing autoExportPolicy cannot be carried by
        UpdateDataRepositoryAssociation, so the controller sets Terminal
        instead of re-sending an Update forever. Restoring the policy clears
        it.
        """
        k8s.patch_custom_resource(ref, {
            "spec": {"s3": {"autoExportPolicy": None}},
        })
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        assert k8s.wait_on_condition(
            ref, condition.CONDITION_TYPE_TERMINAL, "True",
            wait_periods=DRA_MODIFY_WAIT_PERIODS,
            period_length=DRA_MODIFY_PERIOD_LENGTH,
        )
        dra = validator.get(association_id)
        assert dra["S3"].get("AutoExportPolicy", {}).get("Events") == ["NEW"]

        k8s.patch_custom_resource(ref, {
            "spec": {"s3": {"autoExportPolicy": {"events": ["NEW"]}}},
        })
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert k8s.wait_on_condition(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            wait_periods=DRA_MODIFY_WAIT_PERIODS,
            period_length=DRA_MODIFY_PERIOD_LENGTH,
        )

    def _update_tags_and_assert(self, ref, validator, association_id):
        """Tags go through TagResource/UntagResource, not the Update API.

        The step changes TAG_KEY, removes SECOND_TAG_KEY and adds NEW_TAG_KEY,
        so both TagResource and UntagResource run. The ACK system tags must
        survive the removal.
        """
        k8s.patch_custom_resource(ref, {
            "spec": {"tags": [
                {"key": TAG_KEY, "value": UPDATED_TAG_VALUE},
                {"key": NEW_TAG_KEY, "value": NEW_TAG_VALUE},
            ]},
        })
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)

        # Exact match on the non-ACK tags, so this also waits for the removed
        # key to disappear.
        assert validator.wait_until(
            association_id,
            lambda d: tags.clean(
                {t["Key"]: t["Value"] for t in d.get("Tags", [])}
            ) == UPDATED_TAGS,
            "re-tagged",
            wait_periods=DRA_MODIFY_WAIT_PERIODS,
            period_length=DRA_MODIFY_PERIOD_LENGTH,
        )
        assert k8s.wait_on_condition(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            wait_periods=DRA_MODIFY_WAIT_PERIODS,
            period_length=DRA_MODIFY_PERIOD_LENGTH,
        )

        aws_tags = validator.get_tags(association_id)
        tags.assert_ack_system_tags(tags=aws_tags)
        tags.assert_equal_without_ack_tags(expected=UPDATED_TAGS, actual=aws_tags)
        cr = k8s.get_resource(ref)
        tags.assert_equal(
            expected=UPDATED_TAGS, actual=cr["spec"]["tags"],
            key_member_name="key", value_member_name="value",
        )

    def _delete_and_assert(self, ref, validator, association_id):
        """Deletes without the delete-data annotation. That is the default
        path, which the service rejected while the member was left nil. The
        finalizer must be held until FSx has fully removed the association.
        """
        _delete_cr(ref, DRA_DELETE_WAIT_PERIODS, DRA_DELETE_PERIOD_LENGTH)

        # The CR is only gone once Describe reports NotFound, so AWS must
        # agree right away (allow a short eventual-consistency window).
        assert validator.wait_until_gone(
            association_id, wait_periods=4, period_length=15,
        )
