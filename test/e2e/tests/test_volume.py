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

"""Integration tests for the FSx Volume resource.

Two topologies, one test class each:

* ONTAP: an ONTAP FileSystem CR, an SVM CR (fileSystemRef) and a Volume CR
  (storageVirtualMachineRef).
* OpenZFS: an OpenZFS FileSystem CR, a Volume CR whose parent is the root
  volume (read through boto3; it is not a CR), and a grandchild Volume CR
  that uses parentVolumeRef.

Both volumes are created with only the required members, so the tests also
check, against Describe, the late_initialize list in generator.yaml: every
server-defaulted member must end up in the CR spec equal to what FSx
reports, and no reconcile may issue an idle UpdateVolume. Each test logs the
observed Describe output for the PR record.

As in the other FSx tests, each lifecycle is one test so pytest-xdist cannot
reorder the steps or build a second file system.
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
    FSxValidator,
    StorageVirtualMachineValidator,
    VolumeValidator,
    get_controller_api_call_count,
    wait_for_synced_or_terminal,
)

FS_RESOURCE_PLURAL = "filesystems"
SVM_RESOURCE_PLURAL = "storagevirtualmachines"
VOLUME_RESOURCE_PLURAL = "volumes"

SKIP_FINAL_BACKUP_ANNOTATION = "fsx.services.k8s.aws/skip-final-backup"

TAG_KEY = "ack-fsx-e2e"
INITIAL_TAG_VALUE = "initial"
UPDATED_TAG_VALUE = "updated"
SECOND_TAG_KEY = "ack-fsx-e2e-second"
SECOND_TAG_VALUE = "second"
NEW_TAG_KEY = "ack-fsx-e2e-new"
NEW_TAG_VALUE = "new"

INITIAL_TAGS = {TAG_KEY: INITIAL_TAG_VALUE, SECOND_TAG_KEY: SECOND_TAG_VALUE}
# TAG_KEY changed, SECOND_TAG_KEY removed and NEW_TAG_KEY added.
UPDATED_TAGS = {TAG_KEY: UPDATED_TAG_VALUE, NEW_TAG_KEY: NEW_TAG_VALUE}

GIB = 1024 ** 3
ONTAP_INITIAL_SIZE = 20 * GIB
ONTAP_UPDATED_SIZE = 30 * GIB
ONTAP_UPDATED_SNAPSHOT_POLICY = "none"

OPENZFS_UPDATED_QUOTA_GIB = 10
OPENZFS_UPDATED_COMPRESSION = "LZ4"

# File systems: ONTAP up to ~30 min to AVAILABLE, OpenZFS ~20 min; ~10 min to
# disappear.
FS_CREATE_WAIT_PERIODS = 80
FS_CREATE_PERIOD_LENGTH = 30
FS_DELETE_WAIT_PERIODS = 40
FS_DELETE_PERIOD_LENGTH = 30

CREATE_WAIT_AFTER_SECONDS = 30
SVM_CREATE_WAIT_PERIODS = 80
SVM_CREATE_PERIOD_LENGTH = 15
SVM_DELETE_WAIT_PERIODS = 60
SVM_DELETE_PERIOD_LENGTH = 15

VOLUME_CREATE_WAIT_PERIODS = 60
VOLUME_CREATE_PERIOD_LENGTH = 15
MODIFY_WAIT_AFTER_SECONDS = 15
VOLUME_MODIFY_WAIT_PERIODS = 40
VOLUME_MODIFY_PERIOD_LENGTH = 15
# Without skip-final-backup an ONTAP delete waits for the final backup.
VOLUME_DELETE_WAIT_PERIODS = 80
VOLUME_DELETE_PERIOD_LENGTH = 15
LATE_INIT_WAIT_PERIODS = 20
LATE_INIT_PERIOD_LENGTH = 10


def _delete_cr(ref, wait_periods, period_length):
    if k8s.get_resource_exists(ref):
        _, deleted = k8s.delete_custom_resource(
            ref, wait_periods=wait_periods, period_length=period_length,
        )
        assert deleted
        assert not k8s.get_resource_exists(ref)


def _ref(plural, name):
    return k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, plural, name, namespace="default",
    )


def _wait_for_cr(ref, predicate, description):
    for _ in range(LATE_INIT_WAIT_PERIODS):
        cr = k8s.get_resource(ref)
        if cr is not None and predicate(cr):
            return cr
        logging.info(f"{ref.name} not yet {description}")
        time.sleep(LATE_INIT_PERIOD_LENGTH)
    raise AssertionError(f"{ref.name} never {description}: {k8s.get_resource(ref)}")


def _create_file_system(fsx_client, template, prefix):
    """Generator shared by the file system fixtures: yields (name, id) and
    tears the file system down afterwards."""
    resource_name = random_suffix_name(prefix, 32)
    replacements = REPLACEMENT_VALUES.copy()
    replacements["FILE_SYSTEM_NAME"] = resource_name
    resource_data = load_fsx_resource(template, additional_replacements=replacements)
    ref = _ref(FS_RESOURCE_PLURAL, resource_name)
    k8s.create_custom_resource(ref, resource_data)

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


def _create_volume(fsx_client, template, replacements):
    """Creates a Volume CR, waits for sync, and returns (ref, volume_id).
    Callers own the deletion."""
    resource_data = load_fsx_resource(template, additional_replacements=replacements)
    logging.debug(resource_data)
    ref = _ref(VOLUME_RESOURCE_PLURAL, replacements["VOLUME_CR_NAME"])
    k8s.create_custom_resource(ref, resource_data)
    time.sleep(CREATE_WAIT_AFTER_SECONDS)
    assert k8s.wait_resource_consumed_by_controller(ref) is not None
    wait_for_synced_or_terminal(
        ref,
        wait_periods=VOLUME_CREATE_WAIT_PERIODS,
        period_length=VOLUME_CREATE_PERIOD_LENGTH,
    )
    return ref, k8s.get_resource(ref)["status"]["id"]


def _cleanup_volume(fsx_client, ref):
    volume_id = None
    if k8s.get_resource_exists(ref):
        volume_id = (k8s.get_resource(ref) or {}).get("status", {}).get("id")
    _delete_cr(ref, VOLUME_DELETE_WAIT_PERIODS, VOLUME_DELETE_PERIOD_LENGTH)
    if volume_id is not None:
        assert VolumeValidator(fsx_client).wait_until_gone(volume_id)


def _assert_tags(validator, volume_id, cr, expected):
    """Tags must match in AWS (ListTagsForResource) and in the CR spec.

    DescribeVolumes does not report volume tags (e2e: Describe returned no
    Tags while ListTagsForResource had them), so the controller reads them
    with ListTagsForResource, as it does for StorageVirtualMachine.
    """
    listed = validator.list_tags(volume_id)
    tags.assert_ack_system_tags(tags=listed)
    tags.assert_equal_without_ack_tags(expected=expected, actual=listed)
    tags.assert_equal(
        expected=expected, actual=cr["spec"].get("tags"),
        key_member_name="key", value_member_name="value",
    )


def _patch_tags_and_assert(ref, validator, volume_id, new_tags):
    k8s.patch_custom_resource(ref, {
        "spec": {"tags": [{"key": k, "value": v} for k, v in new_tags.items()]},
    })
    time.sleep(MODIFY_WAIT_AFTER_SECONDS)
    # Synced may still be True from the pre-update reconcile; poll AWS.
    assert validator.wait_until(
        volume_id,
        lambda _: tags.clean(validator.list_tags(volume_id)) == new_tags,
        "re-tagged",
        wait_periods=VOLUME_MODIFY_WAIT_PERIODS,
        period_length=VOLUME_MODIFY_PERIOD_LENGTH,
    )
    wait_for_synced_or_terminal(
        ref, VOLUME_MODIFY_WAIT_PERIODS, VOLUME_MODIFY_PERIOD_LENGTH,
        current_generation=True,
    )
    _assert_tags(validator, volume_id, k8s.get_resource(ref), new_tags)


def _assert_no_idle_update(ref, validator, volume_id, retag):
    """Tags-only change, then no UpdateVolume.

    The tag patch bumps generation and so forces a reconcile. A tags-only
    delta must skip UpdateVolume, so an unchanged counter proves the rest of
    the spec (late-initialized defaults, NFS export and quota ordering, the
    restored ConstituentsPerAggregate/CoolingPeriod) converges with Describe.
    """
    before = get_controller_api_call_count("UpdateVolume")
    _patch_tags_and_assert(ref, validator, volume_id, retag)
    after = get_controller_api_call_count("UpdateVolume")
    assert after == before, (
        f"UpdateVolume count went from {before} to {after}: "
        + (
            "the controller restarted mid-window (counter reset)"
            if after < before
            else "a non-Tags delta caused an UpdateVolume call"
        )
    )
    cr = k8s.get_resource(ref)
    by_type = {c["type"]: c for c in cr["status"].get("conditions") or []}
    for t in (condition.CONDITION_TYPE_TERMINAL, condition.CONDITION_TYPE_RECOVERABLE):
        c = by_type.get(t)
        assert c is None or c.get("status") != "True", c
    condition.assert_synced(ref)


def _declared_quotas_present(volume, quotas):
    """Every declared quota is in Describe; extra root entries are allowed."""
    observed = {
        (q.get("Type"), q.get("Id"), q.get("StorageCapacityQuotaGiB"))
        for q in volume["OpenZFSConfiguration"].get("UserAndGroupQuotas") or []
    }
    return all(
        (q["type_"], q["id"], q["storageCapacityQuotaGiB"]) in observed
        for q in quotas
    )


def _adopt_and_assert(fsx_client, validator, volume_id, volume_type, volume_name, check):
    """Adopts the volume by ID into a minimal CR; deletion-policy retain
    leaves the volume for the original CR."""
    adopted_name = random_suffix_name("ack-fsx-vol-adopt", 32)
    replacements = REPLACEMENT_VALUES.copy()
    replacements["ADOPTED_VOLUME_CR_NAME"] = adopted_name
    replacements["VOLUME_ID"] = volume_id
    replacements["VOLUME_TYPE"] = volume_type
    replacements["VOLUME_NAME"] = volume_name
    resource_data = load_fsx_resource("volume_adoption", additional_replacements=replacements)
    adopted_ref = _ref(VOLUME_RESOURCE_PLURAL, adopted_name)
    k8s.create_custom_resource(adopted_ref, resource_data)
    try:
        k8s.wait_resource_consumed_by_controller(adopted_ref)
        wait_for_synced_or_terminal(
            adopted_ref, VOLUME_CREATE_WAIT_PERIODS, VOLUME_CREATE_PERIOD_LENGTH,
        )
        cr = k8s.get_resource(adopted_ref)
        assert cr["status"]["id"] == volume_id
        check(cr)
    finally:
        _delete_cr(adopted_ref, VOLUME_DELETE_WAIT_PERIODS, VOLUME_DELETE_PERIOD_LENGTH)
    # deletion-policy: retain must leave the volume in place.
    assert validator.get(volume_id) is not None


# ---------------------------------------------------------------- ONTAP


@pytest.fixture(scope="module")
def ontap_file_system(fsx_client):
    yield from _create_file_system(fsx_client, "ontap_file_system", "ack-fsx-vol-ontap-fs")


@pytest.fixture(scope="module")
def ontap_svm(fsx_client, ontap_file_system):
    """Teardown runs after the volume's and before the file system's: FSx
    rejects deleting an SVM that still has non-root volumes."""
    (file_system_name, file_system_id) = ontap_file_system
    cr_name = random_suffix_name("ack-fsx-vol-svm", 32)
    svm_name = random_suffix_name("ackfsxvolsvm", 20).replace("-", "_")
    replacements = REPLACEMENT_VALUES.copy()
    replacements.update({
        "SVM_CR_NAME": cr_name,
        "SVM_NAME": svm_name,
        "FILE_SYSTEM_NAME": file_system_name,
        "ROOT_VOLUME_SECURITY_STYLE": "UNIX",
        "TAG_KEY": TAG_KEY,
        "TAG_VALUE": INITIAL_TAG_VALUE,
        "SECOND_TAG_KEY": SECOND_TAG_KEY,
        "SECOND_TAG_VALUE": SECOND_TAG_VALUE,
    })
    resource_data = load_fsx_resource("storage_virtual_machine", additional_replacements=replacements)
    ref = _ref(SVM_RESOURCE_PLURAL, cr_name)
    k8s.create_custom_resource(ref, resource_data)
    svm_id = None
    try:
        k8s.wait_resource_consumed_by_controller(ref)
        wait_for_synced_or_terminal(ref, SVM_CREATE_WAIT_PERIODS, SVM_CREATE_PERIOD_LENGTH)
        svm_id = k8s.get_resource(ref)["status"]["id"]
        yield (cr_name, file_system_id, svm_id)
    finally:
        if svm_id is None and k8s.get_resource_exists(ref):
            svm_id = (k8s.get_resource(ref) or {}).get("status", {}).get("id")
        _delete_cr(ref, SVM_DELETE_WAIT_PERIODS, SVM_DELETE_PERIOD_LENGTH)
        if svm_id is not None:
            assert StorageVirtualMachineValidator(fsx_client).wait_until_gone(svm_id)


@pytest.fixture(scope="module")
def ontap_volume(fsx_client, ontap_svm):
    (svm_cr_name, file_system_id, svm_id) = ontap_svm
    cr_name = random_suffix_name("ack-fsx-vol-ontap", 32)
    # ONTAP volume names are [A-Za-z0-9_]; no hyphens.
    volume_name = random_suffix_name("ackfsxvol", 20).replace("-", "_")
    replacements = REPLACEMENT_VALUES.copy()
    replacements.update({
        "VOLUME_CR_NAME": cr_name,
        "VOLUME_NAME": volume_name,
        "SVM_CR_NAME": svm_cr_name,
        "SIZE_IN_BYTES": str(ONTAP_INITIAL_SIZE),
        "TAG_KEY": TAG_KEY,
        "TAG_VALUE": INITIAL_TAG_VALUE,
        "SECOND_TAG_KEY": SECOND_TAG_KEY,
        "SECOND_TAG_VALUE": SECOND_TAG_VALUE,
    })
    ref = _ref(VOLUME_RESOURCE_PLURAL, cr_name)
    try:
        ref, volume_id = _create_volume(fsx_client, "volume_ontap", replacements)
        yield (ref, file_system_id, svm_id, volume_id, volume_name)
    finally:
        _cleanup_volume(fsx_client, ref)
        # The test deletes without skip-final-backup; remove that backup.
        _delete_final_backups(fsx_client, volume_name)


def _delete_final_backups(fsx_client, volume_name):
    """Best-effort removal of the final backup FSx takes when DeleteVolume
    carries no SkipFinalBackup. Logged rather than raised: it must not mask
    the test's own result."""
    try:
        for page in fsx_client.get_paginator("describe_backups").paginate():
            for b in page.get("Backups", []):
                if (b.get("Volume") or {}).get("Name") != volume_name:
                    continue
                logging.info(f"deleting final backup {b['BackupId']} of {volume_name}")
                fsx_client.delete_backup(BackupId=b["BackupId"])
    except Exception as e:
        logging.error(f"could not remove final backups of {volume_name}: {e}")


# Server-defaulted ONTAP members late_initialize must copy into the spec:
# (CR path under spec.ontapConfiguration, Describe path under OntapConfiguration).
ONTAP_LATE_INIT = [
    (("securityStyle",), ("SecurityStyle",)),
    (("snapshotPolicy",), ("SnapshotPolicy",)),
    (("tieringPolicy", "name"), ("TieringPolicy", "Name")),
    (("copyTagsToBackups",), ("CopyTagsToBackups",)),
    (("type",), ("OntapVolumeType",)),
    (("style",), ("VolumeStyle",)),
    (("aggregateConfiguration", "aggregates"), ("AggregateConfiguration", "Aggregates")),
]

OPENZFS_LATE_INIT = [
    (("dataCompressionType",), ("DataCompressionType",)),
    (("recordSizeKiB",), ("RecordSizeKiB",)),
    (("readOnly",), ("ReadOnly",)),
    (("copyTagsToSnapshots",), ("CopyTagsToSnapshots",)),
    (("storageCapacityQuotaGiB",), ("StorageCapacityQuotaGiB",)),
    (("storageCapacityReservationGiB",), ("StorageCapacityReservationGiB",)),
]


def _dig(d, path):
    for p in path:
        if not isinstance(d, dict) or p not in d:
            return None
        d = d[p]
    return d


def _assert_late_initialized(ref, block, observed, members):
    """Every listed member Describe reports must be copied into the spec."""
    reported = [(c, a) for c, a in members if _dig(observed, a) is not None]
    unreported = [a for c, a in members if _dig(observed, a) is None]
    if unreported:
        logging.warning(f"Describe did not report {unreported}; late-init is a no-op for them")
    cr = _wait_for_cr(
        ref,
        lambda cr: all(_dig(cr["spec"].get(block) or {}, c) is not None for c, _ in reported),
        "late-initialized",
    )
    for c, a in reported:
        assert _dig(cr["spec"][block], c) == _dig(observed, a), (c, a)
    return cr


@service_marker
class TestOntapVolume:
    def test_crud(self, fsx_client, ontap_volume):
        (ref, file_system_id, svm_id, volume_id, volume_name) = ontap_volume
        validator = VolumeValidator(fsx_client)
        self._assert_created(ref, validator, file_system_id, svm_id, volume_id, volume_name)
        _assert_no_idle_update(ref, validator, volume_id, UPDATED_TAGS)
        self._update_and_assert(ref, validator, volume_id)
        _assert_no_idle_update(ref, validator, volume_id, INITIAL_TAGS)

        def check(cr):
            cfg = cr["spec"]["ontapConfiguration"]
            assert cfg["storageVirtualMachineID"] == svm_id
            assert cfg["sizeInBytes"] == ONTAP_UPDATED_SIZE
        _adopt_and_assert(fsx_client, validator, volume_id, "ONTAP", volume_name, check)
        self._delete_and_assert(ref, validator, volume_id)

    def _assert_created(self, ref, validator, file_system_id, svm_id, volume_id, volume_name):
        assert volume_id is not None
        condition.assert_synced(ref)
        cr = k8s.get_resource(ref)
        assert cr["status"]["lifecycle"] == "CREATED"
        assert cr["status"]["fileSystemID"] == file_system_id
        assert cr["status"]["ackResourceMetadata"]["arn"] is not None
        assert cr["spec"]["ontapConfiguration"]["storageVirtualMachineRef"]["from"]["name"]

        vol = validator.get(volume_id)
        assert vol is not None
        logging.info(f"Describe for a minimal ONTAP volume: {vol}")
        assert vol["Lifecycle"] == "CREATED"
        assert vol["VolumeType"] == "ONTAP"
        assert vol["Name"] == volume_name
        assert vol["FileSystemId"] == file_system_id
        assert vol["ResourceARN"] == cr["status"]["ackResourceMetadata"]["arn"]
        ontap = vol["OntapConfiguration"]
        assert ontap["StorageVirtualMachineId"] == svm_id
        assert ontap["SizeInBytes"] == ONTAP_INITIAL_SIZE

        cr = _assert_late_initialized(ref, "ontapConfiguration", ontap, ONTAP_LATE_INIT)
        # Conditional default: must not be copied into the spec.
        assert "coolingPeriod" not in (cr["spec"]["ontapConfiguration"].get("tieringPolicy") or {})
        # Deprecated duplicate of sizeInBytes: not in the spec at all.
        assert "sizeInMegabytes" not in cr["spec"]["ontapConfiguration"]
        _assert_tags(validator, volume_id, cr, INITIAL_TAGS)

    def _update_and_assert(self, ref, validator, volume_id):
        """Changes two OntapConfiguration members. The whole configuration
        (with late-initialized members) is re-sent, which FSx must accept."""
        k8s.patch_custom_resource(ref, {"spec": {"ontapConfiguration": {
            "sizeInBytes": ONTAP_UPDATED_SIZE,
            "snapshotPolicy": ONTAP_UPDATED_SNAPSHOT_POLICY,
        }}})
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert validator.wait_until(
            volume_id,
            lambda v: v["OntapConfiguration"].get("SizeInBytes") == ONTAP_UPDATED_SIZE
            and v["OntapConfiguration"].get("SnapshotPolicy") == ONTAP_UPDATED_SNAPSHOT_POLICY,
            "updated",
            wait_periods=VOLUME_MODIFY_WAIT_PERIODS,
            period_length=VOLUME_MODIFY_PERIOD_LENGTH,
        )
        wait_for_synced_or_terminal(
            ref, VOLUME_MODIFY_WAIT_PERIODS, VOLUME_MODIFY_PERIOD_LENGTH,
            current_generation=True,
        )
        cfg = k8s.get_resource(ref)["spec"]["ontapConfiguration"]
        assert cfg["sizeInBytes"] == ONTAP_UPDATED_SIZE
        assert cfg["snapshotPolicy"] == ONTAP_UPDATED_SNAPSHOT_POLICY

    def _delete_and_assert(self, ref, validator, volume_id):
        """Deletes with the skip-final-backup annotation removed, so
        DeleteVolume carries no OntapConfiguration: FSx must accept that
        (and take its default final backup, removed in fixture teardown)."""
        k8s.patch_custom_resource(ref, {
            "metadata": {"annotations": {SKIP_FINAL_BACKUP_ANNOTATION: None}},
        })
        _delete_cr(ref, VOLUME_DELETE_WAIT_PERIODS, VOLUME_DELETE_PERIOD_LENGTH)
        # The finalizer is held until Describe reports VolumeNotFound.
        assert validator.wait_until_gone(volume_id, wait_periods=4, period_length=15)


# -------------------------------------------------------------- OpenZFS


@pytest.fixture(scope="module")
def openzfs_file_system(fsx_client):
    for (name, file_system_id) in _create_file_system(
        fsx_client, "openzfs_file_system", "ack-fsx-vol-ozfs-fs",
    ):
        fs = FSxValidator(fsx_client).get_file_system(file_system_id)
        root_volume_id = fs["OpenZFSConfiguration"]["RootVolumeId"]
        yield (name, file_system_id, root_volume_id)


@pytest.fixture(scope="module")
def openzfs_volume(fsx_client, openzfs_file_system):
    (_, file_system_id, root_volume_id) = openzfs_file_system
    cr_name = random_suffix_name("ack-fsx-vol-ozfs", 32)
    volume_name = random_suffix_name("ackfsxvol", 20).replace("-", "_")
    replacements = REPLACEMENT_VALUES.copy()
    replacements.update({
        "VOLUME_CR_NAME": cr_name,
        "VOLUME_NAME": volume_name,
        "PARENT_VOLUME_ID": root_volume_id,
        "TAG_KEY": TAG_KEY,
        "TAG_VALUE": INITIAL_TAG_VALUE,
    })
    ref = _ref(VOLUME_RESOURCE_PLURAL, cr_name)
    try:
        ref, volume_id = _create_volume(fsx_client, "volume_openzfs", replacements)
        yield (ref, file_system_id, root_volume_id, volume_id, volume_name)
    finally:
        _cleanup_volume(fsx_client, ref)


@service_marker
class TestOpenZFSVolume:
    def test_crud(self, fsx_client, openzfs_volume):
        (ref, file_system_id, root_volume_id, volume_id, volume_name) = openzfs_volume
        validator = VolumeValidator(fsx_client)
        self._assert_created(ref, validator, file_system_id, root_volume_id, volume_id, volume_name)
        child_ref = _ref(
            VOLUME_RESOURCE_PLURAL, random_suffix_name("ack-fsx-vol-ozfs-child", 32),
        )
        try:
            self._create_child_by_ref(fsx_client, child_ref, ref, validator, volume_id)
            _assert_no_idle_update(ref, validator, volume_id, UPDATED_TAGS)
            self._update_and_assert(ref, validator, volume_id)
            _assert_no_idle_update(ref, validator, volume_id, {TAG_KEY: INITIAL_TAG_VALUE})

            def check(cr):
                cfg = cr["spec"]["openZFSConfiguration"]
                assert cfg["parentVolumeID"] == root_volume_id
                assert cfg["storageCapacityQuotaGiB"] == OPENZFS_UPDATED_QUOTA_GIB
            _adopt_and_assert(fsx_client, validator, volume_id, "OPENZFS", volume_name, check)
        finally:
            # The child must go first: without delete options FSx refuses to
            # delete a volume that has children.
            _cleanup_volume(fsx_client, child_ref)
        self._delete_and_assert(ref, validator, volume_id)

    def _assert_created(self, ref, validator, file_system_id, root_volume_id, volume_id, volume_name):
        assert volume_id is not None
        condition.assert_synced(ref)
        cr = k8s.get_resource(ref)
        assert cr["status"]["lifecycle"] == "AVAILABLE"
        assert cr["status"]["fileSystemID"] == file_system_id

        vol = validator.get(volume_id)
        assert vol is not None
        logging.info(f"Describe for a minimal OpenZFS volume: {vol}")
        assert vol["Lifecycle"] == "AVAILABLE"
        assert vol["VolumeType"] == "OPENZFS"
        assert vol["Name"] == volume_name
        assert vol["ResourceARN"] == cr["status"]["ackResourceMetadata"]["arn"]
        ozfs = vol["OpenZFSConfiguration"]
        assert ozfs["ParentVolumeId"] == root_volume_id
        cr = _assert_late_initialized(ref, "openZFSConfiguration", ozfs, OPENZFS_LATE_INIT)
        _assert_tags(validator, volume_id, cr, {TAG_KEY: INITIAL_TAG_VALUE})

    def _create_child_by_ref(self, fsx_client, child_ref, parent_ref, validator, parent_id):
        """A grandchild volume whose parent is resolved through
        parentVolumeRef (a same-kind reference). The caller owns cleanup."""
        replacements = REPLACEMENT_VALUES.copy()
        replacements.update({
            "VOLUME_CR_NAME": child_ref.name,
            "VOLUME_NAME": random_suffix_name("ackfsxchild", 20).replace("-", "_"),
            "PARENT_VOLUME_CR_NAME": parent_ref.name,
        })
        _, child_id = _create_volume(fsx_client, "volume_openzfs_child", replacements)
        child = validator.get(child_id)
        assert child["Lifecycle"] == "AVAILABLE"
        assert child["OpenZFSConfiguration"]["ParentVolumeId"] == parent_id
        cr = k8s.get_resource(child_ref)
        assert cr["spec"]["openZFSConfiguration"]["parentVolumeRef"]["from"]["name"] == parent_ref.name

    def _update_and_assert(self, ref, validator, volume_id):
        """Updates scalar members and the NFS export / quota lists.

        FSx returns the client configurations in its own order and always
        reports the root (Id 0, quota 0) USER/GROUP quotas next to the
        declared ones; the idle check afterwards proves the controller treats
        that as no change, and the spec check proves the edit is not reverted
        by the (pre-update) UpdateVolume response."""
        nfs_exports = [{"clientConfigurations": [
            {"clients": "10.0.0.0/16", "options": ["rw", "crossmnt", "no_root_squash"]},
            {"clients": "10.1.0.0/16", "options": ["ro"]},
        ]}]
        quotas = [
            {"id": 1001, "type_": "USER", "storageCapacityQuotaGiB": 5},
            {"id": 2001, "type_": "GROUP", "storageCapacityQuotaGiB": 8},
        ]
        k8s.patch_custom_resource(ref, {"spec": {"openZFSConfiguration": {
            "storageCapacityQuotaGiB": OPENZFS_UPDATED_QUOTA_GIB,
            "dataCompressionType": OPENZFS_UPDATED_COMPRESSION,
            "nfsExports": nfs_exports,
            "userAndGroupQuotas": quotas,
        }}})
        time.sleep(MODIFY_WAIT_AFTER_SECONDS)
        assert validator.wait_until(
            volume_id,
            lambda v: v["OpenZFSConfiguration"].get("StorageCapacityQuotaGiB") == OPENZFS_UPDATED_QUOTA_GIB
            and v["OpenZFSConfiguration"].get("DataCompressionType") == OPENZFS_UPDATED_COMPRESSION
            and _declared_quotas_present(v, quotas),
            "updated",
            wait_periods=VOLUME_MODIFY_WAIT_PERIODS,
            period_length=VOLUME_MODIFY_PERIOD_LENGTH,
        )
        wait_for_synced_or_terminal(
            ref, VOLUME_MODIFY_WAIT_PERIODS, VOLUME_MODIFY_PERIOD_LENGTH,
            current_generation=True,
        )
        logging.info(f"Describe after OpenZFS update: {validator.get(volume_id)}")
        cfg = k8s.get_resource(ref)["spec"]["openZFSConfiguration"]
        assert cfg["storageCapacityQuotaGiB"] == OPENZFS_UPDATED_QUOTA_GIB
        assert cfg["dataCompressionType"] == OPENZFS_UPDATED_COMPRESSION
        assert cfg["nfsExports"] == nfs_exports
        assert cfg["userAndGroupQuotas"] == quotas

    def _delete_and_assert(self, ref, validator, volume_id):
        """No delete annotations, so DeleteVolume carries no
        OpenZFSConfiguration."""
        _delete_cr(ref, VOLUME_DELETE_WAIT_PERIODS, VOLUME_DELETE_PERIOD_LENGTH)
        assert validator.wait_until_gone(volume_id, wait_periods=4, period_length=15)
