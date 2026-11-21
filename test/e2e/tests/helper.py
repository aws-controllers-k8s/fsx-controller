# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Helper functions for FSx e2e tests.
"""

import logging
import time
from typing import Dict, Optional

from acktest import tags as ack_tags
from acktest.k8s import condition
from acktest.k8s import resource as k8s
from botocore.exceptions import ClientError
from kubernetes import client


# The controller's metrics Service (config/controller/service.yaml), which
# test-infra applies alongside the controller deployment.
METRICS_SERVICE = "ack-fsx-metrics-service:8080"
METRICS_NAMESPACE = "ack-system"
OUTBOUND_API_REQUESTS_METRIC = "ack_outbound_api_requests_total"


def get_controller_api_call_count(op_id: str) -> int:
    """Returns the controller's ack_outbound_api_requests_total for op_id.

    Assumes a single controller replica behind the metrics Service and no
    restart during the measured window: the counter is per-process, so a
    restart resets it and another replica would report its own count.
    """
    metrics = client.CoreV1Api(
        k8s._get_k8s_api_client()
    ).connect_get_namespaced_service_proxy_with_path(
        METRICS_SERVICE, METRICS_NAMESPACE, "metrics",
    )
    label = f'op_id="{op_id}"'
    total = 0
    for line in metrics.splitlines():
        if not line.startswith(OUTBOUND_API_REQUESTS_METRIC + "{"):
            continue
        labels, _, value = line.rpartition("}")
        if label not in labels:
            continue
        total += int(float(value.split()[0]))
    return total


# Not-found codes FSxValidator maps to None.
NOT_FOUND_ERROR_CODES = frozenset(
    ("FileSystemNotFound", "ResourceNotFoundException")
)


def wait_for_synced_or_terminal(
    ref, wait_periods: int, period_length: int,
    *, current_generation: bool = False,
) -> None:
    """Waits for ACK.ResourceSynced=True, failing fast on ACK.Terminal.

    acktest's wait_on_condition polls the full timeout regardless, so a
    terminal create error costs the whole wait and surfaces as a bare
    `assert False` with the reason only in the controller logs.

    With current_generation=True it also waits until the Synced condition
    was set for the CR's current metadata.generation, so a Synced=True left
    over from an earlier generation doesn't count.
    """
    for _ in range(wait_periods):
        cr = k8s.get_resource(ref)
        conditions = (cr or {}).get("status", {}).get("conditions") or []
        by_type = {c["type"]: c for c in conditions}

        terminal = by_type.get(condition.CONDITION_TYPE_TERMINAL)
        if terminal is not None and terminal.get("status") == "True":
            raise AssertionError(
                f"{ref.name} went terminal instead of syncing: "
                f"{terminal.get('message')}"
            )

        synced = by_type.get(condition.CONDITION_TYPE_RESOURCE_SYNCED)
        if synced is not None and synced.get("status") == "True":
            if not current_generation:
                return
            generation = (cr or {}).get("metadata", {}).get("generation")
            if synced.get("observedGeneration") == generation:
                return

        time.sleep(period_length)

    # Report the last-known conditions so a real timeout is diagnosable.
    cr = k8s.get_resource(ref)
    conditions = (cr or {}).get("status", {}).get("conditions") or []
    generation = (cr or {}).get("metadata", {}).get("generation")
    raise AssertionError(
        f"{ref.name} did not reach ACK.ResourceSynced=True within "
        f"{wait_periods * period_length}s; generation={generation}; "
        "conditions: "
        + "; ".join(
            f"{c.get('type')}={c.get('status')} "
            f"(observedGeneration={c.get('observedGeneration')}) "
            f"({c.get('message')})"
            for c in conditions
        )
    )


class FSxResourceValidator:
    """Shared Describe-based validator for FSx resources.

    Subclasses supply the describe call for a single ID, the error codes that
    mean "not found", a log label and their default wait_until_gone periods.
    """

    def __init__(
        self,
        fsx_client,
        describe,
        not_found_error_codes: frozenset,
        label: str,
        gone_wait_periods: int,
        gone_period_length: int,
    ):
        self.fsx_client = fsx_client
        # describe(resource_id) -> list of matching items from the response.
        self._describe = describe
        self._not_found_error_codes = not_found_error_codes
        self._label = label
        self._gone_wait_periods = gone_wait_periods
        self._gone_period_length = gone_period_length

    def get(self, resource_id: str) -> Optional[dict]:
        """Returns the resource with the supplied ID, or None once FSx no
        longer reports it. Any error other than not-found is re-raised, so a
        transient failure cannot pass for a completed delete.
        """
        try:
            items = self._describe(resource_id)
            if len(items) == 0:
                return None
            return items[0]
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in self._not_found_error_codes:
                return None
            logging.error(
                f"describe {self._label}({resource_id}) failed with {code}"
            )
            raise

    def get_tags(self, resource_id: str) -> Dict[str, str]:
        item = self.get(resource_id)
        if item is None:
            return {}
        return {t["Key"]: t["Value"] for t in item.get("Tags", [])}

    def wait_until(
        self,
        resource_id: str,
        predicate,
        description: str,
        wait_periods: int = 40,
        period_length: int = 15,
    ) -> bool:
        """Polls Describe until predicate(item) is true.

        Synced may still be True from the pre-update reconcile when a test
        looks at it, so the AWS API is the reliable signal an update landed.
        """
        for _ in range(wait_periods):
            item = self.get(resource_id)
            if item is not None and predicate(item):
                return True
            logging.info(
                f"{self._label} {resource_id} not yet {description}; "
                f"lifecycle {None if item is None else item.get('Lifecycle')}"
            )
            time.sleep(period_length)
        return False

    def wait_until_gone(
        self,
        resource_id: str,
        wait_periods: Optional[int] = None,
        period_length: Optional[int] = None,
    ) -> bool:
        """Polls Describe until the resource disappears. Defaults to the
        subclass's wait periods when none are supplied.
        """
        if wait_periods is None:
            wait_periods = self._gone_wait_periods
        if period_length is None:
            period_length = self._gone_period_length
        for _ in range(wait_periods):
            item = self.get(resource_id)
            if item is None:
                return True
            logging.info(
                f"{self._label} {resource_id} still present in state "
                f"{item.get('Lifecycle')}; waiting"
            )
            time.sleep(period_length)
        return self.get(resource_id) is None


class FSxValidator(FSxResourceValidator):
    # FSx keeps a file system in the DELETING lifecycle state for several
    # minutes after DeleteFileSystem returns, so "deleted from Kubernetes" is
    # not the same as "deleted in AWS"; hence the longer wait_until_gone
    # defaults (40 x 30s).
    def __init__(self, fsx_client):
        super().__init__(
            fsx_client,
            describe=lambda i: fsx_client.describe_file_systems(
                FileSystemIds=[i],
            ).get("FileSystems", []),
            not_found_error_codes=NOT_FOUND_ERROR_CODES,
            label="file system",
            gone_wait_periods=40,
            gone_period_length=30,
        )

    def get(self, file_system_id: str) -> Optional[dict]:
        """Returns the FSx FileSystem with the supplied ID, or None if it does
        not exist (or has already been fully deleted).

        Only a genuine not-found error is translated to None. Any other failure
        is re-raised: wait_until_gone() treats None as "deleted in AWS", so
        swallowing a transient error (throttling, expired credentials, a
        connection failure) would let the delete assertions pass while a
        multi-hundred-GiB file system is still running and billing.
        """
        try:
            file_systems = self._describe(file_system_id)
            if len(file_systems) == 0:
                return None
            return file_systems[0]
        except self.fsx_client.exceptions.FileSystemNotFound:
            return None
        except ClientError as e:
            # Some botocore/endpoint combinations surface the modeled 404 as a
            # generic ClientError rather than the FileSystemNotFound subclass.
            code = e.response.get("Error", {}).get("Code")
            if code in NOT_FOUND_ERROR_CODES:
                return None
            logging.error(
                f"describe_file_systems({file_system_id}) failed with {code}"
            )
            raise

    def get_file_system(self, file_system_id: str) -> Optional[dict]:
        return self.get(file_system_id)

    def file_system_exists(self, file_system_id: str) -> bool:
        return self.get_file_system(file_system_id) is not None

    def get_lifecycle(self, file_system_id: str) -> Optional[str]:
        fs = self.get_file_system(file_system_id)
        if fs is None:
            return None
        return fs.get("Lifecycle")

    def wait_until_lustre_maintenance_window(
        self,
        file_system_id: str,
        expected: str,
        wait_periods: int = 20,
        period_length: int = 15,
    ) -> bool:
        """Polls DescribeFileSystems until the Lustre weekly maintenance window
        matches `expected`.

        FSx leaves Lifecycle at AVAILABLE while a maintenance-window change is
        applied, so the controller's ResourceSynced condition can already be
        True from the *pre-update* reconcile when the test looks at it. Polling
        the AWS API directly is the only reliable signal that the update landed.
        """
        for _ in range(wait_periods):
            fs = self.get_file_system(file_system_id)
            if fs is not None:
                actual = fs.get("LustreConfiguration", {}).get(
                    "WeeklyMaintenanceStartTime"
                )
                if actual == expected:
                    return True
                logging.info(
                    f"file system {file_system_id} maintenance window is "
                    f"{actual}; waiting for {expected}"
                )
            time.sleep(period_length)
        return False

    def wait_until_tags(
        self,
        file_system_id: str,
        expected: Dict[str, str],
        wait_periods: int = 20,
        period_length: int = 15,
    ) -> bool:
        """Polls DescribeFileSystems until the file system's non-ACK tags are
        exactly the supplied tags, so it waits both for added or changed keys
        and for removed keys to disappear.

        As with the maintenance window, a tag change does not move the file
        system out of AVAILABLE, so the Synced condition is not a usable signal.
        """
        for _ in range(wait_periods):
            actual = ack_tags.clean(self.get_tags(file_system_id))
            if actual == expected:
                return True
            logging.info(
                f"file system {file_system_id} tags are {actual}; "
                f"waiting for {expected}"
            )
            time.sleep(period_length)
        return False


# Not-found codes DataRepositoryAssociationValidator maps to None.
DRA_NOT_FOUND_ERROR_CODES = frozenset(("DataRepositoryAssociationNotFound",))


class DataRepositoryAssociationValidator(FSxResourceValidator):
    def __init__(self, fsx_client):
        super().__init__(
            fsx_client,
            describe=lambda i: fsx_client.describe_data_repository_associations(
                AssociationIds=[i],
            ).get("Associations", []),
            not_found_error_codes=DRA_NOT_FOUND_ERROR_CODES,
            label="association",
            gone_wait_periods=40,
            gone_period_length=15,
        )


# Not-found codes BackupValidator maps to None.
BACKUP_NOT_FOUND_ERROR_CODES = frozenset(("BackupNotFound",))


class BackupValidator(FSxResourceValidator):
    def __init__(self, fsx_client):
        super().__init__(
            fsx_client,
            describe=lambda i: fsx_client.describe_backups(
                BackupIds=[i],
            ).get("Backups", []),
            not_found_error_codes=BACKUP_NOT_FOUND_ERROR_CODES,
            label="backup",
            gone_wait_periods=20,
            gone_period_length=15,
        )


# Not-found codes StorageVirtualMachineValidator maps to None.
SVM_NOT_FOUND_ERROR_CODES = frozenset(("StorageVirtualMachineNotFound",))


class StorageVirtualMachineValidator(FSxResourceValidator):
    def __init__(self, fsx_client):
        super().__init__(
            fsx_client,
            describe=lambda i: fsx_client.describe_storage_virtual_machines(
                StorageVirtualMachineIds=[i],
            ).get("StorageVirtualMachines", []),
            not_found_error_codes=SVM_NOT_FOUND_ERROR_CODES,
            label="storage virtual machine",
            gone_wait_periods=40,
            gone_period_length=15,
        )

    def get_tags(self, resource_id: str) -> Dict[str, str]:
        """DescribeStorageVirtualMachines does not report SVM tags, so read
        them with ListTagsForResource on the SVM's ARN."""
        item = self.get(resource_id)
        if item is None:
            return {}
        tags = {}
        kwargs = {"ResourceARN": item["ResourceARN"]}
        while True:
            resp = self.fsx_client.list_tags_for_resource(**kwargs)
            tags.update({t["Key"]: t["Value"] for t in resp.get("Tags", [])})
            token = resp.get("NextToken")
            if not token:
                return tags
            kwargs["NextToken"] = token


# Not-found codes VolumeValidator maps to None.
VOLUME_NOT_FOUND_ERROR_CODES = frozenset(("VolumeNotFound",))


class VolumeValidator(FSxResourceValidator):
    def __init__(self, fsx_client):
        super().__init__(
            fsx_client,
            describe=lambda i: fsx_client.describe_volumes(
                VolumeIds=[i],
            ).get("Volumes", []),
            not_found_error_codes=VOLUME_NOT_FOUND_ERROR_CODES,
            label="volume",
            gone_wait_periods=40,
            gone_period_length=15,
        )

    def list_tags(self, resource_id: str) -> Dict[str, str]:
        """Tags from ListTagsForResource. DescribeVolumes does not report
        volume tags (as with SVMs), so the controller reads them the same
        way."""
        item = self.get(resource_id)
        if item is None:
            return {}
        tags = {}
        kwargs = {"ResourceARN": item["ResourceARN"]}
        while True:
            resp = self.fsx_client.list_tags_for_resource(**kwargs)
            tags.update({t["Key"]: t["Value"] for t in resp.get("Tags", [])})
            token = resp.get("NextToken")
            if not token:
                return tags
            kwargs["NextToken"] = token

    # Describe has no volume tags; never compare against it.
    get_tags = list_tags
