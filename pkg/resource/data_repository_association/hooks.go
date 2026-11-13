// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package data_repository_association

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/fsx-controller/pkg/util"
)

// DeleteDataInFileSystemAnnotation sets DeleteDataInFileSystem ("true" or
// "false") when a DataRepositoryAssociation is deleted. When it is absent the
// controller sends false, FSx's documented default: files already imported
// into the file system are kept.
var DeleteDataInFileSystemAnnotation = fmt.Sprintf(
	"%s/delete-data-in-file-system", svcapitypes.GroupVersion.Group,
)

// parseDeleteDataInFileSystem returns nil when the annotation is absent or
// empty. An unparseable value is an error rather than a silent default, so a
// typo cannot quietly change what a delete destroys.
func parseDeleteDataInFileSystem(annotations map[string]string) (*bool, error) {
	value, ok := annotations[DeleteDataInFileSystemAnnotation]
	if !ok || value == "" {
		return nil, nil
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return nil, fmt.Errorf(
			"invalid value %q for annotation %s: %v",
			value, DeleteDataInFileSystemAnnotation, err,
		)
	}
	return &b, nil
}

// setDeleteDataInFileSystem copies the delete-data-in-file-system annotation
// onto the DeleteDataRepositoryAssociation input.
//
// The SDK models DeleteDataInFileSystem as optional with a default of false,
// but the service rejects a request that omits it ("BadRequest: Parameter
// Validation Failed: Required parameter in input: DeleteDataInFileSystem"),
// observed in e2e. That error is retryable, so leaving the member nil would
// hold the finalizer forever. Without the annotation we send the documented
// default, false, which never deletes file system data.
func setDeleteDataInFileSystem(
	r *resource,
	input *svcsdk.DeleteDataRepositoryAssociationInput,
) error {
	v, err := parseDeleteDataInFileSystem(r.ko.ObjectMeta.GetAnnotations())
	if err != nil {
		// Retrying cannot fix a malformed annotation value; editing the
		// annotation triggers a fresh reconcile.
		return ackerr.NewTerminalError(err)
	}
	if v == nil {
		v = aws.Bool(false)
	}
	input.DeleteDataInFileSystem = v
	return nil
}

// requeueWaitWhileDeleting is returned from the delete path while FSx still
// reports the association: DeleteDataRepositoryAssociation is asynchronous.
var requeueWaitWhileDeleting = ackrequeue.NeededAfter(
	fmt.Errorf(
		"data repository association is in %q state, requeuing until it is fully deleted",
		svcsdktypes.DataRepositoryLifecycleDeleting,
	),
	15*time.Second,
)

// syncTags brings the association's AWS tags in line with Spec.Tags using
// TagResource/UntagResource (see util.SyncTags).
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() { exit(err) }()

	return util.SyncTags(
		ctx, desired.ko.Spec.Tags, latest.ko.Spec.Tags,
		latest.ko.Status.ACKResourceMetadata, convertToOrderedACKTags,
		rm.sdkapi, rm.metrics,
	)
}

// keepDataRepositoryPathSpelling keeps desired's DataRepositoryPath when FSx
// reports the same path with a trailing "/" appended. FSx normalizes
// s3://bucket/prefix to s3://bucket/prefix/; writing that back would change an
// immutable field, which the CRD's CEL rule rejects.
func keepDataRepositoryPathSpelling(
	desired *svcapitypes.DataRepositoryAssociation,
	ko *svcapitypes.DataRepositoryAssociation,
) {
	if desired == nil || desired.Spec.DataRepositoryPath == nil || ko.Spec.DataRepositoryPath == nil {
		return
	}
	want, got := *desired.Spec.DataRepositoryPath, *ko.Spec.DataRepositoryPath
	if want != got && strings.TrimSuffix(want, "/") == strings.TrimSuffix(got, "/") {
		ko.Spec.DataRepositoryPath = aws.String(want)
	}
}

// rejectUnexpressibleS3Changes returns a terminal error when desired removes
// S3, AutoExportPolicy or AutoImportPolicy while FSx still reports events for
// it.
//
// This is a convergence check, not validation, mirroring FileSystem's
// rejectUnexpressibleChanges: UpdateDataRepositoryAssociation only applies the
// members it is sent and has no "clear" member, and the generated
// newUpdateRequestPayload omits nil policies. Removing a policy is therefore
// never applied, Describe keeps returning it, and every reconcile would issue
// another Update. Editing the Spec to set the policy explicitly clears the
// terminal condition.
//
// Only a latest policy with at least one event is treated as removed, so an
// empty S3{} reported for an association that never set s3 is not rejected.
// Known gap: in that empty-S3 case Spec.S3 may still appear in the delta and
// be sent as an Update without S3; what Describe returns there has not been
// verified against a live resource.
func rejectUnexpressibleS3Changes(
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) error {
	if delta == nil || !delta.DifferentAt("Spec.S3") {
		return nil
	}
	latestS3 := latest.ko.Spec.S3
	if latestS3 == nil {
		return nil
	}
	desiredS3 := desired.ko.Spec.S3

	var paths []string
	if latestS3.AutoExportPolicy != nil && len(latestS3.AutoExportPolicy.Events) > 0 &&
		(desiredS3 == nil || desiredS3.AutoExportPolicy == nil) {
		paths = append(paths, "Spec.S3.AutoExportPolicy")
	}
	if latestS3.AutoImportPolicy != nil && len(latestS3.AutoImportPolicy.Events) > 0 &&
		(desiredS3 == nil || desiredS3.AutoImportPolicy == nil) {
		paths = append(paths, "Spec.S3.AutoImportPolicy")
	}
	if len(paths) == 0 {
		return nil
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"cannot apply changes to %s: UpdateDataRepositoryAssociation only "+
			"applies the members it is sent, so removing a policy is never "+
			"applied in FSx; set the policy explicitly instead of removing it",
		strings.Join(paths, ", "),
	))
}

// requeueIfNotUpdatable requeues while the association is CREATING, UPDATING
// or DELETING. FSx rejects UpdateDataRepositoryAssociation in those states
// with BadRequest (reported in PR review), which would otherwise surface in
// the CR's conditions on every requeue. It only waits, never rejects.
// MISCONFIGURED stays updatable because an update is one way to fix it;
// FAILED never reaches update because the read hook returns Terminal first.
//
// updateable.when is not used: the generated guard is emitted after the
// sdk_update_pre_build_request hook, so it could not guard syncTags
// (TagResource/UntagResource) or the tags-only early return.
func requeueIfNotUpdatable(latest *resource) error {
	if latest == nil || latest.ko == nil || latest.ko.Status.Lifecycle == nil {
		return nil
	}
	lifecycle := *latest.ko.Status.Lifecycle
	switch lifecycle {
	case string(svcsdktypes.DataRepositoryLifecycleCreating),
		string(svcsdktypes.DataRepositoryLifecycleUpdating),
		string(svcsdktypes.DataRepositoryLifecycleDeleting):
		return ackrequeue.NeededAfter(
			fmt.Errorf(
				"data repository association is in %q state, requeuing until it can be updated",
				lifecycle,
			),
			30*time.Second,
		)
	}
	return nil
}

// terminalIfFailed returns a terminal error when FSx reports the association
// as FAILED, which the API documents as "a terminal state that cannot be
// recovered". The returned resource is non-nil so Status (and FailureDetails)
// is still patched alongside the Terminal condition.
//
// MISCONFIGURED is deliberately excluded: FSx documents it as recoverable once
// the configuration is corrected, and the fix is usually on the bucket side
// (external state). A Terminal condition would stop the requeue, so the CR
// would never observe the association returning to AVAILABLE.
//
// Never while the CR is being deleted: the delete path calls ReadOne first and
// aborts on any error but NotFound, and FAILED associations are deletable.
func terminalIfFailed(ko *svcapitypes.DataRepositoryAssociation) error {
	if ko == nil || ko.Status.Lifecycle == nil {
		return nil
	}
	if ko.DeletionTimestamp != nil {
		return nil
	}
	if *ko.Status.Lifecycle != string(svcsdktypes.DataRepositoryLifecycleFailed) {
		return nil
	}
	reason := "no failure details reported"
	if ko.Status.FailureDetails != nil && ko.Status.FailureDetails.Message != nil {
		reason = *ko.Status.FailureDetails.Message
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"data repository association is in FAILED state and cannot recover: %s", reason,
	))
}

// setDeletingLifecycle records the lifecycle returned by
// DeleteDataRepositoryAssociation (falling back to DELETING) on r, so Status
// shows the delete in progress. Later reconciles are held by the
// deletable.when guard, which does not patch Status, so this value stays.
func setDeletingLifecycle(
	r *resource,
	resp *svcsdk.DeleteDataRepositoryAssociationOutput,
) {
	if resp != nil && resp.Lifecycle != "" {
		r.ko.Status.Lifecycle = aws.String(string(resp.Lifecycle))
		return
	}
	r.ko.Status.Lifecycle = aws.String(string(svcsdktypes.DataRepositoryLifecycleDeleting))
}
