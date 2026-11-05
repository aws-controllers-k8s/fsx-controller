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
	"time"

	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
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
// TagResource/UntagResource. Removals and additions are separate calls.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() { exit(err) }()

	// ResourceARN carries `is_arn: true`, so a read populates
	// Status.ACKResourceMetadata.ARN. The guard is defensive.
	var arn *string
	if latest.ko.Status.ACKResourceMetadata != nil && latest.ko.Status.ACKResourceMetadata.ARN != nil {
		arn = aws.String(string(*latest.ko.Status.ACKResourceMetadata.ARN))
	}
	if arn == nil {
		return fmt.Errorf("cannot sync tags: resource ARN is not yet known")
	}

	desiredTags, _ := convertToOrderedACKTags(desired.ko.Spec.Tags)
	latestTags, _ := convertToOrderedACKTags(latest.ko.Spec.Tags)

	var toRemove []string
	for k := range latestTags {
		if _, found := desiredTags[k]; !found {
			toRemove = append(toRemove, k)
		}
	}

	var toAdd []svcsdktypes.Tag
	for k, v := range desiredTags {
		if latestValue, found := latestTags[k]; !found || latestValue != v {
			toAdd = append(toAdd, svcsdktypes.Tag{
				Key:   aws.String(k),
				Value: aws.String(v),
			})
		}
	}

	if len(toRemove) > 0 {
		_, err = rm.sdkapi.UntagResource(ctx, &svcsdk.UntagResourceInput{
			ResourceARN: arn,
			TagKeys:     toRemove,
		})
		rm.metrics.RecordAPICall("UPDATE", "UntagResource", err)
		if err != nil {
			return err
		}
	}
	if len(toAdd) > 0 {
		_, err = rm.sdkapi.TagResource(ctx, &svcsdk.TagResourceInput{
			ResourceARN: arn,
			Tags:        toAdd,
		})
		rm.metrics.RecordAPICall("UPDATE", "TagResource", err)
		if err != nil {
			return err
		}
	}
	return nil
}
