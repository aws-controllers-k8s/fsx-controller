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

package util

import (
	"context"
	"fmt"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackmetrics "github.com/aws-controllers-k8s/runtime/pkg/metrics"
	acktags "github.com/aws-controllers-k8s/runtime/pkg/tags"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

// SyncTags brings a resource's AWS tags in line with desiredTags using the
// FSx TagResource/UntagResource operations. It is shared by every tagged FSx
// resource. FSx has no bulk "replace tags" call, so removals and additions are
// issued as separate calls (not atomic).
//
// latestMeta is the resource's Status.ACKResourceMetadata; each resource marks
// ResourceARN `is_arn: true`, so a read populates its ARN. The nil guard is
// defensive. toACKTags is the resource's generated convertToOrderedACKTags.
func SyncTags(
	ctx context.Context,
	desiredTags []*svcapitypes.Tag,
	latestTags []*svcapitypes.Tag,
	latestMeta *ackv1alpha1.ResourceMetadata,
	toACKTags func([]*svcapitypes.Tag) (acktags.Tags, []string),
	client *svcsdk.Client,
	metrics *ackmetrics.Metrics,
) error {
	var arn *string
	if latestMeta != nil && latestMeta.ARN != nil {
		arn = aws.String(string(*latestMeta.ARN))
	}
	if arn == nil {
		return fmt.Errorf("cannot sync tags: resource ARN is not yet known")
	}

	desired, _ := toACKTags(desiredTags)
	latest, _ := toACKTags(latestTags)

	toAdd, toRemove := computeTagsDelta(desired, latest)

	if len(toRemove) > 0 {
		_, err := client.UntagResource(ctx, &svcsdk.UntagResourceInput{
			ResourceARN: arn,
			TagKeys:     toRemove,
		})
		metrics.RecordAPICall("UPDATE", "UntagResource", err)
		if err != nil {
			return err
		}
	}
	if len(toAdd) > 0 {
		_, err := client.TagResource(ctx, &svcsdk.TagResourceInput{
			ResourceARN: arn,
			Tags:        toAdd,
		})
		metrics.RecordAPICall("UPDATE", "TagResource", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// computeTagsDelta returns the tags to add or update and the keys to remove
// to bring latest in line with desired.
func computeTagsDelta(
	desired acktags.Tags,
	latest acktags.Tags,
) (toAdd []svcsdktypes.Tag, toRemove []string) {
	for k := range latest {
		if _, found := desired[k]; !found {
			toRemove = append(toRemove, k)
		}
	}

	for k, v := range desired {
		if latestValue, found := latest[k]; !found || latestValue != v {
			toAdd = append(toAdd, svcsdktypes.Tag{
				Key:   aws.String(k),
				Value: aws.String(v),
			})
		}
	}
	return toAdd, toRemove
}
