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
	"reflect"
	"sort"
	"strings"
	"testing"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	acktags "github.com/aws-controllers-k8s/runtime/pkg/tags"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

// toACKTagsForTest mirrors the generated convertToOrderedACKTags. pkg/util
// must not import a resource package, so it is reproduced here.
func toACKTagsForTest(tags []*svcapitypes.Tag) (acktags.Tags, []string) {
	result := acktags.NewTags()
	keyOrder := []string{}
	for _, t := range tags {
		if t.Key != nil {
			keyOrder = append(keyOrder, *t.Key)
			if t.Value != nil {
				result[*t.Key] = *t.Value
			} else {
				result[*t.Key] = ""
			}
		}
	}
	return result, keyOrder
}

func apiTags(kv ...string) []*svcapitypes.Tag {
	var tags []*svcapitypes.Tag
	for i := 0; i+1 < len(kv); i += 2 {
		tags = append(tags, &svcapitypes.Tag{
			Key:   aws.String(kv[i]),
			Value: aws.String(kv[i+1]),
		})
	}
	return tags
}

func sdkTags(kv ...string) []svcsdktypes.Tag {
	var tags []svcsdktypes.Tag
	for i := 0; i+1 < len(kv); i += 2 {
		tags = append(tags, svcsdktypes.Tag{
			Key:   aws.String(kv[i]),
			Value: aws.String(kv[i+1]),
		})
	}
	return tags
}

func TestComputeTagsDelta(t *testing.T) {
	tests := []struct {
		name         string
		desired      acktags.Tags
		latest       acktags.Tags
		wantToAdd    []svcsdktypes.Tag
		wantToRemove []string
	}{
		{
			name:      "add",
			desired:   acktags.Tags{"a": "1", "b": "2"},
			latest:    acktags.Tags{"a": "1"},
			wantToAdd: sdkTags("b", "2"),
		},
		{
			name:         "remove",
			desired:      acktags.Tags{"a": "1"},
			latest:       acktags.Tags{"a": "1", "b": "2"},
			wantToRemove: []string{"b"},
		},
		{
			name:      "value change",
			desired:   acktags.Tags{"a": "2"},
			latest:    acktags.Tags{"a": "1"},
			wantToAdd: sdkTags("a", "2"),
		},
		{
			name:    "no-op",
			desired: acktags.Tags{"a": "1", "b": "2"},
			latest:  acktags.Tags{"a": "1", "b": "2"},
		},
		{
			// Mirrors the e2e tag step: change one, remove one, add one.
			name:         "combined change, remove and add",
			desired:      acktags.Tags{"a": "updated", "c": "new"},
			latest:       acktags.Tags{"a": "initial", "b": "second"},
			wantToAdd:    sdkTags("a", "updated", "c", "new"),
			wantToRemove: []string{"b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotToAdd, gotToRemove := computeTagsDelta(tt.desired, tt.latest)

			// Map iteration order is random; sort before comparing.
			sort.Slice(gotToAdd, func(i, j int) bool {
				return *gotToAdd[i].Key < *gotToAdd[j].Key
			})
			sort.Strings(gotToRemove)

			if len(gotToAdd) != len(tt.wantToAdd) ||
				(len(gotToAdd) > 0 && !reflect.DeepEqual(gotToAdd, tt.wantToAdd)) {
				t.Errorf("toAdd = %v, want %v", tagsString(gotToAdd), tagsString(tt.wantToAdd))
			}
			if len(gotToRemove) != len(tt.wantToRemove) ||
				(len(gotToRemove) > 0 && !reflect.DeepEqual(gotToRemove, tt.wantToRemove)) {
				t.Errorf("toRemove = %v, want %v", gotToRemove, tt.wantToRemove)
			}
		})
	}
}

func tagsString(tags []svcsdktypes.Tag) []string {
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		out = append(out, aws.ToString(t.Key)+"="+aws.ToString(t.Value))
	}
	return out
}

func TestSyncTagsMissingARN(t *testing.T) {
	tests := []struct {
		name string
		meta *ackv1alpha1.ResourceMetadata
	}{
		{name: "nil metadata", meta: nil},
		{name: "nil ARN", meta: &ackv1alpha1.ResourceMetadata{ARN: nil}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := SyncTags(
				context.Background(),
				apiTags("a", "1"), nil, tt.meta,
				toACKTagsForTest, nil, nil,
			)
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !strings.Contains(err.Error(), "resource ARN is not yet known") {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestSyncTagsNoOpMakesNoAPICalls(t *testing.T) {
	arn := ackv1alpha1.AWSResourceName("arn:aws:fsx:us-west-2:123456789012:association/fs-0123456789abcdef0/dra-0123456789abcdef0")
	meta := &ackv1alpha1.ResourceMetadata{ARN: &arn}

	// The nil client and nil metrics are what make this test meaningful: any
	// TagResource/UntagResource call (or metrics record) would panic. Do not
	// replace them with real values.
	err := SyncTags(
		context.Background(),
		apiTags("a", "1", "b", "2"),
		apiTags("a", "1", "b", "2"),
		meta, toACKTagsForTest, nil, nil,
	)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
