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
	"errors"
	"strings"
	"testing"

	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

func TestSetDeleteDataInFileSystem(t *testing.T) {
	tests := []struct {
		name         string
		annotations  map[string]string
		want         *bool
		wantTerminal bool
	}{
		{name: "absent sends the false default", annotations: nil, want: ptr(false)},
		{name: "empty sends the false default", annotations: map[string]string{DeleteDataInFileSystemAnnotation: ""}, want: ptr(false)},
		{name: "true", annotations: map[string]string{DeleteDataInFileSystemAnnotation: "true"}, want: ptr(true)},
		{name: "false", annotations: map[string]string{DeleteDataInFileSystemAnnotation: "false"}, want: ptr(false)},
		{name: "invalid", annotations: map[string]string{DeleteDataInFileSystemAnnotation: "yes please"}, wantTerminal: true},
		{name: "unrelated sends the false default", annotations: map[string]string{"other": "true"}, want: ptr(false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &resource{ko: &svcapitypes.DataRepositoryAssociation{
				ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations},
			}}
			input := &svcsdk.DeleteDataRepositoryAssociationInput{}
			err := setDeleteDataInFileSystem(r, input)
			if tt.wantTerminal {
				if err == nil {
					t.Fatalf("expected terminal error, got nil")
				}
				var te *ackerr.TerminalError
				if !errors.As(err, &te) {
					t.Fatalf("expected terminal error, got %T: %v", err, err)
				}
				if input.DeleteDataInFileSystem != nil {
					t.Fatalf("input must not be set on error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// The service rejects a nil DeleteDataInFileSystem, so it must
			// always be set on success.
			got := input.DeleteDataInFileSystem
			if got == nil || *got != *tt.want {
				t.Fatalf("DeleteDataInFileSystem = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDeleteDataInFileSystemAnnotationName(t *testing.T) {
	if want := "fsx.services.k8s.aws/delete-data-in-file-system"; DeleteDataInFileSystemAnnotation != want {
		t.Fatalf("annotation = %q, want %q", DeleteDataInFileSystemAnnotation, want)
	}
}

func ptr(b bool) *bool { return &b }

func isTerminal(err error) bool {
	var te *ackerr.TerminalError
	return errors.As(err, &te)
}

func s3With(export, imp []string) *svcapitypes.S3DataRepositoryConfiguration {
	c := &svcapitypes.S3DataRepositoryConfiguration{}
	if export != nil {
		c.AutoExportPolicy = &svcapitypes.AutoExportPolicy{Events: aws.StringSlice(export)}
	}
	if imp != nil {
		c.AutoImportPolicy = &svcapitypes.AutoImportPolicy{Events: aws.StringSlice(imp)}
	}
	return c
}

func draWithS3(s3 *svcapitypes.S3DataRepositoryConfiguration) *resource {
	return &resource{ko: &svcapitypes.DataRepositoryAssociation{
		Spec: svcapitypes.DataRepositoryAssociationSpec{S3: s3},
	}}
}

func TestRejectUnexpressibleS3Changes(t *testing.T) {
	s3Delta := ackcompare.NewDelta()
	s3Delta.Add("Spec.S3", nil, nil)
	tests := []struct {
		name         string
		desired      *svcapitypes.S3DataRepositoryConfiguration
		latest       *svcapitypes.S3DataRepositoryConfiguration
		delta        *ackcompare.Delta
		wantTerminal bool
		wantPaths    []string
	}{
		{
			name:         "S3 removed while latest has export events",
			desired:      nil,
			latest:       s3With([]string{"NEW"}, nil),
			delta:        s3Delta,
			wantTerminal: true,
			wantPaths:    []string{"Spec.S3.AutoExportPolicy"},
		},
		{
			name:         "AutoImportPolicy removed while latest has import events",
			desired:      s3With([]string{"NEW"}, nil),
			latest:       s3With([]string{"NEW"}, []string{"NEW", "CHANGED"}),
			delta:        s3Delta,
			wantTerminal: true,
			wantPaths:    []string{"Spec.S3.AutoImportPolicy"},
		},
		{
			name:    "latest S3 without events, desired S3 nil",
			desired: nil,
			latest:  &svcapitypes.S3DataRepositoryConfiguration{},
			delta:   s3Delta,
		},
		{
			name:    "events changed but policies still present",
			desired: s3With([]string{"NEW", "CHANGED"}, []string{"NEW"}),
			latest:  s3With([]string{"NEW"}, []string{"NEW", "CHANGED"}),
			delta:   s3Delta,
		},
		{
			name:    "no Spec.S3 delta",
			desired: nil,
			latest:  s3With([]string{"NEW"}, []string{"NEW"}),
			delta:   ackcompare.NewDelta(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectUnexpressibleS3Changes(draWithS3(tt.desired), draWithS3(tt.latest), tt.delta)
			if !tt.wantTerminal {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !isTerminal(err) {
				t.Fatalf("expected terminal error, got %T: %v", err, err)
			}
			for _, p := range tt.wantPaths {
				if !strings.Contains(err.Error(), p) {
					t.Fatalf("error %q does not name %s", err, p)
				}
			}
		})
	}
}

func draWithLifecycle(lifecycle *string) *resource {
	return &resource{ko: &svcapitypes.DataRepositoryAssociation{
		Status: svcapitypes.DataRepositoryAssociationStatus{Lifecycle: lifecycle},
	}}
}

func TestRequeueIfNotUpdatable(t *testing.T) {
	tests := []struct {
		name        string
		lifecycle   *string
		wantRequeue bool
	}{
		{name: "CREATING", lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleCreating)), wantRequeue: true},
		{name: "UPDATING", lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleUpdating)), wantRequeue: true},
		{name: "DELETING", lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleDeleting)), wantRequeue: true},
		{name: "AVAILABLE", lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleAvailable))},
		{name: "MISCONFIGURED", lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleMisconfigured))},
		{name: "nil", lifecycle: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requeueIfNotUpdatable(draWithLifecycle(tt.lifecycle))
			if !tt.wantRequeue {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			var rq *ackrequeue.RequeueNeededAfter
			if !errors.As(err, &rq) {
				t.Fatalf("expected RequeueNeededAfter, got %T: %v", err, err)
			}
		})
	}
}

func TestTerminalIfFailed(t *testing.T) {
	now := metav1.Now()
	failed := aws.String(string(svcsdktypes.DataRepositoryLifecycleFailed))
	tests := []struct {
		name         string
		ko           *svcapitypes.DataRepositoryAssociation
		wantTerminal bool
		wantMessage  string
	}{
		{
			name: "FAILED with message",
			ko: &svcapitypes.DataRepositoryAssociation{Status: svcapitypes.DataRepositoryAssociationStatus{
				Lifecycle:      failed,
				FailureDetails: &svcapitypes.DataRepositoryFailureDetails{Message: aws.String("bucket in wrong region")},
			}},
			wantTerminal: true,
			wantMessage:  "bucket in wrong region",
		},
		{
			name: "FAILED without FailureDetails",
			ko: &svcapitypes.DataRepositoryAssociation{Status: svcapitypes.DataRepositoryAssociationStatus{
				Lifecycle: failed,
			}},
			wantTerminal: true,
			wantMessage:  "no failure details reported",
		},
		{
			name: "FAILED while being deleted",
			ko: &svcapitypes.DataRepositoryAssociation{
				ObjectMeta: metav1.ObjectMeta{DeletionTimestamp: &now},
				Status:     svcapitypes.DataRepositoryAssociationStatus{Lifecycle: failed},
			},
		},
		{
			name: "MISCONFIGURED",
			ko: &svcapitypes.DataRepositoryAssociation{Status: svcapitypes.DataRepositoryAssociationStatus{
				Lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleMisconfigured)),
			}},
		},
		{
			name: "AVAILABLE",
			ko: &svcapitypes.DataRepositoryAssociation{Status: svcapitypes.DataRepositoryAssociationStatus{
				Lifecycle: aws.String(string(svcsdktypes.DataRepositoryLifecycleAvailable)),
			}},
		},
		{name: "nil", ko: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := terminalIfFailed(tt.ko)
			if !tt.wantTerminal {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if !isTerminal(err) {
				t.Fatalf("expected terminal error, got %T: %v", err, err)
			}
			if !strings.Contains(err.Error(), tt.wantMessage) {
				t.Fatalf("error %q does not contain %q", err, tt.wantMessage)
			}
		})
	}
}

func TestSetDeletingLifecycle(t *testing.T) {
	tests := []struct {
		name string
		resp *svcsdk.DeleteDataRepositoryAssociationOutput
		want string
	}{
		{
			name: "lifecycle from response",
			resp: &svcsdk.DeleteDataRepositoryAssociationOutput{Lifecycle: svcsdktypes.DataRepositoryLifecycleDeleting},
			want: "DELETING",
		},
		{
			name: "empty response lifecycle falls back to DELETING",
			resp: &svcsdk.DeleteDataRepositoryAssociationOutput{},
			want: "DELETING",
		},
		{name: "nil response falls back to DELETING", resp: nil, want: "DELETING"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := draWithLifecycle(aws.String(string(svcsdktypes.DataRepositoryLifecycleAvailable)))
			setDeletingLifecycle(r, tt.resp)
			if got := r.ko.Status.Lifecycle; got == nil || *got != tt.want {
				t.Fatalf("Status.Lifecycle = %v, want %s", got, tt.want)
			}
		})
	}
}

func TestKeepDataRepositoryPathSpelling(t *testing.T) {
	tests := []struct {
		name    string
		desired string
		fsx     string
		want    string
	}{
		{name: "no slash keeps desired spelling", desired: "s3://b/p", fsx: "s3://b/p/", want: "s3://b/p"},
		{name: "slash unchanged", desired: "s3://b/p/", fsx: "s3://b/p/", want: "s3://b/p/"},
		{name: "different path keeps FSx value", desired: "s3://b/p", fsx: "s3://b/other/", want: "s3://b/other/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired := &svcapitypes.DataRepositoryAssociation{}
			desired.Spec.DataRepositoryPath = aws.String(tt.desired)
			ko := &svcapitypes.DataRepositoryAssociation{}
			ko.Spec.DataRepositoryPath = aws.String(tt.fsx)
			keepDataRepositoryPathSpelling(desired, ko)
			if got := aws.ToString(ko.Spec.DataRepositoryPath); got != tt.want {
				t.Fatalf("DataRepositoryPath = %q, want %q", got, tt.want)
			}
		})
	}
}
