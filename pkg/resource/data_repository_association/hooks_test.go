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
	"testing"

	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
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
