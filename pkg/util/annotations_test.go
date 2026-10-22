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
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

func TestParseDeletionAnnotations(t *testing.T) {
	const childVolumes = svcsdktypes.DeleteFileSystemOpenZFSOptionDeleteChildVolumesAndSnapshots
	tests := []struct {
		name        string
		annotations map[string]string
		// nil means unset, leaving FSx's per-type default in effect.
		wantSkipFinalBackup *bool
		wantOptions         []svcsdktypes.DeleteFileSystemOpenZFSOption
		wantErr             bool
	}{
		{name: "nil annotations leave everything unset"},
		{name: "empty annotations leave everything unset", annotations: map[string]string{}},
		{
			name:        "unrelated annotations leave everything unset",
			annotations: map[string]string{"example.com/other": "false"},
		},
		{
			name:        "empty value is treated as absent",
			annotations: map[string]string{svcapitypes.SkipFinalBackupAnnotation: ""},
		},
		{
			name:                "false takes a final backup",
			annotations:         map[string]string{svcapitypes.SkipFinalBackupAnnotation: "false"},
			wantSkipFinalBackup: aws.Bool(false),
		},
		{
			name:                "true skips the final backup",
			annotations:         map[string]string{svcapitypes.SkipFinalBackupAnnotation: "true"},
			wantSkipFinalBackup: aws.Bool(true),
		},
		{
			name:                "ParseBool accepts capitalised False",
			annotations:         map[string]string{svcapitypes.SkipFinalBackupAnnotation: "False"},
			wantSkipFinalBackup: aws.Bool(false),
		},
		{
			name:        "an unparseable skip-final-backup value is an error",
			annotations: map[string]string{svcapitypes.SkipFinalBackupAnnotation: "nope"},
			wantErr:     true,
		},
		{
			name: "openzfs option parsed",
			annotations: map[string]string{
				svcapitypes.OpenZFSDeleteOptionsAnnotation: "DELETE_CHILD_VOLUMES_AND_SNAPSHOTS",
			},
			wantOptions: []svcsdktypes.DeleteFileSystemOpenZFSOption{childVolumes},
		},
		{
			name: "openzfs options tolerate spaces around commas",
			annotations: map[string]string{
				svcapitypes.OpenZFSDeleteOptionsAnnotation: " DELETE_CHILD_VOLUMES_AND_SNAPSHOTS ",
			},
			wantOptions: []svcsdktypes.DeleteFileSystemOpenZFSOption{childVolumes},
		},
		{
			// The old boolean form must not be silently read as "no options".
			name:        "an unknown openzfs option is an error",
			annotations: map[string]string{svcapitypes.OpenZFSDeleteOptionsAnnotation: "true"},
			wantErr:     true,
		},
		{
			name:        "openzfs options are case-sensitive, like the API",
			annotations: map[string]string{svcapitypes.OpenZFSDeleteOptionsAnnotation: "delete_child_volumes_and_snapshots"},
			wantErr:     true,
		},
		{
			name: "both annotations parsed independently",
			annotations: map[string]string{
				svcapitypes.SkipFinalBackupAnnotation:      "false",
				svcapitypes.OpenZFSDeleteOptionsAnnotation: "DELETE_CHILD_VOLUMES_AND_SNAPSHOTS",
			},
			wantSkipFinalBackup: aws.Bool(false),
			wantOptions:         []svcsdktypes.DeleteFileSystemOpenZFSOption{childVolumes},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params, err := ParseDeletionAnnotations(test.annotations)
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got params %+v", params)
				}
				if params != nil {
					t.Errorf("expected nil params alongside the error, got %+v", params)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(params.SkipFinalBackup, test.wantSkipFinalBackup) {
				t.Errorf("SkipFinalBackup = %v, want %v",
					fmtBool(params.SkipFinalBackup), fmtBool(test.wantSkipFinalBackup))
			}
			if !reflect.DeepEqual(params.OpenZFSOptions, test.wantOptions) {
				t.Errorf("OpenZFSOptions = %v, want %v", params.OpenZFSOptions, test.wantOptions)
			}
		})
	}
}

func fmtBool(b *bool) string {
	if b == nil {
		return "<unset>"
	}
	if *b {
		return "true"
	}
	return "false"
}
