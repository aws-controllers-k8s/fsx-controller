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
	"fmt"
	"strconv"
	"strings"

	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

// DeleteInputAnnotationParameters holds delete-time options read from
// annotations rather than the Spec: they only take effect when the file system
// is deleted, so there is nothing to reconcile against.
type DeleteInputAnnotationParameters struct {
	// SkipFinalBackup is nil when the annotation is absent, leaving FSx's own
	// per-type default in effect.
	SkipFinalBackup *bool
	// OpenZFSOptions is empty when the annotation is absent.
	OpenZFSOptions []svcsdktypes.DeleteFileSystemOpenZFSOption
}

// ParseDeletionAnnotations parses the deletion annotations. An unparseable value
// is an error rather than a silent default, so a typo cannot quietly change what
// a delete destroys.
func ParseDeletionAnnotations(annotations map[string]string) (*DeleteInputAnnotationParameters, error) {
	params := &DeleteInputAnnotationParameters{}

	if value, ok := annotations[svcapitypes.SkipFinalBackupAnnotation]; ok && value != "" {
		skipFinalBackup, err := strconv.ParseBool(value)
		if err != nil {
			return nil, fmt.Errorf(
				"invalid value %q for annotation %s: %v",
				value, svcapitypes.SkipFinalBackupAnnotation, err,
			)
		}
		params.SkipFinalBackup = &skipFinalBackup
	}

	if value, ok := annotations[svcapitypes.OpenZFSDeleteOptionsAnnotation]; ok && value != "" {
		// Validated against the SDK enum rather than a hard-coded list, so a
		// value FSx adds later is accepted after an SDK bump with no code change.
		known := svcsdktypes.DeleteFileSystemOpenZFSOption("").Values()
		for _, raw := range strings.Split(value, ",") {
			opt := svcsdktypes.DeleteFileSystemOpenZFSOption(strings.TrimSpace(raw))
			if !containsOption(known, opt) {
				return nil, fmt.Errorf(
					"invalid value %q for annotation %s: want a comma-separated list of %v",
					value, svcapitypes.OpenZFSDeleteOptionsAnnotation, known,
				)
			}
			params.OpenZFSOptions = append(params.OpenZFSOptions, opt)
		}
	}

	return params, nil
}

func containsOption(
	known []svcsdktypes.DeleteFileSystemOpenZFSOption,
	opt svcsdktypes.DeleteFileSystemOpenZFSOption,
) bool {
	for _, k := range known {
		if k == opt {
			return true
		}
	}
	return false
}
