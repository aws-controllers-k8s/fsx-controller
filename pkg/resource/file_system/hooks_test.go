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

package file_system

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	corev1 "k8s.io/api/core/v1"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
)

// deltaAt builds a Delta containing exactly the supplied field paths. The
// before/after values are irrelevant: every consumer in hooks.go only asks the
// delta *whether* a path differs and then reads the value from the Spec.
func deltaAt(paths ...string) *ackcompare.Delta {
	delta := ackcompare.NewDelta()
	for _, p := range paths {
		delta.Add(p, nil, nil)
	}
	return delta
}

// newResource wraps a FileSystemSpec in the package's resource type, giving it
// a Status.FileSystemID so newUpdateRequestPayload has an ID to send.
func newResource(spec svcapitypes.FileSystemSpec) *resource {
	return &resource{
		ko: &svcapitypes.FileSystem{
			Spec: spec,
			Status: svcapitypes.FileSystemStatus{
				FileSystemID: aws.String("fs-0123456789abcdef0"),
			},
		},
	}
}

func TestRouteTableIDsDelta(t *testing.T) {
	strs := func(ss ...string) []*string {
		out := make([]*string, 0, len(ss))
		for _, s := range ss {
			out = append(out, aws.String(s))
		}
		return out
	}

	tests := []struct {
		name       string
		desired    []*string
		latest     []*string
		wantAdd    []string
		wantRemove []string
	}{
		{
			name:    "no-op: identical",
			desired: strs("rtb-1", "rtb-2"),
			latest:  strs("rtb-1", "rtb-2"),
		},
		{
			name:    "no-op: identical but reordered",
			desired: strs("rtb-2", "rtb-1"),
			latest:  strs("rtb-1", "rtb-2"),
		},
		{
			name:    "no-op: both empty",
			desired: nil,
			latest:  nil,
		},
		{
			name:    "add only",
			desired: strs("rtb-1", "rtb-2"),
			latest:  strs("rtb-1"),
			wantAdd: []string{"rtb-2"},
		},
		{
			name:    "add to empty latest",
			desired: strs("rtb-1"),
			latest:  nil,
			wantAdd: []string{"rtb-1"},
		},
		{
			name:       "remove only",
			desired:    strs("rtb-1"),
			latest:     strs("rtb-1", "rtb-2"),
			wantRemove: []string{"rtb-2"},
		},
		{
			name:       "remove all",
			desired:    nil,
			latest:     strs("rtb-1", "rtb-2"),
			wantRemove: []string{"rtb-1", "rtb-2"},
		},
		{
			name:       "add and remove",
			desired:    strs("rtb-1", "rtb-3"),
			latest:     strs("rtb-1", "rtb-2"),
			wantAdd:    []string{"rtb-3"},
			wantRemove: []string{"rtb-2"},
		},
		{
			name:       "nil elements are skipped",
			desired:    []*string{nil, aws.String("rtb-3"), nil},
			latest:     []*string{aws.String("rtb-2"), nil},
			wantAdd:    []string{"rtb-3"},
			wantRemove: []string{"rtb-2"},
		},
		{
			name:    "only nil elements",
			desired: []*string{nil},
			latest:  []*string{nil},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			add, remove := routeTableIDsDelta(tt.desired, tt.latest)
			assertStrings(t, "add", add, tt.wantAdd)
			assertStrings(t, "remove", remove, tt.wantRemove)
		})
	}
}

func assertStrings(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: got %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: got %v, want %v", label, got, want)
		}
	}
}

func TestInt32Converter(t *testing.T) {
	t.Run("nil input yields nil without error", func(t *testing.T) {
		conv := &int32Converter{}
		if got := conv.convert("Spec.X", nil); got != nil {
			t.Fatalf("got %v, want nil", *got)
		}
		if conv.err != nil {
			t.Fatalf("unexpected error: %v", conv.err)
		}
	})

	t.Run("in-range value is narrowed", func(t *testing.T) {
		conv := &int32Converter{}
		got := conv.convert("Spec.StorageCapacity", aws.Int64(1200))
		if got == nil || *got != 1200 {
			t.Fatalf("got %v, want 1200", got)
		}
		if conv.err != nil {
			t.Fatalf("unexpected error: %v", conv.err)
		}
	})

	t.Run("boundary values are accepted", func(t *testing.T) {
		conv := &int32Converter{}
		for _, v := range []int64{math.MaxInt32, math.MinInt32} {
			got := conv.convert("Spec.X", aws.Int64(v))
			if got == nil || int64(*got) != v {
				t.Fatalf("got %v, want %d", got, v)
			}
		}
		if conv.err != nil {
			t.Fatalf("unexpected error: %v", conv.err)
		}
	})

	t.Run("overflow records a terminal error", func(t *testing.T) {
		for _, v := range []int64{math.MaxInt32 + 1, math.MinInt32 - 1} {
			conv := &int32Converter{}
			if got := conv.convert("Spec.StorageCapacity", aws.Int64(v)); got != nil {
				t.Fatalf("got %v, want nil for out-of-range %d", *got, v)
			}
			if conv.err == nil {
				t.Fatalf("expected an error for out-of-range %d", v)
			}
			var terminal *ackerr.TerminalError
			if !errors.As(conv.err, &terminal) {
				t.Fatalf("expected a *ackerr.TerminalError, got %T: %v", conv.err, conv.err)
			}
		}
	})

	t.Run("short-circuits after the first failure", func(t *testing.T) {
		conv := &int32Converter{}
		_ = conv.convert("Spec.First", aws.Int64(math.MaxInt32+1))
		first := conv.err
		if first == nil {
			t.Fatal("expected an error from the first conversion")
		}
		// A subsequent, perfectly valid conversion must return nil and must not
		// overwrite the recorded error.
		if got := conv.convert("Spec.Second", aws.Int64(42)); got != nil {
			t.Fatalf("got %v, want nil after a prior failure", *got)
		}
		if conv.err != first {
			t.Fatalf("error was overwritten: got %v, want %v", conv.err, first)
		}
	})

	t.Run("terminal error surfaces from newUpdateRequestPayload", func(t *testing.T) {
		rm := &resourceManager{}
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType:  aws.String("LUSTRE"),
			StorageCapacity: aws.Int64(math.MaxInt32 + 1),
		})
		input, err := rm.newUpdateRequestPayload(
			context.Background(), desired, desired, deltaAt("Spec.StorageCapacity"),
		)
		if input != nil {
			t.Fatalf("got input %v, want nil", input)
		}
		var terminal *ackerr.TerminalError
		if !errors.As(err, &terminal) {
			t.Fatalf("expected a *ackerr.TerminalError, got %T: %v", err, err)
		}
	})
}

func TestNewUpdateRequestPayload(t *testing.T) {
	ctx := context.Background()
	rm := &resourceManager{}

	t.Run("lustre", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("LUSTRE"),
			LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
				WeeklyMaintenanceStartTime: aws.String("4:05:30"),
			},
		})
		input, err := rm.newUpdateRequestPayload(
			ctx, desired, desired,
			deltaAt("Spec.LustreConfiguration.WeeklyMaintenanceStartTime"),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input == nil {
			t.Fatal("expected a non-nil input")
		}
		if input.FileSystemId == nil || *input.FileSystemId != "fs-0123456789abcdef0" {
			t.Fatalf("unexpected FileSystemId: %v", input.FileSystemId)
		}
		if input.LustreConfiguration == nil {
			t.Fatal("expected LustreConfiguration to be built")
		}
		if got := aws.ToString(input.LustreConfiguration.WeeklyMaintenanceStartTime); got != "4:05:30" {
			t.Fatalf("got WeeklyMaintenanceStartTime %q, want %q", got, "4:05:30")
		}
		if input.OntapConfiguration != nil || input.OpenZFSConfiguration != nil ||
			input.WindowsConfiguration != nil {
			t.Fatal("expected only the Lustre sub-configuration to be built")
		}
	})

	t.Run("ontap translates RouteTableIDs into Add/Remove", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("ONTAP"),
			OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{
				RouteTableIDs:      []*string{aws.String("rtb-1"), aws.String("rtb-3")},
				ThroughputCapacity: aws.Int64(256),
			},
		})
		latest := newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("ONTAP"),
			OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{
				RouteTableIDs:      []*string{aws.String("rtb-1"), aws.String("rtb-2")},
				ThroughputCapacity: aws.Int64(128),
			},
		})
		input, err := rm.newUpdateRequestPayload(
			ctx, desired, latest,
			deltaAt(
				"Spec.OntapConfiguration.RouteTableIDs",
				"Spec.OntapConfiguration.ThroughputCapacity",
			),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input == nil || input.OntapConfiguration == nil {
			t.Fatal("expected OntapConfiguration to be built")
		}
		assertStrings(t, "AddRouteTableIds", input.OntapConfiguration.AddRouteTableIds, []string{"rtb-3"})
		assertStrings(t, "RemoveRouteTableIds", input.OntapConfiguration.RemoveRouteTableIds, []string{"rtb-2"})
		if got := aws.ToInt32(input.OntapConfiguration.ThroughputCapacity); got != 256 {
			t.Fatalf("got ThroughputCapacity %d, want 256", got)
		}
		if input.LustreConfiguration != nil || input.OpenZFSConfiguration != nil ||
			input.WindowsConfiguration != nil {
			t.Fatal("expected only the ONTAP sub-configuration to be built")
		}
	})

	t.Run("openzfs", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("OPENZFS"),
			OpenZFSConfiguration: &svcapitypes.CreateFileSystemOpenZFSConfiguration{
				CopyTagsToVolumes: aws.Bool(true),
			},
		})
		input, err := rm.newUpdateRequestPayload(
			ctx, desired, desired,
			deltaAt("Spec.OpenZFSConfiguration.CopyTagsToVolumes"),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input == nil || input.OpenZFSConfiguration == nil {
			t.Fatal("expected OpenZFSConfiguration to be built")
		}
		if !aws.ToBool(input.OpenZFSConfiguration.CopyTagsToVolumes) {
			t.Fatal("expected CopyTagsToVolumes to be true")
		}
		if input.LustreConfiguration != nil || input.OntapConfiguration != nil ||
			input.WindowsConfiguration != nil {
			t.Fatal("expected only the OpenZFS sub-configuration to be built")
		}
	})

	t.Run("windows", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("WINDOWS"),
			WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{
				AutomaticBackupRetentionDays: aws.Int64(7),
			},
		})
		input, err := rm.newUpdateRequestPayload(
			ctx, desired, desired,
			deltaAt("Spec.WindowsConfiguration.AutomaticBackupRetentionDays"),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input == nil || input.WindowsConfiguration == nil {
			t.Fatal("expected WindowsConfiguration to be built")
		}
		if got := aws.ToInt32(input.WindowsConfiguration.AutomaticBackupRetentionDays); got != 7 {
			t.Fatalf("got AutomaticBackupRetentionDays %d, want 7", got)
		}
		if input.LustreConfiguration != nil || input.OntapConfiguration != nil ||
			input.OpenZFSConfiguration != nil {
			t.Fatal("expected only the Windows sub-configuration to be built")
		}
	})

	t.Run("top-level members", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType:        aws.String("LUSTRE"),
			StorageCapacity:       aws.Int64(2400),
			StorageType:           aws.String("SSD"),
			FileSystemTypeVersion: aws.String("2.15"),
			NetworkType:           aws.String("DUAL"),
		})
		input, err := rm.newUpdateRequestPayload(
			ctx, desired, desired,
			deltaAt(
				"Spec.StorageCapacity", "Spec.StorageType",
				"Spec.FileSystemTypeVersion", "Spec.NetworkType",
			),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input == nil {
			t.Fatal("expected a non-nil input")
		}
		if got := aws.ToInt32(input.StorageCapacity); got != 2400 {
			t.Fatalf("got StorageCapacity %d, want 2400", got)
		}
		if string(input.StorageType) != "SSD" {
			t.Fatalf("got StorageType %q, want SSD", input.StorageType)
		}
		if got := aws.ToString(input.FileSystemTypeVersion); got != "2.15" {
			t.Fatalf("got FileSystemTypeVersion %q, want 2.15", got)
		}
		if string(input.NetworkType) != "DUAL" {
			t.Fatalf("got NetworkType %q, want DUAL", input.NetworkType)
		}
	})

	t.Run("delta with nothing UpdateFileSystem can act on yields (nil, nil)", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("LUSTRE"),
			LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
				WeeklyMaintenanceStartTime: aws.String("4:05:30"),
			},
			Tags: []*svcapitypes.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
		})
		// Tags are handled by TagResource/UntagResource, and SubnetIDs is
		// immutable: neither is a member of UpdateFileSystem.
		input, err := rm.newUpdateRequestPayload(
			ctx, desired, desired, deltaAt("Spec.Tags", "Spec.SubnetIDs"),
		)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if input != nil {
			t.Fatalf("expected a nil input, got %+v", input)
		}
	})

	// A field removed from the spec must not produce an UpdateFileSystem
	// request: with nothing to assign, the request would carry an empty
	// per-type config, which FSx rejects with MissingFileSystemConfiguration --
	// a terminal code, so the CR would wedge.
	t.Run("removing fields from the spec is a no-op", func(t *testing.T) {
		cases := []struct {
			name  string
			spec  svcapitypes.FileSystemSpec
			paths []string
		}{
			{
				name: "lustre numeric members cleared",
				spec: svcapitypes.FileSystemSpec{
					FileSystemType:      aws.String("LUSTRE"),
					LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{},
				},
				paths: []string{
					"Spec.LustreConfiguration.AutomaticBackupRetentionDays",
					"Spec.LustreConfiguration.PerUnitStorageThroughput",
					"Spec.LustreConfiguration.ThroughputCapacity",
				},
			},
			{
				name: "ontap numeric members cleared",
				spec: svcapitypes.FileSystemSpec{
					FileSystemType:     aws.String("ONTAP"),
					OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{},
				},
				paths: []string{
					"Spec.OntapConfiguration.AutomaticBackupRetentionDays",
					"Spec.OntapConfiguration.HAPairs",
					"Spec.OntapConfiguration.ThroughputCapacity",
					"Spec.OntapConfiguration.ThroughputCapacityPerHAPair",
				},
			},
			{
				name: "openzfs numeric and boolean members cleared",
				spec: svcapitypes.FileSystemSpec{
					FileSystemType:       aws.String("OPENZFS"),
					OpenZFSConfiguration: &svcapitypes.CreateFileSystemOpenZFSConfiguration{},
				},
				paths: []string{
					"Spec.OpenZFSConfiguration.AutomaticBackupRetentionDays",
					"Spec.OpenZFSConfiguration.CopyTagsToBackups",
					"Spec.OpenZFSConfiguration.CopyTagsToVolumes",
					"Spec.OpenZFSConfiguration.ThroughputCapacity",
				},
			},
			{
				name: "windows numeric members cleared",
				spec: svcapitypes.FileSystemSpec{
					FileSystemType:       aws.String("WINDOWS"),
					WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{},
				},
				paths: []string{
					"Spec.WindowsConfiguration.AutomaticBackupRetentionDays",
					"Spec.WindowsConfiguration.ThroughputCapacity",
				},
			},
			{
				name: "top-level StorageCapacity cleared",
				spec: svcapitypes.FileSystemSpec{
					FileSystemType: aws.String("LUSTRE"),
				},
				paths: []string{"Spec.StorageCapacity"},
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				desired := newResource(tc.spec)
				input, err := rm.newUpdateRequestPayload(
					ctx, desired, desired, deltaAt(tc.paths...),
				)
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if input != nil {
					t.Fatalf("expected a nil input, got %+v", input)
				}
			})
		}
	})
}

// newAnnotatedResource builds a resource of the supplied file system type with
// the supplied annotations. A nil annotations map is left nil rather than being
// replaced with an empty map, so the "annotation absent" cases exercise the
// same nil path the runtime produces for a CR with no annotations at all.
func newAnnotatedResource(fileSystemType *string, annotations map[string]string) *resource {
	r := newResource(svcapitypes.FileSystemSpec{FileSystemType: fileSystemType})
	r.ko.ObjectMeta.Annotations = annotations
	return r
}

// skipFinalBackupAnnotation returns an annotation map setting the
// skip-final-backup annotation to the supplied raw value.
func skipFinalBackupAnnotation(value string) map[string]string {
	return map[string]string{svcapitypes.SkipFinalBackupAnnotation: value}
}

func TestSetDeleteFileSystemConfiguration(t *testing.T) {
	tests := []struct {
		name           string
		fileSystemType *string
		annotations    map[string]string
		// wantSkip is the expected SkipFinalBackup on whichever per-type
		// config the file system type selects. nil means that config is
		// expected to be left unset entirely.
		wantSkip *bool
	}{
		{
			name:           "lustre, no annotation, skips by default",
			fileSystemType: aws.String("LUSTRE"),
			wantSkip:       aws.Bool(true),
		},
		{
			name:           "lustre, annotation false, takes a final backup",
			fileSystemType: aws.String("LUSTRE"),
			annotations:    skipFinalBackupAnnotation("false"),
			wantSkip:       aws.Bool(false),
		},
		{
			name:           "windows, no annotation, overrides the AWS default of taking a backup",
			fileSystemType: aws.String("WINDOWS"),
			wantSkip:       aws.Bool(true),
		},
		{
			name:           "windows, annotation false, restores the AWS default",
			fileSystemType: aws.String("WINDOWS"),
			annotations:    skipFinalBackupAnnotation("false"),
			wantSkip:       aws.Bool(false),
		},
		{
			name:           "openzfs, no annotation, overrides the AWS default of taking a backup",
			fileSystemType: aws.String("OPENZFS"),
			wantSkip:       aws.Bool(true),
		},
		{
			name:           "openzfs, annotation false, restores the AWS default",
			fileSystemType: aws.String("OPENZFS"),
			annotations:    skipFinalBackupAnnotation("false"),
			wantSkip:       aws.Bool(false),
		},
		{
			name:           "annotation true is honoured explicitly",
			fileSystemType: aws.String("OPENZFS"),
			annotations:    skipFinalBackupAnnotation("true"),
			wantSkip:       aws.Bool(true),
		},
		{
			name:           "unrelated annotations do not disturb the default",
			fileSystemType: aws.String("LUSTRE"),
			annotations:    map[string]string{"example.com/other": "false"},
			wantSkip:       aws.Bool(true),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAnnotatedResource(tt.fileSystemType, tt.annotations)
			input := &svcsdk.DeleteFileSystemInput{}

			if err := setDeleteFileSystemConfiguration(r, input); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Exactly one per-type config must be populated, and it must be
			// the one matching Spec.FileSystemType. A value landing on the
			// wrong config would be silently ignored by FSx.
			var got *bool
			switch *tt.fileSystemType {
			case "LUSTRE":
				if input.LustreConfiguration == nil {
					t.Fatal("LustreConfiguration was not set")
				}
				got = input.LustreConfiguration.SkipFinalBackup
				if input.WindowsConfiguration != nil || input.OpenZFSConfiguration != nil {
					t.Error("a non-Lustre config was also set")
				}
			case "WINDOWS":
				if input.WindowsConfiguration == nil {
					t.Fatal("WindowsConfiguration was not set")
				}
				got = input.WindowsConfiguration.SkipFinalBackup
				if input.LustreConfiguration != nil || input.OpenZFSConfiguration != nil {
					t.Error("a non-Windows config was also set")
				}
			case "OPENZFS":
				if input.OpenZFSConfiguration == nil {
					t.Fatal("OpenZFSConfiguration was not set")
				}
				got = input.OpenZFSConfiguration.SkipFinalBackup
				if input.LustreConfiguration != nil || input.WindowsConfiguration != nil {
					t.Error("a non-OpenZFS config was also set")
				}
				// Cascading deletion is opt-in: without the annotation no
				// options are sent, so FSx surfaces the dependency error
				// instead of destroying child volumes.
				if len(input.OpenZFSConfiguration.Options) != 0 {
					t.Errorf(
						"OpenZFS Options = %v, want none without the cascade-delete annotation",
						input.OpenZFSConfiguration.Options,
					)
				}
			}

			if got == nil {
				t.Fatal("SkipFinalBackup was not set")
			}
			if *got != *tt.wantSkip {
				t.Errorf("SkipFinalBackup = %v, want %v", *got, *tt.wantSkip)
			}
		})
	}
}

func TestSetDeleteFileSystemConfigurationCascadeDelete(t *testing.T) {
	tests := []struct {
		name        string
		annotations map[string]string
		wantOptions int
	}{
		{"absent means no cascade", nil, 0},
		{"explicit false means no cascade", map[string]string{
			svcapitypes.CascadeDeleteAnnotation: "false",
		}, 0},
		{"true opts in", map[string]string{
			svcapitypes.CascadeDeleteAnnotation: "true",
		}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newAnnotatedResource(aws.String("OPENZFS"), tt.annotations)
			input := &svcsdk.DeleteFileSystemInput{}
			if err := setDeleteFileSystemConfiguration(r, input); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			got := input.OpenZFSConfiguration.Options
			if len(got) != tt.wantOptions {
				t.Fatalf("Options = %v, want %d entries", got, tt.wantOptions)
			}
			if tt.wantOptions == 1 &&
				got[0] != svcsdktypes.DeleteFileSystemOpenZFSOptionDeleteChildVolumesAndSnapshots {
				t.Errorf("Options[0] = %v, want DELETE_CHILD_VOLUMES_AND_SNAPSHOTS", got[0])
			}
		})
	}
}

// TestSetDeleteFileSystemConfigurationCascadeIgnoredForOtherTypes pins that the
// annotation cannot smuggle options onto types that have no such field.
func TestSetDeleteFileSystemConfigurationCascadeIgnoredForOtherTypes(t *testing.T) {
	for _, fsType := range []string{"LUSTRE", "WINDOWS"} {
		t.Run(fsType, func(t *testing.T) {
			r := newAnnotatedResource(aws.String(fsType), map[string]string{
				svcapitypes.CascadeDeleteAnnotation: "true",
			})
			input := &svcsdk.DeleteFileSystemInput{}
			if err := setDeleteFileSystemConfiguration(r, input); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if input.OpenZFSConfiguration != nil {
				t.Error("OpenZFSConfiguration should not be set for " + fsType)
			}
		})
	}
}

func TestRestoreNonRoundTrippedSpecFields(t *testing.T) {
	desired := newResource(svcapitypes.FileSystemSpec{
		LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
			// Set so that restoring it would be detectable: with this nil the
			// assertion below passes whether or not the hook copies it.
			AutoImportPolicy:      aws.String("NEW"),
			ExportPath:            aws.String("s3://bucket/export"),
			ImportPath:            aws.String("s3://bucket/import"),
			ImportedFileChunkSize: aws.Int64(2048),
		},
		OpenZFSConfiguration: &svcapitypes.CreateFileSystemOpenZFSConfiguration{
			RootVolumeConfiguration: &svcapitypes.OpenZFSCreateRootVolumeConfiguration{
				ReadOnly: aws.Bool(true),
			},
		},
		WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{
			Aliases: []*string{aws.String("fs.example.com")},
			SelfManagedActiveDirectoryConfiguration: &svcapitypes.SelfManagedActiveDirectoryConfiguration{
				DomainName: aws.String("desired.example.com"),
				Password:   newSecretRef("default", "ad-creds", "password"),
			},
		},
		OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{
			FsxAdminPassword: newSecretRef("default", "creds", "password"),
		},
	})

	// What the generated setters produce from a Describe response: create-only
	// members nil, but the AD attributes populated with the OBSERVED values.
	ko := &svcapitypes.FileSystem{
		Spec: svcapitypes.FileSystemSpec{
			LustreConfiguration:  &svcapitypes.CreateFileSystemLustreConfiguration{},
			OpenZFSConfiguration: &svcapitypes.CreateFileSystemOpenZFSConfiguration{},
			WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{
				SelfManagedActiveDirectoryConfiguration: &svcapitypes.SelfManagedActiveDirectoryConfiguration{
					DomainName: aws.String("observed.example.com"),
				},
			},
			OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{},
		},
	}

	restoreNonRoundTrippedSpecFields(desired, ko)

	// Nothing from LustreConfiguration may be restored: every member is
	// recovered with its observed value instead, so comparison stays honest.
	lc := ko.Spec.LustreConfiguration
	if lc.AutoImportPolicy != nil || lc.ExportPath != nil ||
		lc.ImportPath != nil || lc.ImportedFileChunkSize != nil {
		t.Errorf("LustreConfiguration members must not be restored, got %+v", lc)
	}
	if ko.Spec.OpenZFSConfiguration.RootVolumeConfiguration == nil {
		t.Error("RootVolumeConfiguration was not restored")
	}
	if len(ko.Spec.WindowsConfiguration.Aliases) != 1 {
		t.Error("Aliases was not restored")
	}

	smad := ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration
	// The write-only password must come across...
	if smad.Password == nil {
		t.Error("the self-managed AD password reference was not restored")
	}
	// ...but the observed AD values must survive, or edits to them are masked.
	if smad.DomainName == nil || *smad.DomainName != "observed.example.com" {
		t.Errorf(
			"DomainName = %v, want the observed value; restoring the whole AD block masks edits",
			smad.DomainName,
		)
	}
	if ko.Spec.OntapConfiguration.FsxAdminPassword == nil {
		t.Error("FsxAdminPassword secret reference was not restored")
	}
}

// TestRestoreLeavesMutableADFieldsComparable: an edit to a non-password AD field
// must still produce a delta after the restore hook has run.
func TestRestoreLeavesMutableADFieldsComparable(t *testing.T) {
	const path = "Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.DomainName"

	smad := func(domain string, ref *ackv1alpha1.SecretKeyReference) svcapitypes.FileSystemSpec {
		return svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("WINDOWS"),
			WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{
				SelfManagedActiveDirectoryConfiguration: &svcapitypes.SelfManagedActiveDirectoryConfiguration{
					DomainName: aws.String(domain),
					Password:   ref,
				},
			},
		}
	}
	ref := newSecretRef("default", "ad-creds", "password")

	// User wants a new domain; AWS still reports the old one.
	desired := newResource(smad("new.example.com", ref))
	latestKO := &svcapitypes.FileSystem{Spec: smad("old.example.com", nil)}

	restoreNonRoundTrippedSpecFields(desired, latestKO)

	delta := ackcompare.NewDelta()
	a := desired
	b := &resource{ko: latestKO}
	// Mirror what the generated comparison does for this scalar.
	if *a.ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.DomainName !=
		*b.ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.DomainName {
		delta.Add(path, nil, nil)
	}
	if !delta.DifferentAt(path) {
		t.Fatal("a non-password AD edit was masked by the restore hook")
	}
	// And the password reference still made it across.
	if latestKO.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password == nil {
		t.Error("the password reference was dropped")
	}
}

func TestRecoverObservedLustreDataRepository(t *testing.T) {
	const fsID = "fs-0123456789abcdef0"
	resp := func(drc *svcsdktypes.DataRepositoryConfiguration) *svcsdk.DescribeFileSystemsOutput {
		return &svcsdk.DescribeFileSystemsOutput{
			FileSystems: []svcsdktypes.FileSystem{{
				FileSystemId: aws.String(fsID),
				LustreConfiguration: &svcsdktypes.LustreFileSystemConfiguration{
					DataRepositoryConfiguration: drc,
				},
			}},
		}
	}
	newKO := func() *svcapitypes.FileSystem {
		return &svcapitypes.FileSystem{
			Spec: svcapitypes.FileSystemSpec{
				LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{},
			},
			Status: svcapitypes.FileSystemStatus{FileSystemID: aws.String(fsID)},
		}
	}

	t.Run("declared members pick up observed values", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
				AutoImportPolicy:      aws.String("NEW"),
				ImportPath:            aws.String("s3://bucket/declared-import"),
				ExportPath:            aws.String("s3://bucket/declared-export"),
				ImportedFileChunkSize: aws.Int64(1024),
			},
		})
		ko := newKO()
		recoverObservedLustreDataRepository(desired, resp(&svcsdktypes.DataRepositoryConfiguration{
			AutoImportPolicy:      svcsdktypes.AutoImportPolicyTypeNewChanged,
			ImportPath:            aws.String("s3://bucket/observed-import"),
			ExportPath:            aws.String("s3://bucket/observed-export"),
			ImportedFileChunkSize: aws.Int32(2048),
		}), ko)

		lc := ko.Spec.LustreConfiguration
		if lc.AutoImportPolicy == nil || *lc.AutoImportPolicy != "NEW_CHANGED" {
			t.Errorf("AutoImportPolicy = %v, want the observed NEW_CHANGED", lc.AutoImportPolicy)
		}
		if lc.ImportPath == nil || *lc.ImportPath != "s3://bucket/observed-import" {
			t.Errorf("ImportPath = %v, want the observed value", lc.ImportPath)
		}
		if lc.ExportPath == nil || *lc.ExportPath != "s3://bucket/observed-export" {
			t.Errorf("ExportPath = %v, want the observed value", lc.ExportPath)
		}
		if lc.ImportedFileChunkSize == nil || *lc.ImportedFileChunkSize != 2048 {
			t.Errorf("ImportedFileChunkSize = %v, want the observed 2048", lc.ImportedFileChunkSize)
		}
	})

	t.Run("undeclared members are left alone", func(t *testing.T) {
		// Writing AWS defaults into a Spec the user left empty would create a
		// delta and an UpdateFileSystem call nobody asked for.
		desired := newResource(svcapitypes.FileSystemSpec{
			LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{},
		})
		ko := newKO()
		recoverObservedLustreDataRepository(desired, resp(&svcsdktypes.DataRepositoryConfiguration{
			AutoImportPolicy:      svcsdktypes.AutoImportPolicyTypeNew,
			ImportPath:            aws.String("s3://bucket/observed"),
			ExportPath:            aws.String("s3://bucket/observed"),
			ImportedFileChunkSize: aws.Int32(2048),
		}), ko)
		lc := ko.Spec.LustreConfiguration
		if lc.AutoImportPolicy != nil || lc.ImportPath != nil ||
			lc.ExportPath != nil || lc.ImportedFileChunkSize != nil {
			t.Errorf("undeclared members were populated: %+v", lc)
		}
	})

	t.Run("nil safety and non-matching IDs", func(t *testing.T) {
		desired := newResource(svcapitypes.FileSystemSpec{
			LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
				ImportPath: aws.String("s3://bucket/declared"),
			},
		})
		ko := newKO()
		ko.Status.FileSystemID = aws.String("fs-other")
		recoverObservedLustreDataRepository(desired, resp(&svcsdktypes.DataRepositoryConfiguration{
			ImportPath: aws.String("s3://bucket/observed"),
		}), ko)
		if ko.Spec.LustreConfiguration.ImportPath != nil {
			t.Error("must not copy from a different file system")
		}
		recoverObservedLustreDataRepository(nil, nil, nil)
		recoverObservedLustreDataRepository(desired, nil, newKO())
		recoverObservedLustreDataRepository(desired, resp(nil), newKO())
		recoverObservedLustreDataRepository(desired, &svcsdk.DescribeFileSystemsOutput{}, newKO())
	})
}

// baselineFrom records the immutable paths declared by `spec` as the applied
// baseline, i.e. what a successful create would have persisted.
func baselineFrom(spec svcapitypes.FileSystemSpec) *resource {
	r := newResource(spec)
	setLastAppliedImmutableFields(r.ko)
	return r
}

func TestDeclaredImmutableFingerprints(t *testing.T) {
	if got := declaredImmutableFingerprints(nil); got != nil {
		t.Errorf("nil resource: got %v, want nil", got)
	}

	// An empty parent block must not make its children count as present.
	empty := newResource(svcapitypes.FileSystemSpec{
		LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{},
	})
	if got := declaredImmutableFingerprints(empty.ko); len(got) != 0 {
		t.Errorf("empty spec declared %v", got)
	}

	// Empty slices must not count, or every reconcile of a resource with no
	// security groups would look like a removal.
	emptySlices := newResource(svcapitypes.FileSystemSpec{
		SecurityGroupIDs:     []*string{},
		WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{Aliases: []*string{}},
	})
	if got := declaredImmutableFingerprints(emptySlices.ko); len(got) != 0 {
		t.Errorf("empty slices declared %v", got)
	}

	full := newResource(svcapitypes.FileSystemSpec{
		FileSystemType:   aws.String("LUSTRE"),
		SubnetIDs:        []*string{aws.String("subnet-1")},
		SecurityGroupIDs: []*string{aws.String("sg-1")},
		LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
			ImportPath: aws.String("s3://bucket/import"),
		},
	})
	got := declaredImmutableFingerprints(full.ko)
	for _, path := range []string{
		"Spec.FileSystemType", "Spec.SubnetIDs", "Spec.SecurityGroupIDs",
		"Spec.LustreConfiguration.ImportPath",
	} {
		if _, ok := got[path]; !ok {
			t.Errorf("%s missing from %v", path, got)
		}
	}
	if len(got) != 4 {
		t.Errorf("got %d fingerprints, want 4: %v", len(got), got)
	}

	// Reordering a slice is not a change: FSx does not preserve list order.
	a := newResource(svcapitypes.FileSystemSpec{
		SecurityGroupIDs: []*string{aws.String("sg-a"), aws.String("sg-b")},
	})
	b := newResource(svcapitypes.FileSystemSpec{
		SecurityGroupIDs: []*string{aws.String("sg-b"), aws.String("sg-a")},
	})
	if declaredImmutableFingerprints(a.ko)["Spec.SecurityGroupIDs"] !=
		declaredImmutableFingerprints(b.ko)["Spec.SecurityGroupIDs"] {
		t.Error("slice order must not affect the fingerprint")
	}
}

// TestRejectImmutableFieldChangesValueDrift covers fields DescribeFileSystems
// does not return. ResolveReferences writes resolved IDs into the Spec and
// sdkFind starts from that resolved object, so desired and latest always agree
// and the generated delta is empty however the value changes.
func TestRejectImmutableFieldChangesValueDrift(t *testing.T) {
	withSGs := func(ids ...string) svcapitypes.FileSystemSpec {
		refs := make([]*string, 0, len(ids))
		for _, id := range ids {
			refs = append(refs, aws.String(id))
		}
		return svcapitypes.FileSystemSpec{
			FileSystemType:   aws.String("LUSTRE"),
			SecurityGroupIDs: refs,
		}
	}

	t.Run("swapping sg-a for sg-b is rejected", func(t *testing.T) {
		// Both sides carry the NEW id and the field stays present, so neither
		// the delta nor a presence check sees anything.
		desired := newResource(withSGs("sg-b"))
		latest := baselineFrom(withSGs("sg-a"))
		latest.ko.Spec.SecurityGroupIDs = []*string{aws.String("sg-b")}

		err := rejectImmutableFieldChanges(ackcompare.NewDelta(), desired, latest)
		if err == nil {
			t.Fatal("a resolved security-group change must be rejected")
		}
		if !strings.Contains(err.Error(), "Spec.SecurityGroupIDs") {
			t.Errorf("error %q does not name the field", err.Error())
		}
		if !strings.Contains(err.Error(), "cannot be changed") {
			t.Errorf("error %q should report a change, not add/remove", err.Error())
		}
	})

	t.Run("an unchanged reference is allowed", func(t *testing.T) {
		if err := rejectImmutableFieldChanges(
			ackcompare.NewDelta(), newResource(withSGs("sg-a")), baselineFrom(withSGs("sg-a")),
		); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("adding a second group is rejected", func(t *testing.T) {
		if err := rejectImmutableFieldChanges(
			ackcompare.NewDelta(), newResource(withSGs("sg-a", "sg-b")),
			baselineFrom(withSGs("sg-a")),
		); err == nil {
			t.Error("growing the group list must be rejected")
		}
	})

	t.Run("reordering is not a change", func(t *testing.T) {
		if err := rejectImmutableFieldChanges(
			ackcompare.NewDelta(), newResource(withSGs("sg-b", "sg-a")),
			baselineFrom(withSGs("sg-a", "sg-b")),
		); err != nil {
			t.Errorf("a reorder must not be rejected: %v", err)
		}
	})
}

// TestRestoreLustreDataRepositoryFields covers the create path: the
// CreateFileSystem response nests these members under
// DataRepositoryConfiguration, so without the restore they would be erased from
// the CR and recorded as absent in the immutable baseline.
func TestRestoreLustreDataRepositoryFields(t *testing.T) {
	desired := newResource(svcapitypes.FileSystemSpec{
		FileSystemType: aws.String("LUSTRE"),
		LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
			AutoImportPolicy:      aws.String("NEW"),
			ExportPath:            aws.String("s3://bucket/export"),
			ImportPath:            aws.String("s3://bucket/import"),
			ImportedFileChunkSize: aws.Int64(2048),
		},
	})
	// What the generated create setters produce: the block exists, those four
	// members nil.
	ko := &svcapitypes.FileSystem{
		Spec: svcapitypes.FileSystemSpec{
			FileSystemType:      aws.String("LUSTRE"),
			LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{},
		},
	}

	restoreNonRoundTrippedSpecFields(desired, ko)
	restoreLustreDataRepositoryFields(desired, ko)
	setLastAppliedImmutableFields(ko)

	lc := ko.Spec.LustreConfiguration
	if lc.AutoImportPolicy == nil || *lc.AutoImportPolicy != "NEW" {
		t.Error("AutoImportPolicy did not survive create")
	}
	if lc.ExportPath == nil || *lc.ExportPath != "s3://bucket/export" {
		t.Error("ExportPath did not survive create")
	}
	if lc.ImportPath == nil || *lc.ImportPath != "s3://bucket/import" {
		t.Error("ImportPath did not survive create")
	}
	if lc.ImportedFileChunkSize == nil || *lc.ImportedFileChunkSize != 2048 {
		t.Error("ImportedFileChunkSize did not survive create")
	}

	// The baseline must record the three immutable ones as present, or the next
	// reconcile would read their absence as a removal -- or worse, accept a
	// later add.
	applied, recorded := appliedImmutableFields(ko)
	if !recorded {
		t.Fatal("no baseline recorded after create")
	}
	for _, path := range []string{
		"Spec.LustreConfiguration.ExportPath",
		"Spec.LustreConfiguration.ImportPath",
		"Spec.LustreConfiguration.ImportedFileChunkSize",
	} {
		if _, ok := applied[path]; !ok {
			t.Errorf("%s missing from the baseline: %v", path, applied)
		}
	}

	// And the post-create object must not then look changed to the guard.
	if err := rejectImmutableFieldChanges(
		ackcompare.NewDelta(), desired, &resource{ko: ko},
	); err != nil {
		t.Errorf("a freshly created resource must not be rejected: %v", err)
	}

	restoreLustreDataRepositoryFields(nil, nil)
	restoreLustreDataRepositoryFields(desired, &svcapitypes.FileSystem{})
}

// TestRejectImmutableFieldChangesPresence covers the transitions neither CEL nor
// the generated delta can see.
func TestRejectImmutableFieldChangesPresence(t *testing.T) {
	lustre := func(mutate func(*svcapitypes.CreateFileSystemLustreConfiguration)) svcapitypes.FileSystemSpec {
		lc := &svcapitypes.CreateFileSystemLustreConfiguration{}
		if mutate != nil {
			mutate(lc)
		}
		return svcapitypes.FileSystemSpec{
			FileSystemType:      aws.String("LUSTRE"),
			SubnetIDs:           []*string{aws.String("subnet-1")},
			LustreConfiguration: lc,
		}
	}

	tests := []struct {
		name       string
		applied    svcapitypes.FileSystemSpec
		desired    svcapitypes.FileSystemSpec
		wantErr    bool
		wantSubstr string
	}{
		{
			name:    "unchanged: no error",
			applied: lustre(func(lc *svcapitypes.CreateFileSystemLustreConfiguration) { lc.ImportPath = aws.String("s3://b/i") }),
			desired: lustre(func(lc *svcapitypes.CreateFileSystemLustreConfiguration) { lc.ImportPath = aws.String("s3://b/i") }),
		},
		{
			// absent -> present: CEL does not evaluate the transition rule.
			name:       "adding an unset create-only field is rejected",
			applied:    lustre(nil),
			desired:    lustre(func(lc *svcapitypes.CreateFileSystemLustreConfiguration) { lc.ImportPath = aws.String("s3://b/i") }),
			wantErr:    true,
			wantSubstr: "cannot be added after creation",
		},
		{
			// present -> absent: recovery is gated on the desired value being
			// non-nil, so this produces no delta and only the baseline sees it.
			name:       "removing a create-only field is rejected",
			applied:    lustre(func(lc *svcapitypes.CreateFileSystemLustreConfiguration) { lc.ImportPath = aws.String("s3://b/i") }),
			desired:    lustre(nil),
			wantErr:    true,
			wantSubstr: "cannot be removed after creation",
		},
		{
			// Removing the whole parent block took the same silent path.
			name:       "removing the entire LustreConfiguration is rejected",
			applied:    lustre(func(lc *svcapitypes.CreateFileSystemLustreConfiguration) { lc.ImportPath = aws.String("s3://b/i") }),
			desired:    svcapitypes.FileSystemSpec{FileSystemType: aws.String("LUSTRE"), SubnetIDs: []*string{aws.String("subnet-1")}},
			wantErr:    true,
			wantSubstr: "cannot be removed after creation",
		},
		{
			// Not round-trippable by Describe at all: no delta in either
			// direction, so only the recorded baseline catches it.
			name:    "adding securityGroupIDs is rejected",
			applied: svcapitypes.FileSystemSpec{FileSystemType: aws.String("LUSTRE")},
			desired: svcapitypes.FileSystemSpec{
				FileSystemType:   aws.String("LUSTRE"),
				SecurityGroupIDs: []*string{aws.String("sg-1")},
			},
			wantErr:    true,
			wantSubstr: "Spec.SecurityGroupIDs",
		},
		{
			name: "removing securityGroupIDs is rejected",
			applied: svcapitypes.FileSystemSpec{
				FileSystemType:   aws.String("LUSTRE"),
				SecurityGroupIDs: []*string{aws.String("sg-1")},
			},
			desired:    svcapitypes.FileSystemSpec{FileSystemType: aws.String("LUSTRE")},
			wantErr:    true,
			wantSubstr: "Spec.SecurityGroupIDs",
		},
		{
			name: "adding windows aliases is rejected",
			applied: svcapitypes.FileSystemSpec{
				FileSystemType:       aws.String("WINDOWS"),
				WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{},
			},
			desired: svcapitypes.FileSystemSpec{
				FileSystemType: aws.String("WINDOWS"),
				WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{
					Aliases: []*string{aws.String("fs.example.com")},
				},
			},
			wantErr:    true,
			wantSubstr: "Spec.WindowsConfiguration.Aliases",
		},
		{
			name: "removing openZFS rootVolumeConfiguration is rejected",
			applied: svcapitypes.FileSystemSpec{
				FileSystemType: aws.String("OPENZFS"),
				OpenZFSConfiguration: &svcapitypes.CreateFileSystemOpenZFSConfiguration{
					RootVolumeConfiguration: &svcapitypes.OpenZFSCreateRootVolumeConfiguration{
						ReadOnly: aws.Bool(true),
					},
				},
			},
			desired: svcapitypes.FileSystemSpec{
				FileSystemType:       aws.String("OPENZFS"),
				OpenZFSConfiguration: &svcapitypes.CreateFileSystemOpenZFSConfiguration{},
			},
			wantErr:    true,
			wantSubstr: "Spec.OpenZFSConfiguration.RootVolumeConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rejectImmutableFieldChanges(
				ackcompare.NewDelta(), newResource(tt.desired), baselineFrom(tt.applied),
			)
			if !tt.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("expected a terminal error")
			}
			var terminal *ackerr.TerminalError
			if !errors.As(err, &terminal) {
				t.Errorf("error is %T, want *ackerr.TerminalError", err)
			}
			if !strings.Contains(err.Error(), tt.wantSubstr) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantSubstr)
			}
		})
	}
}

// TestRejectImmutableFieldChangesValue covers observable fields, where the delta
// reports a change no update builder can apply.
func TestRejectImmutableFieldChangesValue(t *testing.T) {
	spec := svcapitypes.FileSystemSpec{
		FileSystemType: aws.String("LUSTRE"),
		KMSKeyID:       aws.String("arn:aws:kms:us-west-2:1234:key/abc"),
	}
	for _, path := range []string{
		"Spec.KMSKeyID",
		"Spec.FileSystemType",
		"Spec.LustreConfiguration.DeploymentType",
		"Spec.OntapConfiguration.PreferredSubnetID",
	} {
		t.Run(path, func(t *testing.T) {
			err := rejectImmutableFieldChanges(
				deltaAt(path), newResource(spec), baselineFrom(spec),
			)
			if err == nil {
				t.Fatalf("expected a terminal error for %s", path)
			}
			if !strings.Contains(err.Error(), "immutable") {
				t.Errorf("error %q should say the field is immutable", err.Error())
			}
		})
	}

	t.Run("a mutable path in the delta is allowed", func(t *testing.T) {
		if err := rejectImmutableFieldChanges(
			deltaAt("Spec.LustreConfiguration.WeeklyMaintenanceStartTime"),
			newResource(spec), baselineFrom(spec),
		); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestRejectImmutableFieldChangesNoBaseline(t *testing.T) {
	spec := svcapitypes.FileSystemSpec{
		FileSystemType:   aws.String("LUSTRE"),
		SecurityGroupIDs: []*string{aws.String("sg-1")},
	}
	// Adopted resource: no baseline recorded, so presence checks must not fire.
	latest := newResource(svcapitypes.FileSystemSpec{FileSystemType: aws.String("LUSTRE")})
	if err := rejectImmutableFieldChanges(
		ackcompare.NewDelta(), newResource(spec), latest,
	); err != nil {
		t.Errorf("adoption must not be rejected: %v", err)
	}

	// The read hook then records the baseline, after which a change is caught.
	ensureImmutableFieldBaseline(latest.ko)
	if latest.ko.Status.LastAppliedImmutableFields == nil {
		t.Fatal("baseline was not established")
	}
	if err := rejectImmutableFieldChanges(
		ackcompare.NewDelta(), newResource(spec), latest,
	); err == nil {
		t.Error("a presence change after the baseline was established was not caught")
	}

	// Re-running must not overwrite the baseline.
	before := *latest.ko.Status.LastAppliedImmutableFields
	latest.ko.Spec.SecurityGroupIDs = []*string{aws.String("sg-9")}
	ensureImmutableFieldBaseline(latest.ko)
	if *latest.ko.Status.LastAppliedImmutableFields != before {
		t.Error("a usable baseline must never be overwritten")
	}

	// Nil safety: runs on every reconcile.
	if err := rejectImmutableFieldChanges(nil, nil, nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if err := rejectImmutableFieldChanges(nil, newResource(spec), nil); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	ensureImmutableFieldBaseline(nil)
	setLastAppliedImmutableFields(nil)
}

// TestRejectImmutableFieldChangesEmptyBaseline pins that a recorded empty set is
// distinguishable from "never recorded": a file system declaring no optional
// immutable fields records `[]`, and if that read as absent the first add would
// slip through.
func TestRejectImmutableFieldChangesEmptyBaseline(t *testing.T) {
	appliedNothing := baselineFrom(svcapitypes.FileSystemSpec{})
	if appliedNothing.ko.Status.LastAppliedImmutableFields == nil {
		t.Fatal("an empty declared set must still be recorded")
	}
	if got := *appliedNothing.ko.Status.LastAppliedImmutableFields; got != "{}" {
		t.Errorf("baseline = %q, want {}", got)
	}
	if _, recorded := appliedImmutableFields(appliedNothing.ko); !recorded {
		t.Error("a recorded empty map must report as recorded")
	}

	desired := newResource(svcapitypes.FileSystemSpec{
		SecurityGroupIDs: []*string{aws.String("sg-1")},
	})
	err := rejectImmutableFieldChanges(ackcompare.NewDelta(), desired, appliedNothing)
	if err == nil {
		t.Fatal("adding a field against an empty baseline must be rejected")
	}
	if !strings.Contains(err.Error(), "Spec.SecurityGroupIDs") {
		t.Errorf("error %q does not name the field", err.Error())
	}
}

// TestImmutableBaselineRepairsCorruption: a malformed baseline must be
// re-recorded rather than permanently disabling the presence and value checks.
func TestImmutableBaselineRepairsCorruption(t *testing.T) {
	spec := svcapitypes.FileSystemSpec{
		FileSystemType:   aws.String("LUSTRE"),
		SecurityGroupIDs: []*string{aws.String("sg-a")},
	}
	corrupt := newResource(spec)
	corrupt.ko.Status.LastAppliedImmutableFields = aws.String("not json")

	if _, recorded := appliedImmutableFields(corrupt.ko); recorded {
		t.Error("a malformed baseline must report as not recorded")
	}
	// Must not wedge the resource while corrupt.
	if err := rejectImmutableFieldChanges(
		ackcompare.NewDelta(), newResource(spec), corrupt,
	); err != nil {
		t.Errorf("a corrupt baseline must not reject: %v", err)
	}

	// The read hook must repair it -- keyed off the parse result, not merely a
	// non-nil pointer, or the corruption would persist for the resource's life.
	ensureImmutableFieldBaseline(corrupt.ko)
	repaired, recorded := appliedImmutableFields(corrupt.ko)
	if !recorded {
		t.Fatal("a malformed baseline was not repaired")
	}
	if _, ok := repaired["Spec.SecurityGroupIDs"]; !ok {
		t.Errorf("repaired baseline is missing the declared field: %v", repaired)
	}

	// Once repaired, a change is caught again.
	changed := newResource(svcapitypes.FileSystemSpec{
		FileSystemType:   aws.String("LUSTRE"),
		SecurityGroupIDs: []*string{aws.String("sg-b")},
	})
	if err := rejectImmutableFieldChanges(
		ackcompare.NewDelta(), changed, corrupt,
	); err == nil {
		t.Error("a change after repair was not caught")
	}

	// A usable baseline is still never overwritten.
	before := *corrupt.ko.Status.LastAppliedImmutableFields
	corrupt.ko.Spec.SecurityGroupIDs = []*string{aws.String("sg-z")}
	ensureImmutableFieldBaseline(corrupt.ko)
	if *corrupt.ko.Status.LastAppliedImmutableFields != before {
		t.Error("a usable baseline must never be overwritten")
	}
}

// TestRejectImmutableFieldChangesSurfacesFromUpdate: the guard must run before
// any AWS call, so the reconcile reports Terminal rather than a silent no-op.
func TestRejectImmutableFieldChangesSurfacesFromUpdate(t *testing.T) {
	rm := &resourceManager{}
	applied := svcapitypes.FileSystemSpec{
		FileSystemType:      aws.String("LUSTRE"),
		LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{},
	}
	desired := newResource(svcapitypes.FileSystemSpec{
		FileSystemType: aws.String("LUSTRE"),
		LustreConfiguration: &svcapitypes.CreateFileSystemLustreConfiguration{
			ImportPath: aws.String("s3://bucket/added-after-create"),
		},
	})
	_, err := rm.customUpdateFileSystem(
		context.Background(), desired, baselineFrom(applied), ackcompare.NewDelta(),
	)
	var terminal *ackerr.TerminalError
	if !errors.As(err, &terminal) {
		t.Fatalf("error is %T, want *ackerr.TerminalError: %v", err, err)
	}
}

// TestEnsureLastAppliedSecretBaselineAdoptThenRotate is the adopt/reconcile then
// change-reference/update sequence, now through Status rather than annotations.
func TestEnsureLastAppliedSecretBaselineAdoptThenRotate(t *testing.T) {
	spec := func(name string) svcapitypes.FileSystemSpec {
		return svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("ONTAP"),
			OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{
				FsxAdminPassword: newSecretRef("default", name, "password"),
			},
		}
	}
	const path = "Spec.OntapConfiguration.FsxAdminPassword"

	// Adopted: no baseline. Adoption must not reset an untouched password.
	adopted := newResource(spec("creds"))
	delta := ackcompare.NewDelta()
	customPreCompare(delta, adopted, adopted)
	if delta.DifferentAt(path) {
		t.Fatal("adoption must not trigger a password update")
	}

	// The read hook establishes the baseline in Status, which the reconciler
	// persists via patchResourceStatus on every reconcile.
	ensureLastAppliedSecretBaseline(adopted.ko)
	want := secretReferenceString(newSecretRef("default", "creds", "password"))
	if adopted.ko.Status.LastAppliedFsxAdminPasswordRef == nil ||
		*adopted.ko.Status.LastAppliedFsxAdminPasswordRef != want {
		t.Fatalf("baseline = %v, want %s", adopted.ko.Status.LastAppliedFsxAdminPasswordRef, want)
	}

	delta = ackcompare.NewDelta()
	customPreCompare(delta, adopted, adopted)
	if delta.DifferentAt(path) {
		t.Fatal("establishing the baseline must not itself trigger an update")
	}

	// User points at a different Secret: detected against the persisted baseline.
	rotated := newResource(spec("creds-v2"))
	latest := newResource(spec("creds-v2"))
	latest.ko.Status = *adopted.ko.Status.DeepCopy()
	delta = ackcompare.NewDelta()
	customPreCompare(delta, rotated, latest)
	if !delta.DifferentAt(path) {
		t.Fatal("a reference change after the baseline was established was not detected")
	}

	// Re-running the helper must not clobber an existing baseline.
	ensureLastAppliedSecretBaseline(latest.ko)
	if *latest.ko.Status.LastAppliedFsxAdminPasswordRef != want {
		t.Error("an existing baseline must never be overwritten on read")
	}
	delta = ackcompare.NewDelta()
	customPreCompare(delta, rotated, latest)
	if !delta.DifferentAt(path) {
		t.Error("rotation was masked by re-running the baseline helper")
	}

	// An empty-string baseline (no reference at create) still counts as recorded.
	noRef := newResource(svcapitypes.FileSystemSpec{FileSystemType: aws.String("ONTAP")})
	ensureLastAppliedSecretBaseline(noRef.ko)
	if noRef.ko.Status.LastAppliedFsxAdminPasswordRef == nil ||
		*noRef.ko.Status.LastAppliedFsxAdminPasswordRef != "" {
		t.Error("a resource with no reference should record an empty baseline")
	}

	ensureLastAppliedSecretBaseline(nil) // must not panic
}

func TestSetLastAppliedSecretRefs(t *testing.T) {
	ko := &svcapitypes.FileSystem{
		Spec: svcapitypes.FileSystemSpec{
			OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{
				FsxAdminPassword: newSecretRef("default", "creds", "password"),
			},
		},
	}
	setLastAppliedSecretRefs(ko)

	want := secretReferenceString(newSecretRef("default", "creds", "password"))
	if ko.Status.LastAppliedFsxAdminPasswordRef == nil ||
		*ko.Status.LastAppliedFsxAdminPasswordRef != want {
		t.Errorf("baseline = %v, want %s", ko.Status.LastAppliedFsxAdminPasswordRef, want)
	}
	// Recorded as empty rather than nil, so customPreCompare sees "recorded"
	// and does not skip the comparison.
	if ko.Status.LastAppliedSelfManagedADPasswordRef == nil {
		t.Error("the unused reference should still be recorded as empty")
	}

	// Round trip: after recording, the same spec shows no rotation.
	r := &resource{ko: ko}
	delta := ackcompare.NewDelta()
	customPreCompare(delta, r, r)
	if delta.DifferentAt("Spec.OntapConfiguration.FsxAdminPassword") {
		t.Error("recording then comparing the same reference must not diff")
	}

	setLastAppliedSecretRefs(nil) // must not panic
}

func newSecretRef(ns, name, key string) *ackv1alpha1.SecretKeyReference {
	return &ackv1alpha1.SecretKeyReference{
		Key:             key,
		SecretReference: corev1.SecretReference{Namespace: ns, Name: name},
	}
}

func ontapSpec(ref *ackv1alpha1.SecretKeyReference) svcapitypes.FileSystemSpec {
	return svcapitypes.FileSystemSpec{
		FileSystemType: aws.String("ONTAP"),
		OntapConfiguration: &svcapitypes.CreateFileSystemOntapConfiguration{
			FsxAdminPassword: ref,
		},
	}
}

func TestCustomPreCompareOntapPasswordRotation(t *testing.T) {
	const path = "Spec.OntapConfiguration.FsxAdminPassword"
	applied := secretReferenceString(newSecretRef("default", "creds", "password"))

	tests := []struct {
		name      string
		ref       *ackv1alpha1.SecretKeyReference
		baseline  *string
		wantDelta bool
	}{
		{"same reference as applied", newSecretRef("default", "creds", "password"), aws.String(applied), false},
		{"different Secret name", newSecretRef("default", "creds-v2", "password"), aws.String(applied), true},
		{"different key, same Secret", newSecretRef("default", "creds", "password-v2"), aws.String(applied), true},
		{"different namespace", newSecretRef("other", "creds", "password"), aws.String(applied), true},
		// Adopted or first reconcile: must not reset an untouched password.
		{"no baseline recorded", newSecretRef("default", "creds", "password"), nil, false},
		{"no reference at all", nil, aws.String(applied), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := newResource(ontapSpec(tt.ref))
			b := newResource(ontapSpec(tt.ref))
			b.ko.Status.LastAppliedFsxAdminPasswordRef = tt.baseline
			delta := ackcompare.NewDelta()
			customPreCompare(delta, a, b)
			if got := delta.DifferentAt(path); got != tt.wantDelta {
				t.Errorf("DifferentAt(%s) = %v, want %v", path, got, tt.wantDelta)
			}
		})
	}
}

// TestSecretReferenceStringIsInjective: dots are legal in both Secret names and
// keys, so a delimited "<ns>/<name>.<key>" encoding collides and a rotation
// between two such references reads as no change.
func TestSecretReferenceStringIsInjective(t *testing.T) {
	a := newSecretRef("default", "creds", "admin.password")
	b := newSecretRef("default", "creds.admin", "password")
	if secretReferenceString(a) == secretReferenceString(b) {
		t.Fatalf("collision: both encode to %s", secretReferenceString(a))
	}
	if secretReferenceString(nil) != "" {
		t.Error("nil reference must encode to the empty string")
	}

	// The rotation between them must now be detected.
	desired := newResource(ontapSpec(b))
	latest := newResource(ontapSpec(b))
	latest.ko.Status.LastAppliedFsxAdminPasswordRef = aws.String(secretReferenceString(a))
	delta := ackcompare.NewDelta()
	customPreCompare(delta, desired, latest)
	if !delta.DifferentAt("Spec.OntapConfiguration.FsxAdminPassword") {
		t.Error("rotation between the previously-colliding references was not detected")
	}
}

// TestCustomPreCompareRotationReachesUpdatePayload proves the injected delta
// opens the parent-gated builder block, not just that a delta entry exists.
func TestCustomPreCompareRotationReachesUpdatePayload(t *testing.T) {
	desired := newResource(ontapSpec(newSecretRef("default", "creds-v2", "password")))
	latest := newResource(ontapSpec(newSecretRef("default", "creds-v2", "password")))
	latest.ko.Status.LastAppliedFsxAdminPasswordRef = aws.String(
		secretReferenceString(newSecretRef("default", "creds", "password")),
	)

	delta := ackcompare.NewDelta()
	customPreCompare(delta, desired, latest)
	if !delta.DifferentAt("Spec.OntapConfiguration.FsxAdminPassword") {
		t.Fatal("expected a rotation delta")
	}
	// DifferentAt matches ancestors, which is what makes the ONTAP builder's
	// parent-path gate open and the password actually reach the request.
	if !delta.DifferentAt("Spec.OntapConfiguration") {
		t.Error("parent path should also report different, or the builder never runs")
	}
}

func TestCustomPreCompareWindowsPasswordRotation(t *testing.T) {
	const path = "Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password"
	mk := func(ref *ackv1alpha1.SecretKeyReference) *resource {
		return newResource(svcapitypes.FileSystemSpec{
			FileSystemType: aws.String("WINDOWS"),
			WindowsConfiguration: &svcapitypes.CreateFileSystemWindowsConfiguration{
				SelfManagedActiveDirectoryConfiguration: &svcapitypes.SelfManagedActiveDirectoryConfiguration{
					Password: ref,
				},
			},
		})
	}

	a := mk(newSecretRef("default", "ad-creds-v2", "password"))
	b := mk(newSecretRef("default", "ad-creds-v2", "password"))
	b.ko.Status.LastAppliedSelfManagedADPasswordRef = aws.String(
		secretReferenceString(newSecretRef("default", "ad-creds", "password")),
	)
	delta := ackcompare.NewDelta()
	customPreCompare(delta, a, b)
	if !delta.DifferentAt(path) {
		t.Error("rotation of the self-managed AD password was not detected")
	}
	// The Windows builder gates on the parent path.
	if !delta.DifferentAt("Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration") {
		t.Error("parent path should also report different, or the builder never runs")
	}

	unchanged := mk(newSecretRef("default", "ad-creds", "password"))
	unchanged.ko.Status.LastAppliedSelfManagedADPasswordRef = aws.String(
		secretReferenceString(newSecretRef("default", "ad-creds", "password")),
	)
	delta2 := ackcompare.NewDelta()
	customPreCompare(delta2, unchanged, unchanged)
	if delta2.DifferentAt(path) {
		t.Error("an unchanged reference must not produce a delta")
	}
}

// TestCustomPreCompareNilSafety: customPreCompare runs on every reconcile.
func TestCustomPreCompareNilSafety(t *testing.T) {
	delta := ackcompare.NewDelta()
	customPreCompare(delta, nil, nil)
	customPreCompare(delta, newResource(svcapitypes.FileSystemSpec{}), nil)
	customPreCompare(delta, nil, newResource(svcapitypes.FileSystemSpec{}))
	if len(delta.Differences) != 0 {
		t.Errorf("expected no differences, got %d", len(delta.Differences))
	}
}

// TestSetDeleteFileSystemConfigurationONTAP pins the fact that ONTAP is a
// deliberate no-op: the FSx API has no DeleteFileSystemOntapConfiguration
// shape, so there is nowhere to put SkipFinalBackup.
func TestSetDeleteFileSystemConfigurationONTAP(t *testing.T) {
	r := newAnnotatedResource(aws.String("ONTAP"), skipFinalBackupAnnotation("false"))
	input := &svcsdk.DeleteFileSystemInput{}

	if err := setDeleteFileSystemConfiguration(r, input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if input.LustreConfiguration != nil ||
		input.WindowsConfiguration != nil ||
		input.OpenZFSConfiguration != nil {
		t.Error("no per-type config should be set for ONTAP")
	}
}

// TestSetDeleteFileSystemConfigurationNilType covers a FileSystem whose Spec
// never got a type, e.g. a delete arriving before the create ever succeeded.
// Dereferencing FileSystemType there would panic on the finalizer path.
func TestSetDeleteFileSystemConfigurationNilType(t *testing.T) {
	r := newAnnotatedResource(nil, skipFinalBackupAnnotation("false"))
	input := &svcsdk.DeleteFileSystemInput{}

	if err := setDeleteFileSystemConfiguration(r, input); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if input.LustreConfiguration != nil ||
		input.WindowsConfiguration != nil ||
		input.OpenZFSConfiguration != nil {
		t.Error("no per-type config should be set when FileSystemType is nil")
	}
}

// TestSetDeleteFileSystemConfigurationBadAnnotation asserts a malformed
// annotation is terminal. A plain error would requeue forever, and silently
// defaulting would destroy a final backup the user explicitly asked for.
func TestSetDeleteFileSystemConfigurationBadAnnotation(t *testing.T) {
	r := newAnnotatedResource(aws.String("WINDOWS"), skipFinalBackupAnnotation("nope"))
	input := &svcsdk.DeleteFileSystemInput{}

	err := setDeleteFileSystemConfiguration(r, input)
	if err == nil {
		t.Fatal("expected an error for an unparseable annotation value")
	}

	var terminal *ackerr.TerminalError
	if !errors.As(err, &terminal) {
		t.Errorf("error is %T, want *ackerr.TerminalError", err)
	}
	// The message has to name the annotation, since it is the only thing the
	// user can act on from the resource's condition.
	if !strings.Contains(err.Error(), svcapitypes.SkipFinalBackupAnnotation) {
		t.Errorf("error %q does not name the offending annotation", err.Error())
	}
	if input.WindowsConfiguration != nil {
		t.Error("no config should be built when the annotation is invalid")
	}
}

func TestIsDeleting(t *testing.T) {
	tests := []struct {
		name      string
		lifecycle *string
		want      bool
	}{
		{"nil lifecycle", nil, false},
		{"available", aws.String("AVAILABLE"), false},
		{"creating", aws.String("CREATING"), false},
		{"deleting", aws.String("DELETING"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newResource(svcapitypes.FileSystemSpec{})
			r.ko.Status.Lifecycle = tt.lifecycle
			if got := isDeleting(r); got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
	if isDeleting(nil) {
		t.Fatal("isDeleting(nil) must be false")
	}
}
