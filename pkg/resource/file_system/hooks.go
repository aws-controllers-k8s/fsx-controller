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
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/fsx-controller/pkg/util"
)

// restoreNonRoundTrippedSpecFields copies Spec members DescribeFileSystems
// cannot round-trip back onto the resource built from an FSx response.
//
// sdkCreate and sdkFind assign ko.Spec.<Type>Configuration wholesale from the
// output shape, so members that shape omits are lost and the reconciler then
// erases them from the CR.
//
// Only write-only or create-only-and-unobservable members are restored.
// Anything Describe does report is left as the observed value: overwriting it
// with the desired value would make a user edit compare equal to AWS and
// suppress the update.
func restoreNonRoundTrippedSpecFields(desired *resource, ko *svcapitypes.FileSystem) {
	if desired == nil || desired.ko == nil || ko == nil {
		return
	}
	src := desired.ko.Spec

	// Nothing from LustreConfiguration is restored: every member Describe fails
	// to name-match is recovered with its OBSERVED value by
	// recoverObservedLustreDataRepository instead, so comparison stays honest.
	// Create-only: the output shape carries RootVolumeId instead.
	if src.OpenZFSConfiguration != nil && ko.Spec.OpenZFSConfiguration != nil {
		ko.Spec.OpenZFSConfiguration.RootVolumeConfiguration = src.OpenZFSConfiguration.RootVolumeConfiguration
	}

	if src.WindowsConfiguration != nil && ko.Spec.WindowsConfiguration != nil {
		// Aliases are not restored here: recoverObservedWindowsAliases supplies
		// the observed names on reads, and restoreWindowsAliases covers create.

		// Only the write-only password. The rest of the AD block is reported by
		// Describe as SelfManagedActiveDirectoryAttributes and is updatable, so
		// replacing the whole struct would mask every edit to those fields.
		//
		// If Describe returned no AD block the reference is left out rather than
		// fabricating a configuration the user never submitted.
		if src.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration != nil &&
			ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration != nil {
			ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password =
				src.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password
		}
	}

	// Write-only: never returned by Describe.
	if src.OntapConfiguration != nil && ko.Spec.OntapConfiguration != nil {
		ko.Spec.OntapConfiguration.FsxAdminPassword = src.OntapConfiguration.FsxAdminPassword
	}
}

// observedFileSystem returns the entry in resp describing the file system in
// ko, or nil.
func observedFileSystem(
	resp *svcsdk.DescribeFileSystemsOutput,
	ko *svcapitypes.FileSystem,
) *svcsdktypes.FileSystem {
	if resp == nil || ko == nil || ko.Status.ID == nil {
		return nil
	}
	for i := range resp.FileSystems {
		fs := &resp.FileSystems[i]
		if fs.FileSystemId != nil && *fs.FileSystemId == *ko.Status.ID {
			return fs
		}
	}
	return nil
}

// recoverObservedWindowsAliases sets Spec.WindowsConfiguration.Aliases to the
// alias names FSx reports, so an edit or external change shows up in the delta.
//
// generator.yaml drops the output member because Describe returns Alias structs
// ({Name, Lifecycle}) while the Spec takes plain DNS names. DNS names are
// case-insensitive and Describe's ordering is not documented, so when the
// observed set matches the desired one the desired list is kept as written;
// otherwise a cosmetic difference would read as an immutable-field edit.
func recoverObservedWindowsAliases(
	desired *resource,
	resp *svcsdk.DescribeFileSystemsOutput,
	ko *svcapitypes.FileSystem,
) {
	if ko == nil || ko.Spec.WindowsConfiguration == nil {
		return
	}
	fs := observedFileSystem(resp, ko)
	if fs == nil || fs.WindowsConfiguration == nil {
		return
	}
	var observed []*string
	for _, a := range fs.WindowsConfiguration.Aliases {
		if a.Name != nil {
			observed = append(observed, aws.String(*a.Name))
		}
	}
	var declared []*string
	if desired != nil && desired.ko != nil && desired.ko.Spec.WindowsConfiguration != nil {
		declared = desired.ko.Spec.WindowsConfiguration.Aliases
	}
	if sameDNSNames(declared, observed) {
		ko.Spec.WindowsConfiguration.Aliases = declared
		return
	}
	ko.Spec.WindowsConfiguration.Aliases = observed
}

// restoreWindowsAliases copies the submitted aliases onto the resource built
// from the CreateFileSystem response, which carries them only as Alias structs.
// Create path only; reads use recoverObservedWindowsAliases.
func restoreWindowsAliases(desired *resource, ko *svcapitypes.FileSystem) {
	if desired == nil || desired.ko == nil || ko == nil ||
		desired.ko.Spec.WindowsConfiguration == nil || ko.Spec.WindowsConfiguration == nil {
		return
	}
	ko.Spec.WindowsConfiguration.Aliases = desired.ko.Spec.WindowsConfiguration.Aliases
}

// sameDNSNames reports whether a and b hold the same names, ignoring order and
// case.
func sameDNSNames(a, b []*string) bool {
	if len(a) != len(b) {
		return false
	}
	count := make(map[string]int, len(a))
	for _, s := range a {
		count[strings.ToLower(aws.ToString(s))]++
	}
	for _, s := range b {
		k := strings.ToLower(aws.ToString(s))
		if count[k] == 0 {
			return false
		}
		count[k]--
	}
	return true
}

// observedLustreDataRepository returns the DataRepositoryConfiguration FSx
// reports for the file system in ko, or nil.
//
// The Spec is generated from CreateFileSystemLustreConfiguration, which carries
// these members flat; Describe nests them under
// LustreFileSystemConfiguration.DataRepositoryConfiguration, which code-gen
// cannot name-match, so sdkFind leaves them nil.
func observedLustreDataRepository(
	resp *svcsdk.DescribeFileSystemsOutput,
	ko *svcapitypes.FileSystem,
) *svcsdktypes.DataRepositoryConfiguration {
	fs := observedFileSystem(resp, ko)
	if fs == nil || fs.LustreConfiguration == nil {
		return nil
	}
	return fs.LustreConfiguration.DataRepositoryConfiguration
}

// restoreLustreDataRepositoryFields copies the four Lustre data-repository
// members from the submitted resource. Used on the create path only.
//
// The CreateFileSystem response carries LustreFileSystemConfiguration, which
// nests these under DataRepositoryConfiguration and so cannot be name-matched
// into the flat Spec members. Without this they would be nil in the object the
// reconciler patches back, erasing the user's declared values from the CR. On
// reads
// recoverObservedLustreDataRepository supplies the observed values instead, so
// comparison stays honest.
func restoreLustreDataRepositoryFields(desired *resource, ko *svcapitypes.FileSystem) {
	if desired == nil || desired.ko == nil || ko == nil {
		return
	}
	src := desired.ko.Spec.LustreConfiguration
	if src == nil || ko.Spec.LustreConfiguration == nil {
		return
	}
	ko.Spec.LustreConfiguration.AutoImportPolicy = src.AutoImportPolicy
	ko.Spec.LustreConfiguration.ExportPath = src.ExportPath
	ko.Spec.LustreConfiguration.ImportPath = src.ImportPath
	ko.Spec.LustreConfiguration.ImportedFileChunkSize = src.ImportedFileChunkSize
}

// recoverObservedLustreDataRepository copies the observed AutoImportPolicy,
// ImportPath, ExportPath and ImportedFileChunkSize into the Spec so the
// generated comparison sees real AWS state.
//
// Recovered only where the user declared the member: writing an AWS-chosen
// default into a Spec left empty would manufacture a delta and an
// UpdateFileSystem call nobody asked for.
func recoverObservedLustreDataRepository(
	desired *resource,
	resp *svcsdk.DescribeFileSystemsOutput,
	ko *svcapitypes.FileSystem,
) {
	if desired == nil || desired.ko == nil || ko == nil {
		return
	}
	src := desired.ko.Spec.LustreConfiguration
	if src == nil || ko.Spec.LustreConfiguration == nil {
		return
	}
	drc := observedLustreDataRepository(resp, ko)
	if drc == nil {
		return
	}
	if src.AutoImportPolicy != nil && drc.AutoImportPolicy != "" {
		ko.Spec.LustreConfiguration.AutoImportPolicy = aws.String(string(drc.AutoImportPolicy))
	}
	if src.ImportPath != nil && drc.ImportPath != nil {
		ko.Spec.LustreConfiguration.ImportPath = drc.ImportPath
	}
	if src.ExportPath != nil && drc.ExportPath != nil {
		ko.Spec.LustreConfiguration.ExportPath = drc.ExportPath
	}
	if src.ImportedFileChunkSize != nil && drc.ImportedFileChunkSize != nil {
		ko.Spec.LustreConfiguration.ImportedFileChunkSize = aws.Int64(
			int64(*drc.ImportedFileChunkSize),
		)
	}
}

// fsxAdminPasswordRef and selfManagedADPasswordRef read the two secret-backed
// Spec fields, tolerating absent parent configuration blocks.
func fsxAdminPasswordRef(ko *svcapitypes.FileSystem) *ackv1alpha1.SecretKeyReference {
	if ko == nil || ko.Spec.OntapConfiguration == nil {
		return nil
	}
	return ko.Spec.OntapConfiguration.FsxAdminPassword
}

func selfManagedADPasswordRef(ko *svcapitypes.FileSystem) *ackv1alpha1.SecretKeyReference {
	if ko == nil || ko.Spec.WindowsConfiguration == nil ||
		ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration == nil {
		return nil
	}
	return ko.Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password
}

// pathString renders a delta path. compare.Path keeps its parts unexported and
// exposes them only through MarshalJSON, so that is the supported way to read
// one back out.
func pathString(p ackcompare.Path) string {
	encoded, err := p.MarshalJSON()
	if err != nil {
		return ""
	}
	var parts struct{ Parts []string }
	if err := json.Unmarshal(encoded, &parts); err != nil {
		return ""
	}
	return strings.Join(parts.Parts, ".")
}

// sizingModeUserProvisioned is the read-cache and metadata mode under which the
// companion size/IOPS member must be supplied; every other mode sizes itself and
// requires that member to be omitted.
const sizingModeUserProvisioned = "USER_PROVISIONED"

// specToRequestMember maps a Spec member name onto its UpdateFileSystem
// counterpart wherever the SDK spells it differently. Unlisted members keep
// their Spec name.
var specToRequestMember = map[string]string{
	"DNSIPs":                   "DnsIps",
	"DiskIOPSConfiguration":    "DiskIopsConfiguration",
	"EndpointIPv6AddressRange": "EndpointIpv6AddressRange",
	"IOPS":                     "Iops",
}

// requestCarriesPath reports whether the built request actually carries a value
// for the supplied Spec path.
//
// That is the question that decides whether an update converges.
// UpdateFileSystem overwrites only the members a request sets, so a change the
// request does not carry is silently discarded and the same delta returns on
// every reconcile. Asking the request rather than asking whether a payload was
// produced at all is what closes the ancestor-match gap: DifferentAt matches
// ancestors, so a removed leaf still yields a non-nil enclosing struct rebuilt
// from its surviving siblings.
//
// known is false when the path has no counterpart in the request shapes, which
// the caller leaves to the payload probe rather than guessing about.
func requestCarriesPath(
	input *svcsdk.UpdateFileSystemInput,
	specPath string,
) (carried bool, known bool) {
	if input == nil {
		return false, false
	}
	parts := strings.Split(specPath, ".")
	if len(parts) < 2 || parts[0] != "Spec" {
		return false, false
	}
	parts = parts[1:]

	// RouteTableIDs has no single counterpart: Create takes one list while
	// Update takes AddRouteTableIds/RemoveRouteTableIds. Either one carries the
	// change, which is how removing every route table stays expressible.
	if len(parts) == 2 && parts[1] == "RouteTableIDs" {
		cfg := reflect.ValueOf(input).Elem().FieldByName(parts[0])
		if !cfg.IsValid() {
			return false, false
		}
		if cfg.IsNil() {
			return false, true
		}
		cfg = cfg.Elem()
		for _, name := range []string{"AddRouteTableIds", "RemoveRouteTableIds"} {
			if f := cfg.FieldByName(name); f.IsValid() && f.Len() > 0 {
				return true, true
			}
		}
		return false, true
	}

	typ := reflect.TypeOf(input).Elem()
	val := reflect.ValueOf(input).Elem()
	// Optional SDK members are pointers; enums and lists are not. Which of the
	// two the last hop was decides how presence reads: a non-nil pointer counts
	// even when it addresses a zero value, because 0 is a meaningful
	// AutomaticBackupRetentionDays, while an enum counts only when non-empty.
	// Lists are judged on nil-ness rather than length -- see below.
	viaPointer := false
	for _, part := range parts {
		if typ.Kind() != reflect.Struct {
			return false, false
		}
		name := part
		if mapped, ok := specToRequestMember[part]; ok {
			name = mapped
		}
		field, ok := typ.FieldByName(name)
		if !ok {
			return false, false
		}
		typ = field.Type
		if val.IsValid() {
			val = val.FieldByName(name)
		}
		viaPointer = typ.Kind() == reflect.Ptr
		if viaPointer {
			typ = typ.Elem()
			if val.IsValid() {
				if val.IsNil() {
					val = reflect.Value{}
				} else {
					val = val.Elem()
				}
			}
		}
	}
	if !val.IsValid() {
		return false, true
	}
	if viaPointer {
		return true, true
	}
	if typ.Kind() == reflect.Slice {
		// An explicitly empty list is a value, not an absence: the smithy
		// serializers gate on `v == nil`, so a non-nil empty slice is written as
		// an explicit []. FSx relies on that -- the Lustre guide documents
		// NoSquashNids=[] as the way to remove every root-squash client
		// exception. Judging by length would reject that operation.
		return !val.IsNil(), true
	}
	return !val.IsZero(), true
}

// absenceExpressible reports whether an absent desired value is itself how the
// API expresses the change, rather than a removal UpdateFileSystem cannot make.
//
// Each case is a sibling mode member whose non-USER_PROVISIONED value requires
// its size/IOPS partner to be omitted, so omitting it is exactly the request FSx
// wants:
//
//   - Lustre MetadataConfiguration -- switching Mode away from USER_PROVISIONED
//     must not send Iops.
//   - Lustre DataReadCacheConfiguration and OpenZFS ReadCacheConfiguration --
//     SizeGiB is required only while SizingMode is USER_PROVISIONED.
//
// Removing route table IDs is deliberately not listed: RemoveRouteTableIds
// carries it, so requestCarriesPath already sees the change in the request.
func absenceExpressible(specPath string, desired *resource) bool {
	if desired == nil || desired.ko == nil {
		return false
	}
	spec := desired.ko.Spec
	switch specPath {
	case "Spec.LustreConfiguration.MetadataConfiguration.IOPS":
		if spec.LustreConfiguration == nil ||
			spec.LustreConfiguration.MetadataConfiguration == nil {
			return false
		}
		return modeSetOtherThan(spec.LustreConfiguration.MetadataConfiguration.Mode)
	case "Spec.LustreConfiguration.DataReadCacheConfiguration.SizeGiB":
		if spec.LustreConfiguration == nil ||
			spec.LustreConfiguration.DataReadCacheConfiguration == nil {
			return false
		}
		return modeSetOtherThan(spec.LustreConfiguration.DataReadCacheConfiguration.SizingMode)
	case "Spec.OpenZFSConfiguration.ReadCacheConfiguration.SizeGiB":
		if spec.OpenZFSConfiguration == nil ||
			spec.OpenZFSConfiguration.ReadCacheConfiguration == nil {
			return false
		}
		return modeSetOtherThan(spec.OpenZFSConfiguration.ReadCacheConfiguration.SizingMode)
	}
	return false
}

// modeSetOtherThan reports whether a sibling mode member is set to a mode other
// than USER_PROVISIONED, which is what makes omitting its partner correct.
func modeSetOtherThan(mode *string) bool {
	return mode != nil && *mode != sizingModeUserProvisioned
}

// rejectUnexpressibleChanges returns a terminal error for any differing Spec
// path UpdateFileSystem cannot carry, before the caller applies side effects.
//
// This is a convergence check, not request validation: UpdateFileSystem
// overwrites only the members it is sent, so a change the request cannot carry
// would be silently dropped and the delta would recur while Synced stays True.
// Whether a request FSx can express is also *valid* is left to the SDK and the
// API, which own those rules.
//
// Each path is probed on its own, because the request -- not the builder's
// control flow -- decides whether an update converges:
//
//  1. No payload at all means no builder handles the path.
//  2. A payload that omits the path's own member means the change is not carried.
//     Absence is exempt only where it is itself the API's way of expressing the
//     change (absenceExpressible).
func (rm *resourceManager) rejectUnexpressibleChanges(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) error {
	paths := deltaPathsOutsideTags(delta)
	if len(paths) == 0 {
		return nil
	}
	unexpressible := []string{}
	for _, path := range paths {
		single := ackcompare.NewDelta()
		single.Add(path, nil, nil)
		input, err := rm.newUpdateRequestPayload(ctx, desired, latest, single)
		if err != nil {
			// e.g. an int32 overflow or an empty password Secret.
			return err
		}
		if input == nil {
			unexpressible = append(unexpressible, path)
			continue
		}
		if absenceExpressible(path, desired) {
			continue
		}
		if carried, known := requestCarriesPath(input, path); known && !carried {
			unexpressible = append(unexpressible, path)
		}
	}
	if len(unexpressible) == 0 {
		return nil
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"cannot apply changes to %s: UpdateFileSystem has no parameter for this "+
			"change (removing a value is not supported; set it explicitly instead)",
		strings.Join(unexpressible, ", "),
	))
}

// deltaPathsOutsideTags returns the differing Spec paths that are not tags.
func deltaPathsOutsideTags(delta *ackcompare.Delta) []string {
	if delta == nil || !delta.DifferentExcept("Spec.Tags") {
		return nil
	}
	seen := map[string]bool{}
	out := []string{}
	for _, d := range delta.Differences {
		p := pathString(d.Path)
		if p == "" || strings.HasPrefix(p, "Spec.Tags") {
			continue
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

// errEmptySecret reports a referenced Secret key holding an empty value.
//
// Recoverable, not terminal: the Secret is usually populated moments later by an
// external process. Omitting the password instead would let creation succeed
// without it, and nothing would apply it once the Secret is filled.
func errEmptySecret(field string, ref *ackv1alpha1.SecretKeyReference) error {
	return ackrequeue.NeededAfter(fmt.Errorf(
		"%s references Secret %s/%s key %q, which holds an empty value",
		field, ref.Namespace, ref.Name, ref.Key,
	), 15*time.Second)
}

// setDeleteFileSystemConfiguration fills in DeleteFileSystemInput's per-type
// Delete*Configuration members, which generator.yaml strips from the model but
// which still exist on the SDK struct. Both options come from annotations -- see
// svcapitypes.SkipFinalBackupAnnotation and OpenZFSDeleteOptionsAnnotation. A
// configuration is sent only when an annotation sets something, so FSx's
// defaults apply otherwise.
//
// ONTAP has no DeleteFileSystemOntapConfiguration shape.
func setDeleteFileSystemConfiguration(
	r *resource,
	input *svcsdk.DeleteFileSystemInput,
) error {
	if r.ko.Spec.Type == nil {
		return nil
	}
	params, err := util.ParseDeletionAnnotations(r.ko.ObjectMeta.GetAnnotations())
	if err != nil {
		// Retrying cannot fix a malformed annotation value. Surface it as
		// terminal so the condition message tells the user what to correct;
		// editing the annotation triggers a fresh reconcile.
		return ackerr.NewTerminalError(err)
	}
	switch svcsdktypes.FileSystemType(*r.ko.Spec.Type) {
	case svcsdktypes.FileSystemTypeLustre:
		if params.SkipFinalBackup != nil {
			input.LustreConfiguration = &svcsdktypes.DeleteFileSystemLustreConfiguration{
				SkipFinalBackup: params.SkipFinalBackup,
			}
		}
	case svcsdktypes.FileSystemTypeOpenzfs:
		if params.SkipFinalBackup != nil || len(params.OpenZFSOptions) > 0 {
			input.OpenZFSConfiguration = &svcsdktypes.DeleteFileSystemOpenZFSConfiguration{
				SkipFinalBackup: params.SkipFinalBackup,
				Options:         params.OpenZFSOptions,
			}
		}
	case svcsdktypes.FileSystemTypeWindows:
		if params.SkipFinalBackup != nil {
			input.WindowsConfiguration = &svcsdktypes.DeleteFileSystemWindowsConfiguration{
				SkipFinalBackup: params.SkipFinalBackup,
			}
		}
	}
	return nil
}

// requeueWaitWhileDeleting is returned from the delete path while FSx still
// reports the file system. DeleteFileSystem is asynchronous and a file system
// stays in the DELETING lifecycle state for 5-10 minutes, during which its
// network interfaces are still attached to the subnet.
var requeueWaitWhileDeleting = ackrequeue.NeededAfter(
	fmt.Errorf(
		"file system is in %q state, requeuing until it is fully deleted",
		svcsdktypes.FileSystemLifecycleDeleting,
	),
	30*time.Second,
)

// isDeleting returns true if FSx has already accepted a delete request for the
// supplied file system.
func isDeleting(r *resource) bool {
	if r == nil || r.ko.Status.Lifecycle == nil {
		return false
	}
	return *r.ko.Status.Lifecycle == string(svcsdktypes.FileSystemLifecycleDeleting)
}

// int32Converter narrows the *int64 fields the CRD exposes (Kubernetes API
// convention) down to the *int32 fields the FSx SDK expects. Values that do
// not fit are a permanent user error, so the first failure is recorded as a
// terminal error and every subsequent conversion short-circuits.
type int32Converter struct {
	err error
}

func (c *int32Converter) convert(fieldPath string, v *int64) *int32 {
	if c.err != nil || v == nil {
		return nil
	}
	if *v > math.MaxInt32 || *v < math.MinInt32 {
		c.err = ackerr.NewTerminalError(fmt.Errorf(
			"%s must fit within an int32, got %d", fieldPath, *v,
		))
		return nil
	}
	out := int32(*v)
	return &out
}

// routeTableIDsDelta returns the route table IDs to add and remove to move
// from `latest` to `desired`. Create takes one RouteTableIds list (what the
// Spec exposes); Update takes AddRouteTableIds/RemoveRouteTableIds.
func routeTableIDsDelta(desired, latest []*string) (add, remove []string) {
	desiredSet := make(map[string]struct{}, len(desired))
	for _, id := range desired {
		if id != nil {
			desiredSet[*id] = struct{}{}
		}
	}
	latestSet := make(map[string]struct{}, len(latest))
	for _, id := range latest {
		if id != nil {
			latestSet[*id] = struct{}{}
		}
	}

	for _, id := range desired {
		if id == nil {
			continue
		}
		if _, found := latestSet[*id]; !found {
			add = append(add, *id)
		}
	}
	for _, id := range latest {
		if id == nil {
			continue
		}
		if _, found := desiredSet[*id]; !found {
			remove = append(remove, *id)
		}
	}
	return add, remove
}

// newDiskIOPSConfiguration converts the Spec representation of a disk IOPS
// configuration into the SDK shape shared by the ONTAP, OpenZFS and Windows
// update configurations.
func newDiskIOPSConfiguration(
	spec *svcapitypes.DiskIOPSConfiguration,
) *svcsdktypes.DiskIopsConfiguration {
	if spec == nil {
		return nil
	}
	res := &svcsdktypes.DiskIopsConfiguration{}
	if spec.IOPS != nil {
		res.Iops = spec.IOPS
	}
	if spec.Mode != nil {
		res.Mode = svcsdktypes.DiskIopsConfigurationMode(*spec.Mode)
	}
	return res
}

// newUpdateFileSystemLustreConfiguration builds the Lustre half of an
// UpdateFileSystem request, populating only members the delta reports as
// changed: UpdateFileSystem applies exactly what it is sent, and several
// Lustre properties are rejected on deployment types that lack them.
// Returns nil when nothing Lustre-related changed.
func newUpdateFileSystemLustreConfiguration(
	delta *ackcompare.Delta,
	spec *svcapitypes.CreateFileSystemLustreConfiguration,
	conv *int32Converter,
) *svcsdktypes.UpdateFileSystemLustreConfiguration {
	if spec == nil {
		return nil
	}
	const p = "Spec.LustreConfiguration."
	res := &svcsdktypes.UpdateFileSystemLustreConfiguration{}
	changed := false

	// Describe nests AutoImportPolicy under DataRepositoryConfiguration;
	// recoverObservedLustreDataRepository supplies it on read.
	if delta.DifferentAt(p+"AutoImportPolicy") && spec.AutoImportPolicy != nil {
		res.AutoImportPolicy = svcsdktypes.AutoImportPolicyType(*spec.AutoImportPolicy)
		changed = true
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DataCompressionType") && spec.DataCompressionType != nil {
		res.DataCompressionType = svcsdktypes.DataCompressionType(*spec.DataCompressionType)
		changed = true
	}
	if delta.DifferentAt(p+"DataReadCacheConfiguration") && spec.DataReadCacheConfiguration != nil {
		drcCfg := &svcsdktypes.LustreReadCacheConfiguration{}
		drcCfg.SizeGiB = conv.convert(
			p+"DataReadCacheConfiguration.SizeGiB", spec.DataReadCacheConfiguration.SizeGiB,
		)
		if spec.DataReadCacheConfiguration.SizingMode != nil {
			drcCfg.SizingMode = svcsdktypes.LustreReadCacheSizingMode(
				*spec.DataReadCacheConfiguration.SizingMode,
			)
		}
		res.DataReadCacheConfiguration = drcCfg
		changed = true
	}
	if delta.DifferentAt(p+"LogConfiguration") && spec.LogConfiguration != nil {
		logCfg := &svcsdktypes.LustreLogCreateConfiguration{}
		if spec.LogConfiguration.Destination != nil {
			logCfg.Destination = spec.LogConfiguration.Destination
		}
		if spec.LogConfiguration.Level != nil {
			logCfg.Level = svcsdktypes.LustreAccessAuditLogLevel(*spec.LogConfiguration.Level)
		}
		res.LogConfiguration = logCfg
		changed = true
	}
	if delta.DifferentAt(p+"MetadataConfiguration") && spec.MetadataConfiguration != nil {
		mdCfg := &svcsdktypes.UpdateFileSystemLustreMetadataConfiguration{}
		mdCfg.Iops = conv.convert(p+"MetadataConfiguration.IOPS", spec.MetadataConfiguration.IOPS)
		if spec.MetadataConfiguration.Mode != nil {
			mdCfg.Mode = svcsdktypes.MetadataConfigurationMode(*spec.MetadataConfiguration.Mode)
		}
		res.MetadataConfiguration = mdCfg
		changed = true
	}
	if delta.DifferentAt(p+"PerUnitStorageThroughput") && spec.PerUnitStorageThroughput != nil {
		res.PerUnitStorageThroughput = conv.convert(
			p+"PerUnitStorageThroughput", spec.PerUnitStorageThroughput,
		)
		changed = true
	}
	if delta.DifferentAt(p+"RootSquashConfiguration") && spec.RootSquashConfiguration != nil {
		rsCfg := &svcsdktypes.LustreRootSquashConfiguration{}
		if spec.RootSquashConfiguration.NoSquashNids != nil {
			rsCfg.NoSquashNids = aws.ToStringSlice(spec.RootSquashConfiguration.NoSquashNids)
		}
		if spec.RootSquashConfiguration.RootSquash != nil {
			rsCfg.RootSquash = spec.RootSquashConfiguration.RootSquash
		}
		res.RootSquashConfiguration = rsCfg
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil
	}
	return res
}

// newUpdateFileSystemOntapConfiguration builds the ONTAP half of an
// UpdateFileSystem request; see the Lustre variant for why only changed
// members are populated. Returns nil when nothing ONTAP-related changed.
func (rm *resourceManager) newUpdateFileSystemOntapConfiguration(
	ctx context.Context,
	delta *ackcompare.Delta,
	desired *resource,
	latest *resource,
	conv *int32Converter,
) (*svcsdktypes.UpdateFileSystemOntapConfiguration, error) {
	spec := desired.ko.Spec.OntapConfiguration
	if spec == nil {
		return nil, nil
	}
	const p = "Spec.OntapConfiguration."
	res := &svcsdktypes.UpdateFileSystemOntapConfiguration{}
	changed := false

	if delta.DifferentAt(p + "RouteTableIDs") {
		var latestRouteTableIDs []*string
		if latest != nil && latest.ko.Spec.OntapConfiguration != nil {
			latestRouteTableIDs = latest.ko.Spec.OntapConfiguration.RouteTableIDs
		}
		add, remove := routeTableIDsDelta(spec.RouteTableIDs, latestRouteTableIDs)
		if len(add) > 0 {
			res.AddRouteTableIds = add
			changed = true
		}
		if len(remove) > 0 {
			res.RemoveRouteTableIds = remove
			changed = true
		}
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DiskIOPSConfiguration") && spec.DiskIOPSConfiguration != nil {
		res.DiskIopsConfiguration = newDiskIOPSConfiguration(spec.DiskIOPSConfiguration)
		changed = true
	}
	if delta.DifferentAt(p+"EndpointIPv6AddressRange") && spec.EndpointIPv6AddressRange != nil {
		res.EndpointIpv6AddressRange = spec.EndpointIPv6AddressRange
		changed = true
	}
	if delta.DifferentAt(p+"HAPairs") && spec.HAPairs != nil {
		res.HAPairs = conv.convert(p+"HAPairs", spec.HAPairs)
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacityPerHAPair") && spec.ThroughputCapacityPerHAPair != nil {
		res.ThroughputCapacityPerHAPair = conv.convert(
			p+"ThroughputCapacityPerHAPair", spec.ThroughputCapacityPerHAPair,
		)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil, nil
	}
	return res, nil
}

// newUpdateFileSystemOpenZFSConfiguration builds the OpenZFS half of an
// UpdateFileSystem request; see the Lustre variant for why only changed
// members are populated. Returns nil when nothing OpenZFS-related changed.
func newUpdateFileSystemOpenZFSConfiguration(
	delta *ackcompare.Delta,
	desired *resource,
	latest *resource,
	conv *int32Converter,
) *svcsdktypes.UpdateFileSystemOpenZFSConfiguration {
	spec := desired.ko.Spec.OpenZFSConfiguration
	if spec == nil {
		return nil
	}
	const p = "Spec.OpenZFSConfiguration."
	res := &svcsdktypes.UpdateFileSystemOpenZFSConfiguration{}
	changed := false

	if delta.DifferentAt(p + "RouteTableIDs") {
		var latestRouteTableIDs []*string
		if latest != nil && latest.ko.Spec.OpenZFSConfiguration != nil {
			latestRouteTableIDs = latest.ko.Spec.OpenZFSConfiguration.RouteTableIDs
		}
		add, remove := routeTableIDsDelta(spec.RouteTableIDs, latestRouteTableIDs)
		if len(add) > 0 {
			res.AddRouteTableIds = add
			changed = true
		}
		if len(remove) > 0 {
			res.RemoveRouteTableIds = remove
			changed = true
		}
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"CopyTagsToBackups") && spec.CopyTagsToBackups != nil {
		res.CopyTagsToBackups = spec.CopyTagsToBackups
		changed = true
	}
	if delta.DifferentAt(p+"CopyTagsToVolumes") && spec.CopyTagsToVolumes != nil {
		res.CopyTagsToVolumes = spec.CopyTagsToVolumes
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DiskIOPSConfiguration") && spec.DiskIOPSConfiguration != nil {
		res.DiskIopsConfiguration = newDiskIOPSConfiguration(spec.DiskIOPSConfiguration)
		changed = true
	}
	if delta.DifferentAt(p+"EndpointIPv6AddressRange") && spec.EndpointIPv6AddressRange != nil {
		res.EndpointIpv6AddressRange = spec.EndpointIPv6AddressRange
		changed = true
	}
	if delta.DifferentAt(p+"ReadCacheConfiguration") && spec.ReadCacheConfiguration != nil {
		rcCfg := &svcsdktypes.OpenZFSReadCacheConfiguration{}
		rcCfg.SizeGiB = conv.convert(p+"ReadCacheConfiguration.SizeGiB", spec.ReadCacheConfiguration.SizeGiB)
		if spec.ReadCacheConfiguration.SizingMode != nil {
			rcCfg.SizingMode = svcsdktypes.OpenZFSReadCacheSizingMode(*spec.ReadCacheConfiguration.SizingMode)
		}
		res.ReadCacheConfiguration = rcCfg
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil
	}
	return res
}

// newUpdateFileSystemWindowsConfiguration builds the Windows half of an
// UpdateFileSystem request; see the Lustre variant for why only changed
// members are populated. Returns nil when nothing Windows-related changed.
//
// Aliases has no member here: FSx changes them via
// Associate/DisassociateFileSystemAliases.
func (rm *resourceManager) newUpdateFileSystemWindowsConfiguration(
	ctx context.Context,
	delta *ackcompare.Delta,
	desired *resource,
	conv *int32Converter,
) (*svcsdktypes.UpdateFileSystemWindowsConfiguration, error) {
	spec := desired.ko.Spec.WindowsConfiguration
	if spec == nil {
		return nil, nil
	}
	const p = "Spec.WindowsConfiguration."
	res := &svcsdktypes.UpdateFileSystemWindowsConfiguration{}
	changed := false

	if delta.DifferentAt(p+"AuditLogConfiguration") && spec.AuditLogConfiguration != nil {
		alCfg := &svcsdktypes.WindowsAuditLogCreateConfiguration{}
		if spec.AuditLogConfiguration.AuditLogDestination != nil {
			alCfg.AuditLogDestination = spec.AuditLogConfiguration.AuditLogDestination
		}
		if spec.AuditLogConfiguration.FileAccessAuditLogLevel != nil {
			alCfg.FileAccessAuditLogLevel = svcsdktypes.WindowsAccessAuditLogLevel(
				*spec.AuditLogConfiguration.FileAccessAuditLogLevel,
			)
		}
		if spec.AuditLogConfiguration.FileShareAccessAuditLogLevel != nil {
			alCfg.FileShareAccessAuditLogLevel = svcsdktypes.WindowsAccessAuditLogLevel(
				*spec.AuditLogConfiguration.FileShareAccessAuditLogLevel,
			)
		}
		res.AuditLogConfiguration = alCfg
		changed = true
	}
	if delta.DifferentAt(p+"AutomaticBackupRetentionDays") && spec.AutomaticBackupRetentionDays != nil {
		res.AutomaticBackupRetentionDays = conv.convert(
			p+"AutomaticBackupRetentionDays", spec.AutomaticBackupRetentionDays,
		)
		changed = true
	}
	if delta.DifferentAt(p+"DailyAutomaticBackupStartTime") && spec.DailyAutomaticBackupStartTime != nil {
		res.DailyAutomaticBackupStartTime = spec.DailyAutomaticBackupStartTime
		changed = true
	}
	if delta.DifferentAt(p+"DiskIOPSConfiguration") && spec.DiskIOPSConfiguration != nil {
		res.DiskIopsConfiguration = newDiskIOPSConfiguration(spec.DiskIOPSConfiguration)
		changed = true
	}
	if delta.DifferentAt(p+"FsrmConfiguration") && spec.FsrmConfiguration != nil {
		fsrmCfg := &svcsdktypes.WindowsFsrmConfiguration{}
		if spec.FsrmConfiguration.EventLogDestination != nil {
			fsrmCfg.EventLogDestination = spec.FsrmConfiguration.EventLogDestination
		}
		if spec.FsrmConfiguration.FsrmServiceEnabled != nil {
			fsrmCfg.FsrmServiceEnabled = spec.FsrmConfiguration.FsrmServiceEnabled
		}
		res.FsrmConfiguration = fsrmCfg
		changed = true
	}
	if delta.DifferentAt(p+"SelfManagedActiveDirectoryConfiguration") &&
		spec.SelfManagedActiveDirectoryConfiguration != nil {
		smad := spec.SelfManagedActiveDirectoryConfiguration
		smadUpdates := &svcsdktypes.SelfManagedActiveDirectoryConfigurationUpdates{}
		if smad.DNSIPs != nil {
			smadUpdates.DnsIps = aws.ToStringSlice(smad.DNSIPs)
		}
		if smad.DomainJoinServiceAccountSecret != nil {
			smadUpdates.DomainJoinServiceAccountSecret = smad.DomainJoinServiceAccountSecret
		}
		if smad.DomainName != nil {
			smadUpdates.DomainName = smad.DomainName
		}
		if smad.FileSystemAdministratorsGroup != nil {
			smadUpdates.FileSystemAdministratorsGroup = smad.FileSystemAdministratorsGroup
		}
		if smad.OrganizationalUnitDistinguishedName != nil {
			smadUpdates.OrganizationalUnitDistinguishedName = smad.OrganizationalUnitDistinguishedName
		}
		if smad.UserName != nil {
			smadUpdates.UserName = smad.UserName
		}
		// Every member of SelfManagedActiveDirectoryConfigurationUpdates is an
		// overwrite, so send the complete block, password included.
		if smad.Password != nil {
			tmpSecret, err := rm.rr.SecretValueFromReference(ctx, smad.Password)
			if err != nil {
				return nil, ackrequeue.Needed(err)
			}
			if tmpSecret == "" {
				return nil, errEmptySecret(
					p+"SelfManagedActiveDirectoryConfiguration.Password", smad.Password,
				)
			}
			smadUpdates.Password = aws.String(tmpSecret)
		}
		res.SelfManagedActiveDirectoryConfiguration = smadUpdates
		changed = true
	}
	if delta.DifferentAt(p+"ThroughputCapacity") && spec.ThroughputCapacity != nil {
		res.ThroughputCapacity = conv.convert(p+"ThroughputCapacity", spec.ThroughputCapacity)
		changed = true
	}
	if delta.DifferentAt(p+"WeeklyMaintenanceStartTime") && spec.WeeklyMaintenanceStartTime != nil {
		res.WeeklyMaintenanceStartTime = spec.WeeklyMaintenanceStartTime
		changed = true
	}

	if !changed {
		return nil, nil
	}
	return res, nil
}

// newUpdateRequestPayload builds the UpdateFileSystem request for the supplied
// delta, returning (nil, nil) when there is nothing UpdateFileSystem can act
// on -- the caller must then skip the API call.
func (rm *resourceManager) newUpdateRequestPayload(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (*svcsdk.UpdateFileSystemInput, error) {
	conv := &int32Converter{}
	// Prefer the ID from the most recent read; fall back to the stored CR in
	// case `latest` was assembled without it.
	fileSystemID := latest.ko.Status.ID
	if fileSystemID == nil {
		fileSystemID = desired.ko.Status.ID
	}
	res := &svcsdk.UpdateFileSystemInput{
		FileSystemId: fileSystemID,
	}
	changed := false

	if delta.DifferentAt("Spec.StorageCapacity") && desired.ko.Spec.StorageCapacity != nil {
		res.StorageCapacity = conv.convert("Spec.StorageCapacity", desired.ko.Spec.StorageCapacity)
		changed = true
	}
	if delta.DifferentAt("Spec.StorageType") && desired.ko.Spec.StorageType != nil {
		res.StorageType = svcsdktypes.StorageType(*desired.ko.Spec.StorageType)
		changed = true
	}
	if delta.DifferentAt("Spec.TypeVersion") && desired.ko.Spec.TypeVersion != nil {
		res.FileSystemTypeVersion = desired.ko.Spec.TypeVersion
		changed = true
	}
	if delta.DifferentAt("Spec.NetworkType") && desired.ko.Spec.NetworkType != nil {
		res.NetworkType = svcsdktypes.NetworkType(*desired.ko.Spec.NetworkType)
		changed = true
	}

	if lustreCfg := newUpdateFileSystemLustreConfiguration(
		delta, desired.ko.Spec.LustreConfiguration, conv,
	); lustreCfg != nil {
		res.LustreConfiguration = lustreCfg
		changed = true
	}

	ontapCfg, err := rm.newUpdateFileSystemOntapConfiguration(ctx, delta, desired, latest, conv)
	if err != nil {
		return nil, err
	}
	if ontapCfg != nil {
		res.OntapConfiguration = ontapCfg
		changed = true
	}

	if openZFSCfg := newUpdateFileSystemOpenZFSConfiguration(
		delta, desired, latest, conv,
	); openZFSCfg != nil {
		res.OpenZFSConfiguration = openZFSCfg
		changed = true
	}

	windowsCfg, err := rm.newUpdateFileSystemWindowsConfiguration(ctx, delta, desired, conv)
	if err != nil {
		return nil, err
	}
	if windowsCfg != nil {
		res.WindowsConfiguration = windowsCfg
		changed = true
	}

	if conv.err != nil {
		return nil, conv.err
	}
	if !changed {
		return nil, nil
	}
	return res, nil
}

// rejectEmptyResolvedPasswords fails the create when a declared password
// reference resolved to an empty value, which the generated builder silently
// omits from the request.
func rejectEmptyResolvedPasswords(
	r *resource,
	input *svcsdk.CreateFileSystemInput,
) error {
	if r == nil || r.ko == nil || input == nil {
		return nil
	}
	if ref := fsxAdminPasswordRef(r.ko); ref != nil {
		if input.OntapConfiguration == nil || input.OntapConfiguration.FsxAdminPassword == nil {
			return errEmptySecret("Spec.OntapConfiguration.FsxAdminPassword", ref)
		}
	}
	if ref := selfManagedADPasswordRef(r.ko); ref != nil {
		smad := input.WindowsConfiguration
		if smad == nil || smad.SelfManagedActiveDirectoryConfiguration == nil ||
			smad.SelfManagedActiveDirectoryConfiguration.Password == nil {
			return errEmptySecret(
				"Spec.WindowsConfiguration.SelfManagedActiveDirectoryConfiguration.Password", ref,
			)
		}
	}
	return nil
}

// terminalIfFailed returns a terminal error when FSx reports the file system as
// FAILED, which the API documents as unrecoverable.
//
// Without this, ResourceSynced stays False (synced.when lists only AVAILABLE)
// and the resource is requeued forever, calling DescribeFileSystems for a state
// that cannot change. The returned resource is non-nil so the runtime still
// patches Status, surfacing FailureDetails alongside the Terminal condition.
//
// Never while the CR is being deleted: the delete path calls ReadOne first and
// aborts on any error but NotFound, so returning terminal there would strand the
// finalizer -- and deletion is the recovery AWS documents for a FAILED file
// system.
func terminalIfFailed(ko *svcapitypes.FileSystem) error {
	if ko == nil || ko.Status.Lifecycle == nil {
		return nil
	}
	if ko.DeletionTimestamp != nil {
		return nil
	}
	if *ko.Status.Lifecycle != string(svcsdktypes.FileSystemLifecycleFailed) {
		return nil
	}
	reason := "no failure details reported"
	if ko.Status.FailureDetails != nil && ko.Status.FailureDetails.Message != nil {
		reason = *ko.Status.FailureDetails.Message
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"file system is in FAILED state and cannot recover: %s", reason,
	))
}

// setFileSystemStatus copies the FSx-owned state from an SDK FileSystem shape
// onto the supplied CR.
func setFileSystemStatus(
	ko *svcapitypes.FileSystem,
	fs *svcsdktypes.FileSystem,
) {
	if fs == nil {
		return
	}
	if fs.CreationTime != nil {
		ko.Status.CreationTime = &metav1.Time{Time: *fs.CreationTime}
	}
	if fs.DNSName != nil {
		ko.Status.DNSName = fs.DNSName
	}
	if fs.FailureDetails != nil {
		ko.Status.FailureDetails = &svcapitypes.FileSystemFailureDetails{
			Message: fs.FailureDetails.Message,
		}
	} else {
		ko.Status.FailureDetails = nil
	}
	if fs.FileSystemId != nil {
		ko.Status.ID = fs.FileSystemId
	}
	if fs.Lifecycle != "" {
		ko.Status.Lifecycle = aws.String(string(fs.Lifecycle))
	}
	if fs.NetworkInterfaceIds != nil {
		ko.Status.NetworkInterfaceIDs = aws.StringSlice(fs.NetworkInterfaceIds)
	}
	if fs.OwnerId != nil {
		ko.Status.OwnerID = fs.OwnerId
	}
	// ResourceARN is marked `is_arn: true` in generator.yaml, so the CRD has no
	// standalone Status.ResourceARN field: the value belongs in
	// Status.ACKResourceMetadata.ARN, which is what syncTags (and every ACK
	// consumer of the resource's ARN) reads.
	if fs.ResourceARN != nil {
		if ko.Status.ACKResourceMetadata == nil {
			ko.Status.ACKResourceMetadata = &ackv1alpha1.ResourceMetadata{}
		}
		arn := ackv1alpha1.AWSResourceName(*fs.ResourceARN)
		ko.Status.ACKResourceMetadata.ARN = &arn
	}
	if fs.VpcId != nil {
		ko.Status.VPCID = fs.VpcId
	}
}

// syncTags brings the file system's AWS tags in line with Spec.Tags using the
// FSx TagResource/UntagResource operations. FSx has no bulk "replace tags"
// call, so removals and additions are issued separately.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.syncTags")
	defer func() { exit(err) }()

	// ResourceARN carries `is_arn: true` in generator.yaml, so sdkFind/sdkCreate
	// populate Status.ACKResourceMetadata.ARN and `latest` (which always comes
	// from a read) has it. The guard is purely defensive.
	arn := (*string)(nil)
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

// customUpdateFileSystem replaces the generated sdkUpdate, which does not
// compile: FSx models each config block with three shapes (Create*, Update*,
// <Type>FileSystemConfiguration), and where their members diverge -- notably
// Add/RemoveRouteTableIds vs the Spec's single RouteTableIDs list -- code-gen
// emits references to Spec fields that do not exist.
func (rm *resourceManager) customUpdateFileSystem(
	ctx context.Context,
	desired *resource,
	latest *resource,
	delta *ackcompare.Delta,
) (updated *resource, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.customUpdateFileSystem")
	defer func() { exit(err) }()

	// desired already carries the observed Status (runtime v0.63.0+).
	ko := desired.ko.DeepCopy()

	// Reject changes UpdateFileSystem cannot carry -- including edits to
	// immutable fields, which no Update*Configuration shape has -- before
	// anything is sent. Otherwise an edit mixing a supported and an unsupported
	// change would apply the supported half (possibly an irreversible storage
	// increase) and then fail. This is not atomicity: TagResource and
	// UpdateFileSystem can still fail independently.
	if err = rm.rejectUnexpressibleChanges(ctx, desired, latest, delta); err != nil {
		return nil, err
	}

	// Tags are managed by TagResource/UntagResource rather than
	// UpdateFileSystem.
	if delta.DifferentAt("Spec.Tags") {
		if err = rm.syncTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}

	input, err := rm.newUpdateRequestPayload(ctx, desired, latest, delta)
	if err != nil {
		return nil, err
	}
	if input == nil {
		// Preflight guarantees every non-tag path is expressible, so reaching
		// here means tags were the only difference.
		rlog.Debug("only tags changed; skipping UpdateFileSystem")
		rm.setStatusDefaults(ko)
		return &resource{ko}, nil
	}

	var resp *svcsdk.UpdateFileSystemOutput
	resp, err = rm.sdkapi.UpdateFileSystem(ctx, input)
	rm.metrics.RecordAPICall("UPDATE", "UpdateFileSystem", err)
	if err != nil {
		return nil, err
	}

	setFileSystemStatus(ko, resp.FileSystem)
	rm.setStatusDefaults(ko)
	return &resource{ko}, nil
}
