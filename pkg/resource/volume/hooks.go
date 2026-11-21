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

package volume

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	ackerr "github.com/aws-controllers-k8s/runtime/pkg/errors"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/fsx"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/fsx/types"

	svcapitypes "github.com/aws-controllers-k8s/fsx-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/fsx-controller/pkg/util"
)

// BypassSnaplockEnterpriseRetentionAnnotation sets
// DeleteVolumeOntapConfiguration.BypassSnaplockEnterpriseRetention ("true" or
// "false") when an ONTAP Volume is deleted. Absent sends nothing, so FSx's
// default (false: retained WORM files block the delete) applies. Setting it
// to "true" deletes a SnapLock Enterprise volume that still holds retained
// files; it is never defaulted on.
//
// The other delete-time options reuse the FileSystem annotations:
//   - svcapitypes.SkipFinalBackupAnnotation sets
//     DeleteVolumeOntapConfiguration.SkipFinalBackup (ONTAP only). Absent
//     sends nothing, so FSx takes a final backup.
//   - svcapitypes.OpenZFSDeleteOptionsAnnotation sets
//     DeleteVolumeOpenZFSConfiguration.Options (OpenZFS only), validated
//     against the DeleteOpenZFSVolumeOption enum. Absent sends nothing, so FSx
//     refuses to delete a volume that has child volumes or snapshots, and the
//     refusal surfaces as a condition.
var BypassSnaplockEnterpriseRetentionAnnotation = fmt.Sprintf(
	"%s/bypass-snaplock-enterprise-retention", svcapitypes.GroupVersion.Group,
)

// requeueWaitWhileDeleting is returned from the delete path while FSx still
// reports the volume: DeleteVolume is asynchronous.
var requeueWaitWhileDeleting = ackrequeue.NeededAfter(
	fmt.Errorf(
		"volume is in %q state, requeuing until it is fully deleted",
		svcsdktypes.VolumeLifecycleDeleting,
	),
	15*time.Second,
)

// syncTags brings the volume's AWS tags in line with Spec.Tags using
// TagResource/UntagResource (see util.SyncTags). UpdateVolume has no Tags
// member.
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

// getTags returns the volume's tags from ListTagsForResource.
//
// DescribeVolumes does not report volume tags although the model has
// Volume.Tags (e2e: Describe returned no Tags while ListTagsForResource had
// them). Without this latest.Spec.Tags is always nil: every reconcile sees a
// Spec.Tags delta and re-issues TagResource, removed tags are never untagged,
// and the nil is patched back onto the CR, wiping spec.tags.
func (rm *resourceManager) getTags(
	ctx context.Context,
	arn string,
) (tags []*svcapitypes.Tag, err error) {
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("rm.getTags")
	defer func() { exit(err) }()

	tags = []*svcapitypes.Tag{}
	input := &svcsdk.ListTagsForResourceInput{ResourceARN: aws.String(arn)}
	for {
		resp, err := rm.sdkapi.ListTagsForResource(ctx, input)
		rm.metrics.RecordAPICall("READ_MANY", "ListTagsForResource", err)
		if err != nil {
			return nil, err
		}
		for _, t := range resp.Tags {
			tags = append(tags, &svcapitypes.Tag{Key: t.Key, Value: t.Value})
		}
		if resp.NextToken == nil || *resp.NextToken == "" {
			break
		}
		input.NextToken = resp.NextToken
	}
	if len(tags) == 0 {
		return nil, nil
	}
	return tags, nil
}

// setTagsFromAWS replaces ko.Spec.Tags with the tags FSx reports for the
// volume. It is skipped while the volume is DELETING, where tags no longer
// matter and the delete path only needs Describe to report the volume gone,
// and when the ARN is not known yet.
func (rm *resourceManager) setTagsFromAWS(
	ctx context.Context,
	ko *svcapitypes.Volume,
) error {
	if ko.Status.Lifecycle != nil &&
		*ko.Status.Lifecycle == string(svcsdktypes.VolumeLifecycleDeleting) {
		return nil
	}
	if ko.Status.ACKResourceMetadata == nil || ko.Status.ACKResourceMetadata.ARN == nil {
		return nil
	}
	tags, err := rm.getTags(ctx, string(*ko.Status.ACKResourceMetadata.ARN))
	if err != nil {
		return err
	}
	ko.Spec.Tags = tags
	return nil
}

// restoreNonRoundTrippedSpecFields fixes up the ONTAP configuration the
// generated setters build from a Create or Describe response, which replace
// ko.Spec.OntapConfiguration wholesale (the Update response does not set the
// Spec; see generator.yaml):
//
//   - AggregateConfiguration.ConstituentsPerAggregate is create-only (absent
//     from UpdateOntapVolumeConfiguration) and the output shape has no such
//     member (it reports TotalConstituents instead), so it is copied from
//     desired when declared. Restoring it cannot mask a change the controller
//     could apply.
//   - TieringPolicy.CoolingPeriod is dropped when desired leaves it unset.
//     FSx reports a default only for the AUTO and SNAPSHOT_ONLY policies, so
//     it cannot be late-initialized: once copied into the Spec it would be
//     re-sent when the policy changes to one that rejects it. A declared
//     value is left as observed, so drift on it stays visible.
//
// Nothing is restored when either side has no ONTAP configuration, so no
// configuration the user did not submit is fabricated.
func restoreNonRoundTrippedSpecFields(
	desired *svcapitypes.Volume,
	ko *svcapitypes.Volume,
) {
	if desired == nil || ko == nil {
		return
	}
	src := desired.Spec.OntapConfiguration
	dst := ko.Spec.OntapConfiguration
	if src == nil || dst == nil {
		return
	}
	if src.AggregateConfiguration != nil &&
		src.AggregateConfiguration.ConstituentsPerAggregate != nil {
		if dst.AggregateConfiguration == nil {
			dst.AggregateConfiguration = &svcapitypes.CreateAggregateConfiguration{}
		}
		dst.AggregateConfiguration.ConstituentsPerAggregate =
			src.AggregateConfiguration.ConstituentsPerAggregate
	}
	if dst.TieringPolicy != nil &&
		(src.TieringPolicy == nil || src.TieringPolicy.CoolingPeriod == nil) {
		dst.TieringPolicy.CoolingPeriod = nil
	}
}

// terminalIfFailed returns a terminal error when FSx reports the volume as
// FAILED ("Amazon FSx was unable to create the volume"). FAILED is final on
// the AWS side: no Spec edit or retry recovers it, and deleting and
// recreating the CR is the only way out. The Terminal condition stops a
// pointless requeue and surfaces LifecycleTransitionReason. The returned
// resource is non-nil so Status is still patched alongside the condition.
//
// MISCONFIGURED is deliberately excluded: FSx documents it as recoverable, and
// a Terminal condition would stop the requeue that observes recovery.
//
// Never while the CR is being deleted: the delete path calls ReadOne first and
// aborts on any error but NotFound, and FAILED volumes are deletable.
func terminalIfFailed(ko *svcapitypes.Volume) error {
	if ko == nil || ko.Status.Lifecycle == nil {
		return nil
	}
	if ko.DeletionTimestamp != nil {
		return nil
	}
	if *ko.Status.Lifecycle != string(svcsdktypes.VolumeLifecycleFailed) {
		return nil
	}
	reason := "no lifecycle transition reason reported"
	if ko.Status.LifecycleTransitionReason != nil &&
		ko.Status.LifecycleTransitionReason.Message != nil {
		reason = *ko.Status.LifecycleTransitionReason.Message
	}
	return ackerr.NewTerminalError(fmt.Errorf(
		"volume is in FAILED state and cannot recover: %s", reason,
	))
}

// setDeletingLifecycle records the lifecycle returned by DeleteVolume (falling
// back to DELETING) on r, so Status shows the delete in progress. Later
// reconciles are held by the deletable.when guard, which does not patch
// Status, so this value stays.
func setDeletingLifecycle(
	r *resource,
	resp *svcsdk.DeleteVolumeOutput,
) {
	if resp != nil && resp.Lifecycle != "" {
		r.ko.Status.Lifecycle = aws.String(string(resp.Lifecycle))
		return
	}
	r.ko.Status.Lifecycle = aws.String(string(svcsdktypes.VolumeLifecycleDeleting))
}

// deleteVolumeAnnotationParameters holds the delete-time options read from
// annotations. Each is nil/empty when its annotation is absent.
type deleteVolumeAnnotationParameters struct {
	SkipFinalBackup                   *bool
	BypassSnaplockEnterpriseRetention *bool
	OpenZFSOptions                    []svcsdktypes.DeleteOpenZFSVolumeOption
}

// parseDeleteVolumeAnnotations parses the Volume deletion annotations. An
// unparseable value is an error rather than a silent default, so a typo
// cannot quietly change what a delete destroys.
//
// util.ParseDeletionAnnotations is not reused: it validates the OpenZFS
// options against the FileSystem enum (DeleteFileSystemOpenZFSOption), whose
// values differ from DeleteOpenZFSVolumeOption.
func parseDeleteVolumeAnnotations(
	annotations map[string]string,
) (*deleteVolumeAnnotationParameters, error) {
	params := &deleteVolumeAnnotationParameters{}
	var err error
	if params.SkipFinalBackup, err = parseBoolAnnotation(
		annotations, svcapitypes.SkipFinalBackupAnnotation,
	); err != nil {
		return nil, err
	}
	if params.BypassSnaplockEnterpriseRetention, err = parseBoolAnnotation(
		annotations, BypassSnaplockEnterpriseRetentionAnnotation,
	); err != nil {
		return nil, err
	}
	if value, ok := annotations[svcapitypes.OpenZFSDeleteOptionsAnnotation]; ok && value != "" {
		// Validated against the SDK enum rather than a hard-coded list, so a
		// value FSx adds later is accepted after an SDK bump with no code change.
		known := svcsdktypes.DeleteOpenZFSVolumeOption("").Values()
		for _, raw := range strings.Split(value, ",") {
			opt := svcsdktypes.DeleteOpenZFSVolumeOption(strings.TrimSpace(raw))
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

func parseBoolAnnotation(annotations map[string]string, key string) (*bool, error) {
	value, ok := annotations[key]
	if !ok || value == "" {
		return nil, nil
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return nil, fmt.Errorf("invalid value %q for annotation %s: %v", value, key, err)
	}
	return &b, nil
}

func containsOption(
	known []svcsdktypes.DeleteOpenZFSVolumeOption,
	opt svcsdktypes.DeleteOpenZFSVolumeOption,
) bool {
	for _, k := range known {
		if k == opt {
			return true
		}
	}
	return false
}

// setDeleteVolumeConfiguration fills in DeleteVolumeInput's per-type
// configuration, which generator.yaml strips from the model but which still
// exists on the SDK struct. A configuration is sent only when an annotation
// for that volume type sets something, so FSx's defaults apply otherwise.
// FinalBackupTags is not supported.
func setDeleteVolumeConfiguration(
	r *resource,
	input *svcsdk.DeleteVolumeInput,
) error {
	if r.ko.Spec.Type == nil {
		return nil
	}
	params, err := parseDeleteVolumeAnnotations(r.ko.ObjectMeta.GetAnnotations())
	if err != nil {
		// Returned as a retryable error, not a terminal one: with the default
		// feature gates an annotation edit does not start a reconcile (only
		// a generation change does), so a terminal error would leave the CR
		// stuck in deletion until the next resync. The requeue backoff picks
		// up the corrected annotation, and the error stays in the condition
		// message until then. Nothing is sent to FSx while it is malformed.
		return err
	}
	switch svcsdktypes.VolumeType(*r.ko.Spec.Type) {
	case svcsdktypes.VolumeTypeOntap:
		if params.SkipFinalBackup != nil || params.BypassSnaplockEnterpriseRetention != nil {
			input.OntapConfiguration = &svcsdktypes.DeleteVolumeOntapConfiguration{
				SkipFinalBackup:                   params.SkipFinalBackup,
				BypassSnaplockEnterpriseRetention: params.BypassSnaplockEnterpriseRetention,
			}
		}
	case svcsdktypes.VolumeTypeOpenzfs:
		if len(params.OpenZFSOptions) > 0 {
			input.OpenZFSConfiguration = &svcsdktypes.DeleteVolumeOpenZFSConfiguration{
				Options: params.OpenZFSOptions,
			}
		}
	}
	return nil
}

// normalizeOpenZFSLists makes the generated, order-sensitive compare of the
// OpenZFS NFS export and quota lists ignore what FSx rewrites, by copying
// desired's list into latest when the two are equivalent:
//
//   - NfsExports[].ClientConfigurations: FSx returns the client
//     configurations in its own order (e2e: two declared entries came back
//     reversed). Equal as multisets ⇒ equivalent.
//   - UserAndGroupQuotas: FSx always reports the root entries
//     {Type: USER|GROUP, Id: 0, StorageCapacityQuotaGiB: 0} (e2e: present on
//     a volume created without quotas, and appended after two declared
//     entries by UpdateVolume) and may reorder entries. A reported root entry
//     with quota 0 whose (Type, Id 0) desired does not declare is skipped;
//     the rest must match as a multiset.
//
// Any other difference is left to the generated compare, so a real change
// (or drift) still produces a delta. latest gets a copy, never desired's
// slice, so later writes to one cannot change the other.
func normalizeOpenZFSLists(a, b *resource) {
	if a.ko.Spec.OpenZFSConfiguration == nil || b.ko.Spec.OpenZFSConfiguration == nil {
		return
	}
	desired := a.ko.Spec.OpenZFSConfiguration
	latest := b.ko.Spec.OpenZFSConfiguration

	if len(desired.NfsExports) == len(latest.NfsExports) {
		for i := range desired.NfsExports {
			d, l := desired.NfsExports[i], latest.NfsExports[i]
			if d == nil || l == nil {
				continue
			}
			if clientConfigurationsEquivalent(d.ClientConfigurations, l.ClientConfigurations) {
				l.ClientConfigurations = make([]*svcapitypes.OpenZFSClientConfiguration, len(d.ClientConfigurations))
				for j, c := range d.ClientConfigurations {
					l.ClientConfigurations[j] = c.DeepCopy()
				}
			}
		}
	}

	if quotasEquivalent(desired.UserAndGroupQuotas, latest.UserAndGroupQuotas) {
		if desired.UserAndGroupQuotas == nil {
			latest.UserAndGroupQuotas = nil
		} else {
			latest.UserAndGroupQuotas = make([]*svcapitypes.OpenZFSUserOrGroupQuota, len(desired.UserAndGroupQuotas))
			for j, q := range desired.UserAndGroupQuotas {
				latest.UserAndGroupQuotas[j] = q.DeepCopy()
			}
		}
	}
}

func strVal(s *string) string {
	if s == nil {
		return "\x00nil"
	}
	return *s
}

func int64Val(i *int64) string {
	if i == nil {
		return "nil"
	}
	return strconv.FormatInt(*i, 10)
}

func clientConfigurationKey(c *svcapitypes.OpenZFSClientConfiguration) string {
	if c == nil {
		return "\x00nil"
	}
	parts := []string{strVal(c.Clients)}
	for _, o := range c.Options {
		parts = append(parts, strVal(o))
	}
	return strings.Join(parts, "\x01")
}

// clientConfigurationsEquivalent reports whether a and b hold the same
// client configurations regardless of order. Options keep their order: FSx
// returned them as submitted.
func clientConfigurationsEquivalent(a, b []*svcapitypes.OpenZFSClientConfiguration) bool {
	if len(a) != len(b) {
		return false
	}
	counts := map[string]int{}
	for _, c := range a {
		counts[clientConfigurationKey(c)]++
	}
	for _, c := range b {
		k := clientConfigurationKey(c)
		if counts[k] == 0 {
			return false
		}
		counts[k]--
	}
	return true
}

func quotaKey(q *svcapitypes.OpenZFSUserOrGroupQuota) string {
	if q == nil {
		return "\x00nil"
	}
	return strVal(q.Type) + "\x01" + int64Val(q.ID) + "\x01" + int64Val(q.StorageCapacityQuotaGiB)
}

// isImplicitRootQuota reports whether q is the root entry FSx reports for
// Type without being asked: Id 0 with a quota of 0.
func isImplicitRootQuota(q *svcapitypes.OpenZFSUserOrGroupQuota) bool {
	return q != nil && q.ID != nil && *q.ID == 0 &&
		q.StorageCapacityQuotaGiB != nil && *q.StorageCapacityQuotaGiB == 0
}

// quotasEquivalent reports whether latest equals desired as a multiset once
// the implicit root entries desired does not declare are skipped.
func quotasEquivalent(desired, latest []*svcapitypes.OpenZFSUserOrGroupQuota) bool {
	declaredRoot := map[string]bool{}
	counts := map[string]int{}
	for _, q := range desired {
		if q != nil && q.ID != nil && *q.ID == 0 {
			declaredRoot[strVal(q.Type)] = true
		}
		counts[quotaKey(q)]++
	}
	for _, q := range latest {
		if isImplicitRootQuota(q) && !declaredRoot[strVal(q.Type)] {
			continue
		}
		k := quotaKey(q)
		if counts[k] == 0 {
			return false
		}
		counts[k]--
	}
	for _, n := range counts {
		if n != 0 {
			return false
		}
	}
	return true
}
