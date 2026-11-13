	// Before any side effect: reject removing an S3 policy, which the update
	// request cannot carry, and wait while the association is CREATING,
	// UPDATING or DELETING. UpdateDataRepositoryAssociation has no Tags member,
	// so tags are synced with TagResource/UntagResource and, when they are the
	// only change, the update call is skipped.
	if err = rejectUnexpressibleS3Changes(desired, latest, delta); err != nil {
		return nil, err
	}
	if err = requeueIfNotUpdatable(latest); err != nil {
		return nil, err
	}
	if delta.DifferentAt("Spec.Tags") {
		if err = rm.syncTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if !delta.DifferentExcept("Spec.Tags") {
		return desired, nil
	}
