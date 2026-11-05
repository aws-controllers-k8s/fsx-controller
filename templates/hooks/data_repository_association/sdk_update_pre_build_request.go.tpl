	// UpdateDataRepositoryAssociation has no Tags member, so tags are synced
	// with TagResource/UntagResource and, when they are the only change, the
	// update call is skipped.
	if delta.DifferentAt("Spec.Tags") {
		if err = rm.syncTags(ctx, desired, latest); err != nil {
			return nil, err
		}
	}
	if !delta.DifferentExcept("Spec.Tags") {
		return desired, nil
	}
