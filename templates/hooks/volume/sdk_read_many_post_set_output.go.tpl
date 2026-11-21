	// Describe has no ConstituentsPerAggregate and reports a conditional
	// CoolingPeriod default; keep what the user submitted.
	restoreNonRoundTrippedSpecFields(r.ko, ko)
	// DescribeVolumes does not report volume tags; read them from
	// ListTagsForResource.
	if err := rm.setTagsFromAWS(ctx, ko); err != nil {
		return nil, err
	}
	// FAILED is final; surface it as Terminal with FSx's
	// LifecycleTransitionReason rather than requeueing forever.
	if err := terminalIfFailed(ko); err != nil {
		return &resource{ko}, err
	}
